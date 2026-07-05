use chrono::{Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;
use tracing::{error, info, warn};

use domain::entities::job::Job;
use domain::entities::outbox::OutboxEvent;
use domain::entities::retry_policy::RetryPolicy;
use domain::enums::OutboxEventType;
use repositories::outbox_repository::OutboxRepository;

/// Executor: runs a claimed job, then handles success/failure/retry/DLQ.
/// All state changes and outbox events are written in the SAME transaction.
pub async fn execute_job(pool: PgPool, worker_id: Uuid, job: Job) {
    let started_at = Utc::now();

    // Mark as running
    if let Err(e) = sqlx::query(
        "UPDATE jobs SET status = 'running', updated_at = NOW() WHERE id = $1"
    )
    .bind(job.id)
    .execute(&pool)
    .await {
        error!(job_id = %job.id, error = %e, "Failed to mark job as running");
        return;
    }

    let result = simulate_execute(&job).await;
    let finished_at = Utc::now();
    let duration_ms = (finished_at - started_at).num_milliseconds();
    let execution_id = Uuid::new_v4();

    match result {
        Ok(()) => {
            let mut tx = match pool.begin().await {
                Ok(tx) => tx,
                Err(e) => { error!(error = %e, "Failed to begin transaction"); return; }
            };

            let _ = sqlx::query(
                "UPDATE jobs SET status = 'completed', completed_at = NOW(), updated_at = NOW(), worker_id = NULL WHERE id = $1"
            )
            .bind(job.id)
            .execute(&mut *tx)
            .await;

            let _ = sqlx::query(
                r#"INSERT INTO job_executions
                   (id, job_id, worker_id, attempt_number, started_at, finished_at, success, duration_ms, correlation_id)
                   VALUES ($1, $2, $3, $4, $5, $6, true, $7, $8)"#
            )
            .bind(execution_id)
            .bind(job.id)
            .bind(worker_id)
            .bind(job.attempt_count + 1)
            .bind(started_at)
            .bind(finished_at)
            .bind(duration_ms)
            .bind(job.correlation_id)
            .execute(&mut *tx)
            .await;

            let event = OutboxEvent::new(
                OutboxEventType::JobCompleted,
                job.id,
                serde_json::json!({ "job_id": job.id, "queue_id": job.queue_id }),
                job.correlation_id,
            );
            let _ = OutboxRepository::insert_in_tx(&mut tx, &event).await;

            if let Err(e) = tx.commit().await {
                error!(error = %e, "Failed to commit success transaction");
                return;
            }

            info!(job_id = %job.id, duration_ms = duration_ms, "Job completed");

            // Re-enqueue next occurrence for recurring jobs
            if job.is_recurring() {
                if let Err(e) = scheduler_core::recurring::schedule_next_occurrence(&pool, &job).await {
                    warn!(job_id = %job.id, error = %e, "Failed to schedule next recurring occurrence");
                }
            }
        }

        Err(error_msg) => {
            let attempt = job.attempt_count + 1;
            let should_dlq = attempt >= job.max_attempts;
            let mut tx = match pool.begin().await {
                Ok(tx) => tx,
                Err(e) => { error!(error = %e, "Failed to begin transaction"); return; }
            };

            let _ = sqlx::query(
                r#"INSERT INTO job_executions
                   (id, job_id, worker_id, attempt_number, started_at, finished_at, success, error_message, duration_ms, correlation_id)
                   VALUES ($1, $2, $3, $4, $5, $6, false, $7, $8, $9)"#
            )
            .bind(execution_id)
            .bind(job.id)
            .bind(worker_id)
            .bind(attempt)
            .bind(started_at)
            .bind(finished_at)
            .bind(&error_msg)
            .bind(duration_ms)
            .bind(job.correlation_id)
            .execute(&mut *tx)
            .await;

            if should_dlq {
                let _ = sqlx::query(
                    "UPDATE jobs SET status = 'failed', attempt_count = $2, worker_id = NULL, updated_at = NOW() WHERE id = $1"
                )
                .bind(job.id)
                .bind(attempt)
                .execute(&mut *tx)
                .await;

                let _ = sqlx::query(
                    r#"INSERT INTO dead_letter_queue
                       (id, job_id, queue_id, project_id, job_name, payload, last_error, attempt_count, moved_to_dlq_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#
                )
                .bind(Uuid::new_v4())
                .bind(job.id)
                .bind(job.queue_id)
                .bind(job.project_id)
                .bind(&job.name)
                .bind(&job.payload)
                .bind(&error_msg)
                .bind(attempt)
                .bind(Utc::now())
                .execute(&mut *tx)
                .await;

                let event = OutboxEvent::new(
                    OutboxEventType::JobMovedToDlq,
                    job.id,
                    serde_json::json!({ "job_id": job.id, "queue_id": job.queue_id, "error": error_msg }),
                    job.correlation_id,
                );
                let _ = OutboxRepository::insert_in_tx(&mut tx, &event).await;
                warn!(job_id = %job.id, attempts = attempt, "Job moved to DLQ");
            } else {
                // Strategy Pattern: use the job's retry policy if available, else exponential default
                let delay_secs = calculate_retry_delay(&pool, &job, attempt).await;
                let next_attempt_at = Utc::now() + Duration::seconds(delay_secs);

                let _ = sqlx::query(
                    "UPDATE jobs SET status = 'scheduled', next_attempt_at = $2, attempt_count = $3, worker_id = NULL, updated_at = NOW() WHERE id = $1"
                )
                .bind(job.id)
                .bind(next_attempt_at)
                .bind(attempt)
                .execute(&mut *tx)
                .await;

                let event = OutboxEvent::new(
                    OutboxEventType::JobRetryScheduled,
                    job.id,
                    serde_json::json!({ "job_id": job.id, "next_attempt_at": next_attempt_at, "attempt": attempt }),
                    job.correlation_id,
                );
                let _ = OutboxRepository::insert_in_tx(&mut tx, &event).await;
                info!(job_id = %job.id, next_attempt_at = ?next_attempt_at, attempt, "Job scheduled for retry");
            }

            let _ = tx.commit().await;
        }
    }
}

/// Strategy Pattern: look up the retry policy attached to the job and calculate delay.
/// Falls back to a sensible exponential default (10s * 2^attempt, capped at 300s).
async fn calculate_retry_delay(pool: &PgPool, job: &Job, attempt: i32) -> i64 {
    if let Some(policy_id) = job.retry_policy_id {
        let policy: Option<RetryPolicy> = sqlx::query_as(
            r#"SELECT id, name, max_attempts, backoff_strategy, base_delay_seconds,
                      max_delay_seconds, multiplier, created_at
               FROM retry_policies WHERE id = $1"#,
        )
        .bind(policy_id)
        .fetch_optional(pool)
        .await
        .unwrap_or(None);

        if let Some(p) = policy {
            return p.next_delay_seconds(attempt);
        }
    }

    // Default: exponential 10s * 2^(attempt-1), capped at 300s
    let raw = 10i64 * (2i64.pow((attempt - 1).max(0) as u32));
    raw.min(300)
}

/// Simulated job handler.
/// In production: dispatch by job.name or job_type to registered async handlers.
async fn simulate_execute(job: &Job) -> Result<(), String> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    info!(job_id = %job.id, job_name = %job.name, "Executing job payload");
    Ok(())
}
