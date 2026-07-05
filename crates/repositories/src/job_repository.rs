use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use domain::entities::job::{Job, JobExecution, JobSummary};
use domain::enums::JobStatus;

/// All SQLx queries for jobs. No business logic here — only data access.
pub struct JobRepository {
    pub pool: PgPool,
}

impl JobRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The core atomic claim query.
    /// SELECT ... FOR UPDATE SKIP LOCKED ensures exactly one worker claims each job.
    pub async fn claim_next_job(
        &self,
        queue_id: Uuid,
        worker_id: Uuid,
    ) -> Result<Option<Job>, sqlx::Error> {
        sqlx::query_as(
            r#"
            UPDATE jobs SET
                status = 'claimed',
                worker_id = $2,
                claimed_at = NOW(),
                updated_at = NOW()
            WHERE id = (
                SELECT id FROM jobs
                WHERE status = 'queued'
                  AND queue_id = $1
                  AND (scheduled_at IS NULL OR scheduled_at <= NOW())
                ORDER BY
                    CASE priority
                        WHEN 'critical' THEN 20
                        WHEN 'high'     THEN 10
                        WHEN 'normal'   THEN 5
                        WHEN 'low'      THEN 1
                    END DESC,
                    created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING
                id, queue_id, project_id, job_type, name, payload, status,
                priority, attempt_count, max_attempts, scheduled_at, next_attempt_at,
                cron_expression, worker_id, claimed_at, idempotency_key, correlation_id,
                batch_id, retry_policy_id, created_at, updated_at, completed_at
            "#
        )
        .bind(queue_id)
        .bind(worker_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn find_by_id(&self, id: Uuid) -> Result<Option<Job>, sqlx::Error> {
        sqlx::query_as(
            r#"
            SELECT id, queue_id, project_id, job_type, name, payload, status,
                   priority, attempt_count, max_attempts, scheduled_at, next_attempt_at,
                   cron_expression, worker_id, claimed_at, idempotency_key, correlation_id,
                   batch_id, retry_policy_id, created_at, updated_at, completed_at
            FROM jobs WHERE id = $1
            "#
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn list_by_queue(
        &self,
        queue_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<JobSummary>, sqlx::Error> {
        sqlx::query_as(
            r#"
            SELECT id, name, queue_id, status, priority, job_type,
                   attempt_count, max_attempts, created_at, updated_at
            FROM jobs
            WHERE queue_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#
        )
        .bind(queue_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn mark_running(&self, job_id: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE jobs SET status = 'running', updated_at = NOW() WHERE id = $1")
            .bind(job_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn mark_completed(&self, job_id: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"UPDATE jobs
               SET status = 'completed', completed_at = NOW(), updated_at = NOW(), worker_id = NULL
               WHERE id = $1"#
        )
        .bind(job_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn schedule_retry(
        &self,
        job_id: Uuid,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"UPDATE jobs
               SET status = 'scheduled', next_attempt_at = $2,
                   attempt_count = attempt_count + 1, worker_id = NULL, updated_at = NOW()
               WHERE id = $1"#
        )
        .bind(job_id)
        .bind(next_attempt_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_failed(&self, job_id: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"UPDATE jobs
               SET status = 'failed', attempt_count = attempt_count + 1,
                   worker_id = NULL, updated_at = NOW()
               WHERE id = $1"#
        )
        .bind(job_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Re-surface scheduled jobs whose next_attempt_at is in the past.
    pub async fn re_queue_scheduled_jobs(&self) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            r#"UPDATE jobs
               SET status = 'queued', updated_at = NOW()
               WHERE status = 'scheduled'
                 AND COALESCE(next_attempt_at, scheduled_at) <= NOW()"#
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Recover jobs held by stale workers (heartbeat older than threshold).
    pub async fn recover_stale_jobs(&self, stale_threshold_seconds: i64) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            r#"UPDATE jobs
               SET status = 'queued', worker_id = NULL, claimed_at = NULL, updated_at = NOW()
               WHERE status IN ('claimed', 'running')
                 AND worker_id IN (
                     SELECT w.id FROM workers w
                     LEFT JOIN (
                         SELECT worker_id, MAX(last_seen) AS last_seen
                         FROM worker_heartbeats GROUP BY worker_id
                     ) hb ON w.id = hb.worker_id
                     WHERE hb.last_seen IS NULL
                        OR EXTRACT(EPOCH FROM (NOW() - hb.last_seen)) > $1
                 )"#
        )
        .bind(stale_threshold_seconds as f64)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn insert_execution(&self, execution: &JobExecution) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"INSERT INTO job_executions
               (id, job_id, worker_id, attempt_number, started_at, finished_at,
                success, error_message, duration_ms, correlation_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#
        )
        .bind(execution.id)
        .bind(execution.job_id)
        .bind(execution.worker_id)
        .bind(execution.attempt_number)
        .bind(execution.started_at)
        .bind(execution.finished_at)
        .bind(execution.success)
        .bind(&execution.error_message)
        .bind(execution.duration_ms)
        .bind(execution.correlation_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn find_by_idempotency_key(
        &self,
        key: &str,
        queue_id: Uuid,
    ) -> Result<Option<Job>, sqlx::Error> {
        sqlx::query_as(
            r#"SELECT id, queue_id, project_id, job_type, name, payload, status,
                      priority, attempt_count, max_attempts, scheduled_at, next_attempt_at,
                      cron_expression, worker_id, claimed_at, idempotency_key, correlation_id,
                      batch_id, retry_policy_id, created_at, updated_at, completed_at
               FROM jobs WHERE idempotency_key = $1 AND queue_id = $2"#
        )
        .bind(key)
        .bind(queue_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn count_by_status_and_queue(
        &self,
        queue_id: Uuid,
        status: &JobStatus,
    ) -> Result<i64, sqlx::Error> {
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM jobs WHERE queue_id = $1 AND status = $2::job_status"
        )
        .bind(queue_id)
        .bind(status.to_string())
        .fetch_one(&self.pool)
        .await?;
        Ok(count.0)
    }

    /// Return all execution attempts for a job, newest first.
    pub async fn list_executions(&self, job_id: Uuid) -> Result<Vec<JobExecution>, sqlx::Error> {
        sqlx::query_as(
            r#"SELECT id, job_id, worker_id, attempt_number, started_at, finished_at,
                      success, error_message, duration_ms, correlation_id
               FROM job_executions
               WHERE job_id = $1
               ORDER BY attempt_number DESC"#,
        )
        .bind(job_id)
        .fetch_all(&self.pool)
        .await
    }
}
