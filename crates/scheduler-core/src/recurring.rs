use chrono::Utc;
use sqlx::PgPool;

use domain::entities::job::Job;

#[derive(Debug, thiserror::Error)]
pub enum RecurringError {
    #[error("Job {0} is not recurring")]
    NotRecurring(uuid::Uuid),

    #[error("Recurring job {0} has no cron expression")]
    MissingCronExpression(uuid::Uuid),

    #[error("Invalid cron expression `{0}`")]
    InvalidCronExpression(String),

    #[error("Cron expression produced no future occurrence")]
    NoFutureOccurrence,

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Insert the next scheduled occurrence for a completed recurring job.
pub async fn schedule_next_occurrence(pool: &PgPool, job: &Job) -> Result<Job, RecurringError> {
    if !job.is_recurring() {
        return Err(RecurringError::NotRecurring(job.id));
    }

    let expression = job
        .cron_expression
        .as_deref()
        .ok_or(RecurringError::MissingCronExpression(job.id))?;

    let schedule = expression
        .parse::<cron::Schedule>()
        .map_err(|_| RecurringError::InvalidCronExpression(expression.to_string()))?;

    let next_at = schedule
        .upcoming(Utc)
        .next()
        .ok_or(RecurringError::NoFutureOccurrence)?;

    let next_job: Job = sqlx::query_as(
        r#"INSERT INTO jobs (
               id, queue_id, project_id, job_type, name, payload, status,
               priority, attempt_count, max_attempts, scheduled_at,
               cron_expression, correlation_id, batch_id, retry_policy_id
           )
           SELECT gen_random_uuid(), queue_id, project_id, job_type, name, payload,
                  'scheduled'::job_status, priority, 0, max_attempts, $2,
                  cron_expression, gen_random_uuid(), batch_id, retry_policy_id
           FROM jobs
           WHERE id = $1
           RETURNING
               id, queue_id, project_id, job_type, name, payload, status,
               priority, attempt_count, max_attempts, scheduled_at, next_attempt_at,
               cron_expression, worker_id, claimed_at, idempotency_key,
               correlation_id, batch_id, retry_policy_id,
               created_at, updated_at, completed_at"#,
    )
    .bind(job.id)
    .bind(next_at)
    .fetch_one(pool)
    .await?;

    tracing::info!(
        job_id = %job.id,
        next_job_id = %next_job.id,
        next_at = ?next_at,
        "Recurring job next occurrence scheduled"
    );

    Ok(next_job)
}
