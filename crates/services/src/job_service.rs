use chrono::Utc;
use serde::Deserialize;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use domain::entities::job::{Job, JobExecution, JobSummary};
use domain::enums::{JobPriority, JobStatus, JobType, UserRole};
use repositories::job_repository::JobRepository;
use repositories::project_repository::ProjectRepository;
use repositories::queue_repository::QueueRepository;

/// Request object for creating a new job.
/// Factory Pattern: this drives which construction path is used.
#[derive(Debug, Deserialize)]
pub struct CreateJobRequest {
    pub name: String,
    pub payload: Value,
    pub job_type: JobType,
    pub priority: Option<JobPriority>,
    pub max_attempts: Option<i32>,
    pub scheduled_at: Option<chrono::DateTime<Utc>>,
    pub cron_expression: Option<String>,
    pub idempotency_key: Option<String>,
    pub batch_id: Option<Uuid>,
    pub retry_policy_id: Option<Uuid>,
}

pub struct JobService {
    job_repo: JobRepository,
    project_repo: ProjectRepository,
    queue_repo: QueueRepository,
    pool: PgPool,
}

impl JobService {
    pub fn new(pool: PgPool) -> Self {
        Self {
            job_repo: JobRepository::new(pool.clone()),
            project_repo: ProjectRepository::new(pool.clone()),
            queue_repo: QueueRepository::new(pool.clone()),
            pool,
        }
    }

    /// Job Factory: single entry point, dispatches to correct constructor.
    pub async fn create_job(
        &self,
        queue_id: Uuid,
        user_id: Uuid,
        req: CreateJobRequest,
    ) -> Result<Job, JobServiceError> {
        // --- Idempotency check ---
        if let Some(ref key) = req.idempotency_key {
            if let Some(existing) = self.job_repo.find_by_idempotency_key(key, queue_id).await? {
                tracing::info!(job_id = %existing.id, "Idempotent job submission — returning existing job");
                return Ok(existing);
            }
        }

        let queue = self
            .queue_repo
            .find_by_id(queue_id)
            .await?
            .ok_or(JobServiceError::QueueNotFound(queue_id))?;

        if !queue.is_accepting_jobs() {
            return Err(JobServiceError::QueuePaused(queue_id));
        }

        let role = self
            .project_repo
            .get_member_role(queue.project_id, user_id)
            .await?
            .ok_or(JobServiceError::Forbidden)?;

        if role == UserRole::Viewer {
            return Err(JobServiceError::Forbidden);
        }

        let project_id = queue.project_id;

        let correlation_id = common::correlation::new_correlation_id();
        let priority = req.priority.unwrap_or_default();
        let max_attempts = req.max_attempts.unwrap_or(3);

        // Factory: initial status depends on job type
        let (status, scheduled_at) = match req.job_type {
            JobType::Immediate => (JobStatus::Queued, None),
            JobType::Delayed | JobType::Scheduled => (JobStatus::Scheduled, req.scheduled_at),
            JobType::Recurring => (JobStatus::Queued, None),
            JobType::Batch => (JobStatus::Queued, None),
        };

        // Validate cron expression for recurring jobs
        if req.job_type == JobType::Recurring {
            if let Some(ref expr) = req.cron_expression {
                expr.parse::<cron::Schedule>().map_err(|_| {
                    JobServiceError::InvalidCronExpression(expr.clone())
                })?;
            }
        }

        let retry_policy_id = req.retry_policy_id.or(queue.retry_policy_id);

        let job: Job = sqlx::query_as(
            r#"INSERT INTO jobs (
                id, queue_id, project_id, job_type, name, payload, status,
                priority, attempt_count, max_attempts, scheduled_at,
                cron_expression, idempotency_key, correlation_id,
                batch_id, retry_policy_id
            )
            VALUES (
                gen_random_uuid(), $1, $2, $3::job_type, $4, $5, $6::job_status,
                $7::job_priority, 0, $8, $9, $10, $11, $12, $13, $14
            )
            RETURNING
                id, queue_id, project_id, job_type, name, payload, status,
                priority, attempt_count, max_attempts, scheduled_at, next_attempt_at,
                cron_expression, worker_id, claimed_at, idempotency_key,
                correlation_id, batch_id, retry_policy_id,
                created_at, updated_at, completed_at"#
        )
        .bind(queue_id)
        .bind(project_id)
        .bind(req.job_type.to_string())
        .bind(&req.name)
        .bind(&req.payload)
        .bind(status.to_string())
        .bind(priority.to_string())
        .bind(max_attempts)
        .bind(scheduled_at)
        .bind(&req.cron_expression)
        .bind(&req.idempotency_key)
        .bind(correlation_id)
        .bind(req.batch_id)
        .bind(retry_policy_id)
        .fetch_one(&self.pool)
        .await?;

        tracing::info!(job_id = %job.id, job_type = ?job.job_type, queue = %queue_id, "Job created");
        Ok(job)
    }

    pub async fn get_job(&self, job_id: Uuid) -> Result<Option<Job>, sqlx::Error> {
        self.job_repo.find_by_id(job_id).await
    }

    pub async fn list_jobs(
        &self,
        queue_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<JobSummary>, sqlx::Error> {
        self.job_repo.list_by_queue(queue_id, limit, offset).await
    }

    /// Return execution history for a job (all attempts, ordered newest-first).
    pub async fn list_executions(&self, job_id: Uuid) -> Result<Vec<JobExecution>, sqlx::Error> {
        self.job_repo.list_executions(job_id).await
    }

    /// Cancel a pending or queued job. Only the job's project member (non-viewer) may cancel.
    pub async fn cancel_job(&self, job_id: Uuid, user_id: Uuid) -> Result<(), JobServiceError> {
        let job = self
            .job_repo
            .find_by_id(job_id)
            .await?
            .ok_or(JobServiceError::QueueNotFound(job_id))?; // reuse error variant

        let role = self
            .project_repo
            .get_member_role(job.project_id, user_id)
            .await?
            .ok_or(JobServiceError::Forbidden)?;

        if role == UserRole::Viewer {
            return Err(JobServiceError::Forbidden);
        }

        // Only pending/queued/scheduled jobs can be cancelled
        sqlx::query(
            r#"UPDATE jobs SET status = 'failed', updated_at = NOW()
               WHERE id = $1 AND status IN ('pending', 'queued', 'scheduled')"#,
        )
        .bind(job_id)
        .execute(&self.pool)
        .await?;

        tracing::info!(job_id = %job_id, "Job cancelled by user");
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JobServiceError {
    #[error("Queue {0} not found")]
    QueueNotFound(Uuid),

    #[error("Queue {0} is paused")]
    QueuePaused(Uuid),

    #[error("User is not allowed to create jobs in this queue")]
    Forbidden,

    #[error("Invalid cron expression: {0}")]
    InvalidCronExpression(String),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}
