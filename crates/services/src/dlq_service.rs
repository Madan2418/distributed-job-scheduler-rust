use sqlx::PgPool;
use uuid::Uuid;

use domain::entities::dlq::DeadLetterEntry;
use domain::enums::UserRole;
use repositories::dlq_repository::DlqRepository;
use repositories::project_repository::ProjectRepository;

pub struct DlqService {
    dlq_repo: DlqRepository,
    project_repo: ProjectRepository,
    pool: PgPool,
}

impl DlqService {
    pub fn new(pool: PgPool) -> Self {
        Self {
            dlq_repo: DlqRepository::new(pool.clone()),
            project_repo: ProjectRepository::new(pool.clone()),
            pool,
        }
    }

    pub async fn list(
        &self,
        project_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<DeadLetterEntry>, sqlx::Error> {
        self.dlq_repo.list_by_project(project_id, limit, offset).await
    }

    /// Manual retry: mark the DLQ entry and re-queue the original job atomically.
    pub async fn retry_job(&self, dlq_id: Uuid, retried_by: Uuid) -> Result<(), DlqServiceError> {
        let entry = self
            .dlq_repo
            .find_by_id(dlq_id)
            .await?
            .ok_or(DlqServiceError::DlqEntryNotFound(dlq_id))?;

        if entry.manually_retried_at.is_some() {
            return Err(DlqServiceError::AlreadyRetried(dlq_id));
        }

        let role = self
            .project_repo
            .get_member_role(entry.project_id, retried_by)
            .await?
            .ok_or(DlqServiceError::Forbidden)?;

        if role == UserRole::Viewer {
            return Err(DlqServiceError::Forbidden);
        }

        let mut tx = self.pool.begin().await?;

        let updated = sqlx::query(
            r#"UPDATE jobs
               SET status = 'queued',
                   worker_id = NULL,
                   claimed_at = NULL,
                   next_attempt_at = NULL,
                   completed_at = NULL,
                   updated_at = NOW()
               WHERE id = $1"#,
        )
        .bind(entry.job_id)
        .execute(&mut *tx)
        .await?;

        if updated.rows_affected() == 0 {
            return Err(DlqServiceError::JobNotFound(entry.job_id));
        }

        sqlx::query(
            r#"UPDATE dead_letter_queue
               SET manually_retried_at = NOW(), manually_retried_by = $2
               WHERE id = $1"#,
        )
        .bind(dlq_id)
        .bind(retried_by)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        tracing::info!(
            dlq_id = %dlq_id,
            job_id = %entry.job_id,
            retried_by = %retried_by,
            "DLQ job manually retried"
        );
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DlqServiceError {
    #[error("DLQ entry {0} not found")]
    DlqEntryNotFound(Uuid),

    #[error("DLQ entry {0} was already retried")]
    AlreadyRetried(Uuid),

    #[error("Original job {0} not found")]
    JobNotFound(Uuid),

    #[error("User is not allowed to retry this DLQ entry")]
    Forbidden,

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}
