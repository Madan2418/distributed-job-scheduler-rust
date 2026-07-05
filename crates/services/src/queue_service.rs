use sqlx::PgPool;
use uuid::Uuid;

use domain::entities::queue::{Queue, QueueStats};
use domain::enums::UserRole;
use repositories::project_repository::ProjectRepository;
use repositories::queue_repository::QueueRepository;

pub struct QueueService {
    queue_repo: QueueRepository,
    project_repo: ProjectRepository,
}

impl QueueService {
    pub fn new(pool: PgPool) -> Self {
        Self {
            queue_repo: QueueRepository::new(pool.clone()),
            project_repo: ProjectRepository::new(pool),
        }
    }

    pub async fn create_queue(
        &self,
        user_id: Uuid,
        project_id: Uuid,
        name: &str,
        description: Option<&str>,
        concurrency_limit: i32,
        retry_policy_id: Option<Uuid>,
    ) -> Result<Queue, QueueServiceError> {
        self.require_project_writer(project_id, user_id).await?;
        Ok(self
            .queue_repo
            .create(project_id, name, description, concurrency_limit, retry_policy_id)
            .await?)
    }

    pub async fn pause_queue(&self, user_id: Uuid, queue_id: Uuid) -> Result<(), QueueServiceError> {
        let queue = self.queue_for_action(queue_id, user_id, true).await?;
        self.queue_repo.set_paused(queue.id, true).await?;
        tracing::info!(queue_id = %queue_id, "Queue paused");
        Ok(())
    }

    pub async fn resume_queue(&self, user_id: Uuid, queue_id: Uuid) -> Result<(), QueueServiceError> {
        let queue = self.queue_for_action(queue_id, user_id, true).await?;
        self.queue_repo.set_paused(queue.id, false).await?;
        tracing::info!(queue_id = %queue_id, "Queue resumed");
        Ok(())
    }

    pub async fn get_stats(
        &self,
        user_id: Uuid,
        queue_id: Uuid,
    ) -> Result<QueueStats, QueueServiceError> {
        self.queue_for_action(queue_id, user_id, false).await?;
        Ok(self.queue_repo.get_stats(queue_id).await?)
    }

    pub async fn list_by_project(
        &self,
        user_id: Uuid,
        project_id: Uuid,
    ) -> Result<Vec<Queue>, QueueServiceError> {
        self.require_project_member(project_id, user_id).await?;
        Ok(self.queue_repo.list_by_project(project_id).await?)
    }

    async fn queue_for_action(
        &self,
        queue_id: Uuid,
        user_id: Uuid,
        require_writer: bool,
    ) -> Result<Queue, QueueServiceError> {
        let queue = self
            .queue_repo
            .find_by_id(queue_id)
            .await?
            .ok_or(QueueServiceError::QueueNotFound(queue_id))?;

        if require_writer {
            self.require_project_writer(queue.project_id, user_id).await?;
        } else {
            self.require_project_member(queue.project_id, user_id).await?;
        }

        Ok(queue)
    }

    async fn require_project_member(
        &self,
        project_id: Uuid,
        user_id: Uuid,
    ) -> Result<UserRole, QueueServiceError> {
        self.project_repo
            .get_member_role(project_id, user_id)
            .await?
            .ok_or(QueueServiceError::Forbidden)
    }

    async fn require_project_writer(
        &self,
        project_id: Uuid,
        user_id: Uuid,
    ) -> Result<(), QueueServiceError> {
        let role = self.require_project_member(project_id, user_id).await?;
        if role == UserRole::Viewer {
            return Err(QueueServiceError::Forbidden);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QueueServiceError {
    #[error("Queue {0} not found")]
    QueueNotFound(Uuid),

    #[error("User is not allowed to access this queue")]
    Forbidden,

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}
