use redis::aio::ConnectionManager;
use sqlx::PgPool;
use std::sync::Arc;

use services::auth_service::AuthService;
use services::job_service::JobService;
use services::queue_service::QueueService;
use services::dlq_service::DlqService;
use services::project_service::ProjectService;
use services::workflow_service::WorkflowService;

/// Shared state injected into every Axum handler.
#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub redis: ConnectionManager,
    pub auth_service: Arc<AuthService>,
    pub job_service: Arc<JobService>,
    pub queue_service: Arc<QueueService>,
    pub dlq_service: Arc<DlqService>,
    pub project_service: Arc<ProjectService>,
    pub workflow_service: Arc<WorkflowService>,
    pub jwt_secret: String,
}

impl AppState {
    pub fn new(pool: PgPool, redis: ConnectionManager, jwt_secret: String) -> Self {
        Self {
            auth_service: Arc::new(AuthService::new(pool.clone(), jwt_secret.clone())),
            job_service: Arc::new(JobService::new(pool.clone())),
            queue_service: Arc::new(QueueService::new(pool.clone())),
            dlq_service: Arc::new(DlqService::new(pool.clone())),
            project_service: Arc::new(ProjectService::new(pool.clone())),
            workflow_service: Arc::new(WorkflowService::new(pool.clone())),
            pool,
            redis,
            jwt_secret,
        }
    }
}
