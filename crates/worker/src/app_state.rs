use sqlx::PgPool;
use std::sync::Arc;
use std::sync::atomic::AtomicI32;
use uuid::Uuid;

/// Shared state for the worker process.
#[derive(Clone)]
pub struct WorkerAppState {
    pub pool: PgPool,
    pub worker_id: Uuid,
    pub worker_name: String,
    /// Number of jobs currently in flight — used for heartbeat reporting.
    pub active_jobs: Arc<AtomicI32>,
}

impl WorkerAppState {
    pub fn new(pool: PgPool, worker_id: Uuid, worker_name: String) -> Self {
        Self {
            pool,
            worker_id,
            worker_name,
            active_jobs: Arc::new(AtomicI32::new(0)),
        }
    }
}
