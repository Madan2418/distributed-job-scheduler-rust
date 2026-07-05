use sqlx::PgPool;
use tokio::time::{interval, Duration};
use tracing::{info, warn};
use uuid::Uuid;

use repositories::worker_repository::WorkerRepository;

/// Heartbeat task: periodically signals liveness to the database.
/// If this stops, the recovery task will detect the stale worker
/// and re-queue its claimed jobs.
pub async fn run_heartbeat(
    pool: PgPool,
    worker_id: Uuid,
    active_jobs_fn: impl Fn() -> i32,
) {
    let repo = WorkerRepository::new(pool);
    let mut ticker = interval(Duration::from_secs(10));
    info!(worker_id = %worker_id, "Heartbeat started");

    loop {
        ticker.tick().await;
        let active = active_jobs_fn();
        match repo.upsert_heartbeat(worker_id, active).await {
            Ok(_) => tracing::debug!(worker_id = %worker_id, active_jobs = active, "Heartbeat sent"),
            Err(e) => warn!(error = %e, "Failed to send heartbeat"),
        }
    }
}
