use sqlx::PgPool;
use tokio::time::{interval, Duration};
use tracing::info;

use repositories::job_repository::JobRepository;

/// Periodically re-queues scheduled jobs whose `next_attempt_at` has passed.
/// This covers both retries and one-time scheduled jobs.
pub async fn run_scheduled_job_promoter(pool: PgPool) {
    let job_repo = JobRepository::new(pool);
    let mut ticker = interval(Duration::from_secs(5));
    info!("Scheduled job promoter started");

    loop {
        ticker.tick().await;
        match job_repo.re_queue_scheduled_jobs().await {
            Ok(n) if n > 0 => info!(count = n, "Re-queued scheduled jobs"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "Failed to re-queue scheduled jobs"),
        }
    }
}
