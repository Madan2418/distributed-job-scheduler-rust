use sqlx::PgPool;
use tokio::time::{interval, Duration};
use tracing::info;

use repositories::job_repository::JobRepository;

/// Stale job recovery: detects workers with no recent heartbeat and
/// re-queues any jobs they were holding. Runs every 30 seconds.
pub async fn run_stale_recovery(pool: PgPool, stale_threshold_seconds: i64) {
    let job_repo = JobRepository::new(pool);
    let mut ticker = interval(Duration::from_secs(30));
    info!("Stale job recovery started (threshold: {}s)", stale_threshold_seconds);

    loop {
        ticker.tick().await;
        match job_repo.recover_stale_jobs(stale_threshold_seconds).await {
            Ok(n) if n > 0 => info!(count = n, "Recovered stale jobs from dead workers"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "Stale job recovery error"),
        }
    }
}
