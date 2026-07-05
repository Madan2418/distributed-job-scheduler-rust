use anyhow::Context;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::signal;
use tracing::info;
use uuid::Uuid;

use worker::app_state::WorkerAppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment
    dotenvy::dotenv().ok();
    common::tracing_setup::init();

    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL not set")?;
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());
    let jwt_secret = std::env::var("JWT_SECRET").context("JWT_SECRET not set")?;
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()
        .unwrap_or(8080);

    info!("Connecting to database...");
    let pool = infrastructure::db::create_pool(&database_url)
        .await
        .context("Failed to connect to database")?;

    // Run migrations automatically
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .context("Failed to run database migrations")?;

    info!("Connecting to Redis...");
    let redis = infrastructure::redis_client::create_connection_manager(&redis_url)
        .await
        .context("Failed to connect to Redis")?;

    let app_state = api::app_state::AppState::new(pool.clone(), redis.clone(), jwt_secret);

    // Worker setup — shared atomic job counter for accurate heartbeat metrics
    let worker_id = Uuid::new_v4();
    let worker_name = format!("worker-{}", &worker_id.to_string()[..8]);
    let worker_state = WorkerAppState::new(pool.clone(), worker_id, worker_name.clone());
    let active_jobs = Arc::clone(&worker_state.active_jobs);

    // Spawn all background tasks
    tokio::spawn(infrastructure::outbox_relay::run_relay(pool.clone(), redis));

    tokio::spawn(scheduler_core::cron_scheduler::run_scheduled_job_promoter(pool.clone()));

    tokio::spawn(scheduler_core::stale_recovery::run_stale_recovery(pool.clone(), 60));

    // Heartbeat: reads active job count from the shared atomic counter
    {
        let counter = Arc::clone(&active_jobs);
        tokio::spawn(worker::heartbeat::run_heartbeat(
            pool.clone(),
            worker_id,
            move || counter.load(Ordering::SeqCst),
        ));
    }

    // Auto-register worker
    {
        let repo = repositories::worker_repository::WorkerRepository::new(pool.clone());
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        repo.register(worker_id, &worker_name, &hostname).await?;
        info!(worker_id = %worker_id, "Worker registered");
    }

    // Start polling queues — uses the counter-aware variant for accurate metrics
    let queue_ids_str = std::env::var("QUEUE_IDS").unwrap_or_default();
    let queue_ids: Vec<Uuid> = queue_ids_str
        .split(',')
        .filter_map(|s| Uuid::parse_str(s.trim()).ok())
        .collect();

    if !queue_ids.is_empty() {
        let counter = Arc::clone(&active_jobs);
        tokio::spawn(worker::poller::run_poller_with_counter(
            pool.clone(),
            worker_id,
            queue_ids,
            counter,
        ));
    }

    // Start API server (blocks until shutdown)
    tokio::select! {
        _ = api::server::run(app_state, port) => {},
        _ = signal::ctrl_c() => {
            info!("Shutdown signal received");
        }
    }

    // Graceful shutdown: deregister worker
    let repo = repositories::worker_repository::WorkerRepository::new(pool.clone());
    let _ = repo.deregister(worker_id).await;
    info!("Worker deregistered. Shutdown complete.");

    Ok(())
}
