use sqlx::PgPool;
use tokio::time::{interval, Duration as TokioDuration};
use tracing::{error, info, warn};
use uuid::Uuid;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use repositories::job_repository::JobRepository;
use repositories::queue_repository::QueueRepository;

use crate::executor::execute_job;

/// Poller: continuously polls queues for claimable jobs using
/// `SELECT ... FOR UPDATE SKIP LOCKED`. This is the core concurrency mechanism.
pub async fn run_poller(pool: PgPool, worker_id: Uuid, queue_ids: Vec<Uuid>) {
    let job_repo = JobRepository::new(pool.clone());
    let queue_repo = QueueRepository::new(pool.clone());
    let mut ticker = interval(TokioDuration::from_millis(500));

    info!(worker_id = %worker_id, queues = ?queue_ids, "Poller started");

    loop {
        ticker.tick().await;

        for &queue_id in &queue_ids {
            // Check queue is not paused
            let queue = match queue_repo.find_by_id(queue_id).await {
                Ok(Some(q)) => q,
                Ok(None) => continue,
                Err(e) => { warn!(error = %e, "Failed to fetch queue"); continue; }
            };

            if queue.is_paused {
                continue;
            }

            // Check concurrency limit
            let running = match queue_repo.count_running_jobs(queue_id).await {
                Ok(c) => c,
                Err(e) => { warn!(error = %e, "Failed to count running jobs"); continue; }
            };

            if running >= queue.concurrency_limit as i64 {
                continue;
            }

            // Attempt to atomically claim a job
            match job_repo.claim_next_job(queue_id, worker_id).await {
                Ok(Some(job)) => {
                    info!(
                        job_id = %job.id,
                        job_name = %job.name,
                        worker_id = %worker_id,
                        "Job claimed"
                    );

                    let pool_clone = pool.clone();

                    // Spawn each job execution as an independent Tokio task
                    tokio::spawn(async move {
                        execute_job(pool_clone, worker_id, job).await;
                    });
                }
                Ok(None) => {} // No job available — wait for next tick
                Err(e) => {
                    error!(error = %e, queue_id = %queue_id, "Error claiming job");
                }
            }
        }
    }
}

/// Poller variant that tracks active job count via a shared atomic counter.
/// Use this overload to report accurate in-flight metrics to the heartbeat.
pub async fn run_poller_with_counter(
    pool: PgPool,
    worker_id: Uuid,
    queue_ids: Vec<Uuid>,
    active_jobs: Arc<AtomicI32>,
) {
    let job_repo = JobRepository::new(pool.clone());
    let queue_repo = QueueRepository::new(pool.clone());
    let mut ticker = interval(TokioDuration::from_millis(500));

    info!(worker_id = %worker_id, queues = ?queue_ids, "Poller (with counter) started");

    loop {
        ticker.tick().await;

        for &queue_id in &queue_ids {
            let queue = match queue_repo.find_by_id(queue_id).await {
                Ok(Some(q)) => q,
                Ok(None) => continue,
                Err(e) => { warn!(error = %e, "Failed to fetch queue"); continue; }
            };

            if queue.is_paused {
                continue;
            }

            let running = match queue_repo.count_running_jobs(queue_id).await {
                Ok(c) => c,
                Err(e) => { warn!(error = %e, "Failed to count running jobs"); continue; }
            };

            if running >= queue.concurrency_limit as i64 {
                continue;
            }

            match job_repo.claim_next_job(queue_id, worker_id).await {
                Ok(Some(job)) => {
                    info!(
                        job_id = %job.id,
                        job_name = %job.name,
                        worker_id = %worker_id,
                        "Job claimed"
                    );

                    let pool_clone = pool.clone();
                    let counter = Arc::clone(&active_jobs);

                    // Increment before spawn; decrement inside the task when done
                    counter.fetch_add(1, Ordering::SeqCst);

                    tokio::spawn(async move {
                        execute_job(pool_clone, worker_id, job).await;
                        counter.fetch_sub(1, Ordering::SeqCst);
                    });
                }
                Ok(None) => {}
                Err(e) => {
                    error!(error = %e, queue_id = %queue_id, "Error claiming job");
                }
            }
        }
    }
}
