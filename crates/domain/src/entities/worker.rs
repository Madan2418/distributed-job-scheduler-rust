use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A registered worker instance. Workers self-register on startup.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Worker {
    pub id: Uuid,
    /// Human-readable identifier (hostname + process id).
    pub name: String,
    pub hostname: String,
    pub is_active: bool,
    pub registered_at: DateTime<Utc>,
    pub deregistered_at: Option<DateTime<Utc>>,
}

/// Periodic liveness signal sent by each worker.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct WorkerHeartbeat {
    pub id: Uuid,
    pub worker_id: Uuid,
    pub last_seen: DateTime<Utc>,
    /// Number of jobs currently running on this worker.
    pub active_jobs: i32,
}

impl Worker {
    /// A worker is considered stale if its last heartbeat is older than the threshold.
    pub fn is_stale(last_seen: DateTime<Utc>, threshold_seconds: i64) -> bool {
        let age = Utc::now()
            .signed_duration_since(last_seen)
            .num_seconds();
        age > threshold_seconds
    }
}
