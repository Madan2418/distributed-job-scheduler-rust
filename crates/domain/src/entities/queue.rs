use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A named queue that holds jobs. Workers poll specific queues.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Queue {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub description: Option<String>,

    /// Maximum number of jobs that can run concurrently across all workers.
    pub concurrency_limit: i32,

    /// Default retry policy applied to jobs in this queue.
    pub retry_policy_id: Option<Uuid>,

    /// If true, the poller will not claim any new jobs from this queue.
    pub is_paused: bool,

    /// Shard index for queue sharding (hash(queue_id) % shard_count).
    pub shard: Option<i32>,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Queue {
    pub fn is_accepting_jobs(&self) -> bool {
        !self.is_paused
    }
}

/// Lightweight queue stats used by the dashboard.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct QueueStats {
    pub queue_id: Uuid,
    pub queue_name: String,
    pub queued_count: i64,
    pub running_count: i64,
    pub failed_count: i64,
    pub completed_count: i64,
    pub dlq_count: i64,
}
