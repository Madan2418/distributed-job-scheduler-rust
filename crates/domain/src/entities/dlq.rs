use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// Dead Letter Queue entry — created when a job exceeds max_attempts.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct DeadLetterEntry {
    pub id: Uuid,
    pub job_id: Uuid,
    pub queue_id: Uuid,
    pub project_id: Uuid,
    pub job_name: String,
    pub payload: Value,
    pub last_error: Option<String>,
    pub attempt_count: i32,
    /// Optional AI-generated failure summary (bonus feature).
    pub ai_summary: Option<String>,
    pub moved_to_dlq_at: DateTime<Utc>,
    pub manually_retried_at: Option<DateTime<Utc>>,
    pub manually_retried_by: Option<Uuid>,
}
