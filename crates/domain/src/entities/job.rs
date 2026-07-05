use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::enums::{JobPriority, JobStatus, JobType};
use crate::error::DomainError;

/// Core Job entity. This is a pure domain struct — no HTTP, no SQL imports.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Job {
    pub id: Uuid,
    pub queue_id: Uuid,
    pub project_id: Uuid,

    /// Job type drives how it was constructed and how it behaves.
    pub job_type: JobType,

    /// Human-readable name for dashboard visibility.
    pub name: String,

    /// Arbitrary JSON payload — the "work" to be done.
    pub payload: Value,

    /// Current lifecycle state.
    pub status: JobStatus,

    /// Priority used for ordering in the claim query.
    pub priority: JobPriority,

    /// Number of execution attempts so far.
    pub attempt_count: i32,

    /// Maximum attempts before moving to DLQ.
    pub max_attempts: i32,

    /// For delayed/scheduled jobs: when to surface this job.
    pub scheduled_at: Option<DateTime<Utc>>,

    /// Set by the worker after retry-backoff calculation.
    pub next_attempt_at: Option<DateTime<Utc>>,

    /// Cron expression for recurring jobs.
    pub cron_expression: Option<String>,

    /// ID of the worker instance currently holding this job.
    pub worker_id: Option<Uuid>,

    /// When the current worker claimed this job.
    pub claimed_at: Option<DateTime<Utc>>,

    /// Client-supplied idempotency key for safe re-submission.
    pub idempotency_key: Option<String>,

    /// Correlation ID propagated through all executions, logs, and outbox events.
    pub correlation_id: Uuid,

    /// If this job belongs to a batch, the parent batch ID.
    pub batch_id: Option<Uuid>,

    /// Foreign key to the retry_policies table.
    pub retry_policy_id: Option<Uuid>,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl Job {
    /// Validate that a state transition is legal according to the state machine.
    pub fn can_transition_to(&self, next: &JobStatus) -> bool {
        use JobStatus::*;
        matches!(
            (&self.status, next),
            (Pending, Queued)
                | (Queued, Claimed)
                | (Claimed, Running)
                | (Running, Completed)
                | (Running, Scheduled)
                | (Running, Failed)
                | (Scheduled, Queued)
        )
    }

    pub fn transition_to(&mut self, next: JobStatus) -> Result<(), DomainError> {
        if !self.can_transition_to(&next) {
            return Err(DomainError::InvalidStateTransition {
                from: self.status.to_string(),
                to: next.to_string(),
            });
        }
        self.status = next;
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Returns true if this job should be moved to the Dead Letter Queue.
    pub fn should_move_to_dlq(&self) -> bool {
        self.attempt_count >= self.max_attempts
    }

    /// Returns true if this is a recurring job.
    pub fn is_recurring(&self) -> bool {
        self.job_type == JobType::Recurring && self.cron_expression.is_some()
    }
}

/// Lightweight view of a job used in list responses.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct JobSummary {
    pub id: Uuid,
    pub name: String,
    pub queue_id: Uuid,
    pub status: JobStatus,
    pub priority: JobPriority,
    pub job_type: JobType,
    pub attempt_count: i32,
    pub max_attempts: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Record of a single execution attempt.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct JobExecution {
    pub id: Uuid,
    pub job_id: Uuid,
    pub worker_id: Uuid,
    pub attempt_number: i32,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub success: bool,
    pub error_message: Option<String>,
    pub duration_ms: Option<i64>,
    /// Correlation ID — same as the parent job's.
    pub correlation_id: Uuid,
}

/// A single log line emitted during job execution.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct JobLog {
    pub id: Uuid,
    pub job_id: Uuid,
    pub execution_id: Option<Uuid>,
    pub level: String,
    pub message: String,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
}
