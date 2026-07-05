use serde::{Deserialize, Serialize};

/// All possible states in the job lifecycle state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "job_status", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Queued,
    Claimed,
    Running,
    Completed,
    Scheduled,
    Failed,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStatus::Pending   => write!(f, "pending"),
            JobStatus::Queued    => write!(f, "queued"),
            JobStatus::Claimed   => write!(f, "claimed"),
            JobStatus::Running   => write!(f, "running"),
            JobStatus::Completed => write!(f, "completed"),
            JobStatus::Scheduled => write!(f, "scheduled"),
            JobStatus::Failed    => write!(f, "failed"),
        }
    }
}

/// Job type — drives which factory path is used during creation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "job_type", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum JobType {
    Immediate,
    Delayed,
    Scheduled,
    Recurring,
    Batch,
}

impl std::fmt::Display for JobType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobType::Immediate  => write!(f, "immediate"),
            JobType::Delayed    => write!(f, "delayed"),
            JobType::Scheduled  => write!(f, "scheduled"),
            JobType::Recurring  => write!(f, "recurring"),
            JobType::Batch      => write!(f, "batch"),
        }
    }
}

/// Priority levels — higher priority jobs are claimed first.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "job_priority", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum JobPriority {
    Low,
    Normal,
    High,
    Critical,
}

impl Default for JobPriority {
    fn default() -> Self { JobPriority::Normal }
}

impl std::fmt::Display for JobPriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobPriority::Low      => write!(f, "low"),
            JobPriority::Normal   => write!(f, "normal"),
            JobPriority::High     => write!(f, "high"),
            JobPriority::Critical => write!(f, "critical"),
        }
    }
}

/// Retry backoff strategies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "backoff_strategy", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum BackoffStrategy {
    Fixed,
    Linear,
    Exponential,
}

/// User roles for RBAC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "user_role", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum UserRole {
    Admin,
    Member,
    Viewer,
}

impl Default for UserRole {
    fn default() -> Self { UserRole::Member }
}

impl std::fmt::Display for UserRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UserRole::Admin  => write!(f, "admin"),
            UserRole::Member => write!(f, "member"),
            UserRole::Viewer => write!(f, "viewer"),
        }
    }
}

/// Outbox event types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "outbox_event_type", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum OutboxEventType {
    JobCompleted,
    JobFailed,
    JobRetryScheduled,
    JobMovedToDlq,
    JobClaimed,
    QueuePaused,
    QueueResumed,
}

impl std::fmt::Display for OutboxEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutboxEventType::JobCompleted      => write!(f, "job_completed"),
            OutboxEventType::JobFailed         => write!(f, "job_failed"),
            OutboxEventType::JobRetryScheduled => write!(f, "job_retry_scheduled"),
            OutboxEventType::JobMovedToDlq     => write!(f, "job_moved_to_dlq"),
            OutboxEventType::JobClaimed        => write!(f, "job_claimed"),
            OutboxEventType::QueuePaused       => write!(f, "queue_paused"),
            OutboxEventType::QueueResumed      => write!(f, "queue_resumed"),
        }
    }
}
