use thiserror::Error;

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("Invalid job state transition from {from} to {to}")]
    InvalidStateTransition { from: String, to: String },

    #[error("Job has exceeded maximum retry attempts ({max})")]
    MaxAttemptsExceeded { max: i32 },

    #[error("Queue concurrency limit reached: {limit}")]
    QueueConcurrencyLimitReached { limit: i32 },

    #[error("Queue is paused")]
    QueuePaused,

    #[error("Invalid cron expression: {expr}")]
    InvalidCronExpression { expr: String },

    #[error("Retry policy not found for job")]
    RetryPolicyNotFound,

    #[error("Workflow dependency cycle detected")]
    WorkflowCyclicDependency,

    #[error("Validation error: {0}")]
    Validation(String),
}
