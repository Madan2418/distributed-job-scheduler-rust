use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::enums::BackoffStrategy;

/// Configurable retry policy. Stored separately so multiple queues can share one.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct RetryPolicy {
    pub id: Uuid,
    pub name: String,
    pub max_attempts: i32,
    pub backoff_strategy: BackoffStrategy,
    /// Base delay in seconds (meaning varies by strategy).
    pub base_delay_seconds: i64,
    /// Max delay cap for exponential backoff (seconds).
    pub max_delay_seconds: Option<i64>,
    /// Multiplier for exponential backoff.
    pub multiplier: Option<f64>,
    pub created_at: DateTime<Utc>,
}

impl RetryPolicy {
    /// Strategy Pattern: calculate next delay in seconds given the current attempt number.
    pub fn next_delay_seconds(&self, attempt: i32) -> i64 {
        let base = self.base_delay_seconds;
        let delay = match self.backoff_strategy {
            BackoffStrategy::Fixed => base,
            BackoffStrategy::Linear => base * attempt as i64,
            BackoffStrategy::Exponential => {
                let mult = self.multiplier.unwrap_or(2.0);
                let raw = (base as f64 * mult.powi(attempt - 1)) as i64;
                if let Some(max) = self.max_delay_seconds {
                    raw.min(max)
                } else {
                    raw
                }
            }
        };
        delay
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_policy(strategy: BackoffStrategy) -> RetryPolicy {
        RetryPolicy {
            id: Uuid::new_v4(),
            name: "test".into(),
            max_attempts: 5,
            backoff_strategy: strategy,
            base_delay_seconds: 10,
            max_delay_seconds: Some(300),
            multiplier: Some(2.0),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn fixed_strategy_always_same_delay() {
        let p = make_policy(BackoffStrategy::Fixed);
        assert_eq!(p.next_delay_seconds(1), 10);
        assert_eq!(p.next_delay_seconds(3), 10);
        assert_eq!(p.next_delay_seconds(5), 10);
    }

    #[test]
    fn linear_strategy_grows_linearly() {
        let p = make_policy(BackoffStrategy::Linear);
        assert_eq!(p.next_delay_seconds(1), 10);
        assert_eq!(p.next_delay_seconds(2), 20);
        assert_eq!(p.next_delay_seconds(4), 40);
    }

    #[test]
    fn exponential_strategy_doubles_each_attempt() {
        let p = make_policy(BackoffStrategy::Exponential);
        assert_eq!(p.next_delay_seconds(1), 10);   // 10 * 2^0
        assert_eq!(p.next_delay_seconds(2), 20);   // 10 * 2^1
        assert_eq!(p.next_delay_seconds(3), 40);   // 10 * 2^2
    }

    #[test]
    fn exponential_caps_at_max_delay() {
        let p = make_policy(BackoffStrategy::Exponential);
        // 10 * 2^10 = 10240, but max is 300
        assert_eq!(p.next_delay_seconds(10), 300);
    }
}
