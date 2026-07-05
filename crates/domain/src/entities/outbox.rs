use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::enums::OutboxEventType;

/// Outbox Pattern: event written in the SAME transaction as the state change.
/// A background relay reads unpublished rows and publishes to Redis Pub/Sub.
/// This prevents dual-write bugs between PostgreSQL and Redis.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct OutboxEvent {
    pub id: Uuid,
    pub event_type: OutboxEventType,
    pub aggregate_id: Uuid,
    pub payload: Value,
    pub published: bool,
    pub published_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}

impl OutboxEvent {
    pub fn new(
        event_type: OutboxEventType,
        aggregate_id: Uuid,
        payload: Value,
        correlation_id: Uuid,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            event_type,
            aggregate_id,
            payload,
            published: false,
            published_at: None,
            created_at: Utc::now(),
            correlation_id,
        }
    }
}
