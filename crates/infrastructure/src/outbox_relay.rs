use redis::aio::ConnectionManager;
use serde_json::json;
use sqlx::PgPool;
use tokio::time::{interval, Duration};
use tracing::{error, info};

use crate::redis_client::{publish, DLQ_CHANNEL, JOBS_CHANNEL};

/// Outbox Relay: polls the `outbox_events` table for unpublished events
/// and publishes them to Redis Pub/Sub. Runs as a background Tokio task.
pub async fn run_relay(db: PgPool, mut redis: ConnectionManager) {
    let mut ticker = interval(Duration::from_millis(500));
    info!("Outbox relay started");

    loop {
        ticker.tick().await;

        let rows: Result<Vec<(uuid::Uuid, String, uuid::Uuid, serde_json::Value, uuid::Uuid)>, _> =
            sqlx::query_as(
                r#"SELECT id, event_type::text, aggregate_id, payload, correlation_id
                   FROM outbox_events
                   WHERE published = false
                   ORDER BY created_at ASC
                   LIMIT 50"#,
            )
            .fetch_all(&db)
            .await;

        match rows {
            Err(e) => {
                error!(error = %e, "Outbox relay: failed to fetch events");
                continue;
            }
            Ok(events) => {
                for (id, event_type, aggregate_id, payload, correlation_id) in &events {
                    let channel = if event_type == "job_moved_to_dlq" {
                        DLQ_CHANNEL
                    } else {
                        JOBS_CHANNEL
                    };

                    let msg = json!({
                        "event_type": event_type,
                        "aggregate_id": aggregate_id,
                        "payload": payload,
                        "correlation_id": correlation_id,
                    })
                    .to_string();

                    if let Err(e) = publish(&mut redis, channel, &msg).await {
                        error!(error = %e, "Outbox relay: failed to publish event");
                        continue;
                    }

                    if let Err(e) = sqlx::query(
                        "UPDATE outbox_events SET published = true, published_at = NOW() WHERE id = $1"
                    )
                    .bind(id)
                    .execute(&db)
                    .await
                    {
                        error!(error = %e, "Outbox relay: failed to mark event published");
                    }
                }
            }
        }
    }
}
