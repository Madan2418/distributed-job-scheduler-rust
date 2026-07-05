use sqlx::PgPool;

use domain::entities::outbox::OutboxEvent;

pub struct OutboxRepository {
    pub pool: PgPool,
}

impl OutboxRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Insert an outbox event inside an existing transaction.
    /// Call this from the same transaction that updates job status.
    pub async fn insert_in_tx<'a>(
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        event: &OutboxEvent,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"INSERT INTO outbox_events
               (id, event_type, aggregate_id, payload, published, correlation_id, created_at)
               VALUES ($1, $2::outbox_event_type, $3, $4, false, $5, $6)"#
        )
        .bind(event.id)
        .bind(event.event_type.to_string())
        .bind(event.aggregate_id)
        .bind(&event.payload)
        .bind(event.correlation_id)
        .bind(event.created_at)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}
