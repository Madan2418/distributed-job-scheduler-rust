use sqlx::PgPool;
use uuid::Uuid;

use domain::entities::worker::{Worker, WorkerHeartbeat};

pub struct WorkerRepository {
    pub pool: PgPool,
}

impl WorkerRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn register(&self, worker_id: Uuid, name: &str, hostname: &str) -> Result<Worker, sqlx::Error> {
        sqlx::query_as(
            r#"INSERT INTO workers (id, name, hostname, is_active)
               VALUES ($1, $2, $3, true)
               ON CONFLICT (id) DO UPDATE SET is_active = true, deregistered_at = NULL
               RETURNING id, name, hostname, is_active, registered_at, deregistered_at"#
        )
        .bind(worker_id)
        .bind(name)
        .bind(hostname)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn deregister(&self, worker_id: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE workers SET is_active = false, deregistered_at = NOW() WHERE id = $1"
        )
        .bind(worker_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_heartbeat(&self, worker_id: Uuid, active_jobs: i32) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"INSERT INTO worker_heartbeats (id, worker_id, last_seen, active_jobs)
               VALUES (gen_random_uuid(), $1, NOW(), $2)
               ON CONFLICT (worker_id) DO UPDATE SET last_seen = NOW(), active_jobs = $2"#
        )
        .bind(worker_id)
        .bind(active_jobs)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_active(&self) -> Result<Vec<WorkerHeartbeat>, sqlx::Error> {
        sqlx::query_as(
            "SELECT id, worker_id, last_seen, active_jobs FROM worker_heartbeats ORDER BY last_seen DESC"
        )
        .fetch_all(&self.pool)
        .await
    }
}
