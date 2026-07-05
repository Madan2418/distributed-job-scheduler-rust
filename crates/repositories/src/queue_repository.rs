use sqlx::PgPool;
use uuid::Uuid;

use domain::entities::queue::{Queue, QueueStats};

pub struct QueueRepository {
    pub pool: PgPool,
}

impl QueueRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn find_by_id(&self, id: Uuid) -> Result<Option<Queue>, sqlx::Error> {
        sqlx::query_as(
            r#"SELECT id, project_id, name, description, concurrency_limit,
                      retry_policy_id, is_paused, shard, created_at, updated_at
               FROM queues WHERE id = $1"#
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn list_by_project(&self, project_id: Uuid) -> Result<Vec<Queue>, sqlx::Error> {
        sqlx::query_as(
            r#"SELECT id, project_id, name, description, concurrency_limit,
                      retry_policy_id, is_paused, shard, created_at, updated_at
               FROM queues WHERE project_id = $1 ORDER BY name"#
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn create(
        &self,
        project_id: Uuid,
        name: &str,
        description: Option<&str>,
        concurrency_limit: i32,
        retry_policy_id: Option<Uuid>,
    ) -> Result<Queue, sqlx::Error> {
        sqlx::query_as(
            r#"INSERT INTO queues (id, project_id, name, description, concurrency_limit, retry_policy_id)
               VALUES (gen_random_uuid(), $1, $2, $3, $4, $5)
               RETURNING id, project_id, name, description, concurrency_limit,
                         retry_policy_id, is_paused, shard, created_at, updated_at"#
        )
        .bind(project_id)
        .bind(name)
        .bind(description)
        .bind(concurrency_limit)
        .bind(retry_policy_id)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn set_paused(&self, queue_id: Uuid, paused: bool) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE queues SET is_paused = $2, updated_at = NOW() WHERE id = $1")
            .bind(queue_id)
            .bind(paused)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_stats(&self, queue_id: Uuid) -> Result<QueueStats, sqlx::Error> {
        sqlx::query_as(
            r#"SELECT
                q.id AS queue_id,
                q.name AS queue_name,
                COUNT(CASE WHEN j.status = 'queued'    THEN 1 END) AS queued_count,
                COUNT(CASE WHEN j.status = 'running'   THEN 1 END) AS running_count,
                COUNT(CASE WHEN j.status = 'failed'    THEN 1 END) AS failed_count,
                COUNT(CASE WHEN j.status = 'completed' THEN 1 END) AS completed_count,
                COUNT(d.id)                                         AS dlq_count
               FROM queues q
               LEFT JOIN jobs j ON j.queue_id = q.id
               LEFT JOIN dead_letter_queue d ON d.queue_id = q.id
               WHERE q.id = $1
               GROUP BY q.id, q.name"#
        )
        .bind(queue_id)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn count_running_jobs(&self, queue_id: Uuid) -> Result<i64, sqlx::Error> {
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM jobs WHERE queue_id = $1 AND status = 'running'"
        )
        .bind(queue_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(count.0)
    }
}
