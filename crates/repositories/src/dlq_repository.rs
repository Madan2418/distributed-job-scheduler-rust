use sqlx::PgPool;
use uuid::Uuid;

use domain::entities::dlq::DeadLetterEntry;

pub struct DlqRepository {
    pub pool: PgPool,
}

impl DlqRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(&self, entry: &DeadLetterEntry) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"INSERT INTO dead_letter_queue
               (id, job_id, queue_id, project_id, job_name, payload,
                last_error, attempt_count, ai_summary, moved_to_dlq_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#
        )
        .bind(entry.id)
        .bind(entry.job_id)
        .bind(entry.queue_id)
        .bind(entry.project_id)
        .bind(&entry.job_name)
        .bind(&entry.payload)
        .bind(&entry.last_error)
        .bind(entry.attempt_count)
        .bind(&entry.ai_summary)
        .bind(entry.moved_to_dlq_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_by_project(
        &self,
        project_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<DeadLetterEntry>, sqlx::Error> {
        sqlx::query_as(
            r#"SELECT id, job_id, queue_id, project_id, job_name, payload,
                      last_error, attempt_count, ai_summary, moved_to_dlq_at,
                      manually_retried_at, manually_retried_by
               FROM dead_letter_queue
               WHERE project_id = $1
               ORDER BY moved_to_dlq_at DESC
               LIMIT $2 OFFSET $3"#
        )
        .bind(project_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn find_by_id(&self, dlq_id: Uuid) -> Result<Option<DeadLetterEntry>, sqlx::Error> {
        sqlx::query_as(
            r#"SELECT id, job_id, queue_id, project_id, job_name, payload,
                      last_error, attempt_count, ai_summary, moved_to_dlq_at,
                      manually_retried_at, manually_retried_by
               FROM dead_letter_queue
               WHERE id = $1"#,
        )
        .bind(dlq_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn mark_retried(&self, dlq_id: Uuid, retried_by: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"UPDATE dead_letter_queue
               SET manually_retried_at = NOW(), manually_retried_by = $2
               WHERE id = $1"#
        )
        .bind(dlq_id)
        .bind(retried_by)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
