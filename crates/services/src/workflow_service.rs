use sqlx::PgPool;
use uuid::Uuid;

/// Workflow / Saga service.
/// Manages job dependency DAGs and triggers compensation on failure.
pub struct WorkflowService {
    pool: PgPool,
}

impl WorkflowService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// After Job A completes, check if any jobs depending only on A are now ready.
    pub async fn on_job_completed(&self, completed_job_id: Uuid) -> Result<(), sqlx::Error> {
        let dependents: Vec<(Uuid,)> = sqlx::query_as(
            "SELECT dependent_job_id FROM job_dependencies WHERE dependency_job_id = $1"
        )
        .bind(completed_job_id)
        .fetch_all(&self.pool)
        .await?;

        for (job_id,) in dependents {
            let unsatisfied: (i64,) = sqlx::query_as(
                r#"SELECT COUNT(*) FROM job_dependencies jd
                   JOIN jobs j ON j.id = jd.dependency_job_id
                   WHERE jd.dependent_job_id = $1 AND j.status != 'completed'"#
            )
            .bind(job_id)
            .fetch_one(&self.pool)
            .await?;

            if unsatisfied.0 == 0 {
                sqlx::query(
                    "UPDATE jobs SET status = 'queued', updated_at = NOW() WHERE id = $1 AND status = 'pending'"
                )
                .bind(job_id)
                .execute(&self.pool)
                .await?;

                tracing::info!(job_id = %job_id, "Workflow: all dependencies satisfied, Pending → Queued");
            }
        }

        Ok(())
    }

    /// On job failure: log compensation requirements for upstream Saga steps.
    pub async fn on_job_failed(&self, failed_job_id: Uuid) -> Result<(), sqlx::Error> {
        let upstream: Vec<(Uuid, String)> = sqlx::query_as(
            r#"SELECT jd.dependency_job_id, j.name
               FROM job_dependencies jd
               JOIN jobs j ON j.id = jd.dependency_job_id
               WHERE jd.dependent_job_id = $1 AND j.status = 'completed'"#
        )
        .bind(failed_job_id)
        .fetch_all(&self.pool)
        .await?;

        for (upstream_id, name) in &upstream {
            tracing::warn!(
                failed_job = %failed_job_id,
                upstream_job = %upstream_id,
                upstream_name = %name,
                "Saga: compensation required for upstream completed job"
            );
        }

        Ok(())
    }

    pub async fn create_dependency(
        &self,
        dependent_job_id: Uuid,
        dependency_job_id: Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"INSERT INTO job_dependencies (id, dependent_job_id, dependency_job_id)
               VALUES (gen_random_uuid(), $1, $2) ON CONFLICT DO NOTHING"#
        )
        .bind(dependent_job_id)
        .bind(dependency_job_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
