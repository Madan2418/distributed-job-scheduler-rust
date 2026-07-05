use sqlx::PgPool;
use uuid::Uuid;

use domain::entities::project::Project;
use repositories::project_repository::ProjectRepository;

pub struct ProjectService {
    project_repo: ProjectRepository,
    pool: PgPool,
}

impl ProjectService {
    pub fn new(pool: PgPool) -> Self {
        Self {
            project_repo: ProjectRepository::new(pool.clone()),
            pool,
        }
    }

    pub async fn list_for_user(&self, user_id: Uuid) -> Result<Vec<Project>, sqlx::Error> {
        self.project_repo.list_by_user(user_id).await
    }

    pub async fn create_project(
        &self,
        user_id: Uuid,
        organization_name: &str,
        organization_slug: &str,
        name: &str,
        slug: &str,
        description: Option<&str>,
    ) -> Result<Project, sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        let organization_id: Uuid = sqlx::query_scalar(
            r#"INSERT INTO organizations (id, name, slug)
               VALUES (gen_random_uuid(), $1, $2)
               ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
               RETURNING id"#,
        )
        .bind(organization_name)
        .bind(organization_slug)
        .fetch_one(&mut *tx)
        .await?;

        let project: Project = sqlx::query_as(
            r#"INSERT INTO projects (id, organization_id, name, slug, description)
               VALUES (gen_random_uuid(), $1, $2, $3, $4)
               RETURNING id, organization_id, name, slug, description, created_at, updated_at"#,
        )
        .bind(organization_id)
        .bind(name)
        .bind(slug)
        .bind(description)
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query(
            r#"INSERT INTO project_members (id, project_id, user_id, role)
               VALUES (gen_random_uuid(), $1, $2, 'admin')
               ON CONFLICT (project_id, user_id) DO NOTHING"#,
        )
        .bind(project.id)
        .bind(user_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(project)
    }
}
