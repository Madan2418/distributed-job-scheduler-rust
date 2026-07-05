use sqlx::PgPool;
use uuid::Uuid;

use domain::entities::project::Project;
use domain::entities::user::ProjectMember;
use domain::enums::UserRole;

pub struct ProjectRepository {
    pub pool: PgPool,
}

impl ProjectRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn find_by_id(&self, id: Uuid) -> Result<Option<Project>, sqlx::Error> {
        sqlx::query_as(
            r#"SELECT id, organization_id, name, slug, description, created_at, updated_at
               FROM projects WHERE id = $1"#
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn create(
        &self,
        org_id: Uuid,
        name: &str,
        slug: &str,
        description: Option<&str>,
    ) -> Result<Project, sqlx::Error> {
        sqlx::query_as(
            r#"INSERT INTO projects (id, organization_id, name, slug, description)
               VALUES (gen_random_uuid(), $1, $2, $3, $4)
               RETURNING id, organization_id, name, slug, description, created_at, updated_at"#
        )
        .bind(org_id)
        .bind(name)
        .bind(slug)
        .bind(description)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn get_member_role(
        &self,
        project_id: Uuid,
        user_id: Uuid,
    ) -> Result<Option<UserRole>, sqlx::Error> {
        let row: Option<(UserRole,)> = sqlx::query_as(
            "SELECT role FROM project_members WHERE project_id = $1 AND user_id = $2"
        )
        .bind(project_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn add_member(
        &self,
        project_id: Uuid,
        user_id: Uuid,
        role: &UserRole,
    ) -> Result<ProjectMember, sqlx::Error> {
        sqlx::query_as(
            r#"INSERT INTO project_members (id, project_id, user_id, role)
               VALUES (gen_random_uuid(), $1, $2, $3::user_role)
               RETURNING id, project_id, user_id, role, created_at"#
        )
        .bind(project_id)
        .bind(user_id)
        .bind(role.to_string())
        .fetch_one(&self.pool)
        .await
    }

    pub async fn list_by_user(&self, user_id: Uuid) -> Result<Vec<Project>, sqlx::Error> {
        sqlx::query_as(
            r#"SELECT p.id, p.organization_id, p.name, p.slug, p.description,
                      p.created_at, p.updated_at
               FROM projects p
               JOIN project_members pm ON pm.project_id = p.id
               WHERE pm.user_id = $1 ORDER BY p.name"#
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
    }
}
