use sqlx::PgPool;
use uuid::Uuid;

use domain::entities::user::{RefreshToken, User};

pub struct UserRepository {
    pub pool: PgPool,
}

impl UserRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn find_by_email(&self, email: &str) -> Result<Option<User>, sqlx::Error> {
        sqlx::query_as(
            r#"SELECT id, email, password_hash, display_name, is_active, created_at, updated_at
               FROM users WHERE email = $1"#
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn find_by_id(&self, id: Uuid) -> Result<Option<User>, sqlx::Error> {
        sqlx::query_as(
            r#"SELECT id, email, password_hash, display_name, is_active, created_at, updated_at
               FROM users WHERE id = $1"#
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn create(
        &self,
        email: &str,
        password_hash: &str,
        display_name: Option<&str>,
    ) -> Result<User, sqlx::Error> {
        sqlx::query_as(
            r#"INSERT INTO users (id, email, password_hash, display_name)
               VALUES (gen_random_uuid(), $1, $2, $3)
               RETURNING id, email, password_hash, display_name, is_active, created_at, updated_at"#
        )
        .bind(email)
        .bind(password_hash)
        .bind(display_name)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn store_refresh_token(&self, token: &RefreshToken) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at)
               VALUES ($1, $2, $3, $4)"#
        )
        .bind(token.id)
        .bind(token.user_id)
        .bind(&token.token_hash)
        .bind(token.expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Return all non-revoked, non-expired refresh tokens.
    /// Used by AuthService.refresh() and AuthService.logout() to locate a token by value.
    pub async fn find_all_active_refresh_tokens(&self) -> Result<Vec<RefreshToken>, sqlx::Error> {
        sqlx::query_as(
            r#"SELECT id, user_id, token_hash, expires_at, revoked, created_at
               FROM refresh_tokens
               WHERE revoked = false AND expires_at > NOW()
               ORDER BY created_at DESC
               LIMIT 1000"#,
        )
        .fetch_all(&self.pool)
        .await
    }

    pub async fn find_refresh_token(&self, token_hash: &str) -> Result<Option<RefreshToken>, sqlx::Error> {
        sqlx::query_as(
            r#"SELECT id, user_id, token_hash, expires_at, revoked, created_at
               FROM refresh_tokens WHERE token_hash = $1 AND revoked = false"#
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn revoke_refresh_token(&self, token_hash: &str) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE refresh_tokens SET revoked = true WHERE token_hash = $1")
            .bind(token_hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
