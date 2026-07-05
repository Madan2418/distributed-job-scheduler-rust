use bcrypt::{hash, verify, DEFAULT_COST};
use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use domain::entities::user::{RefreshToken, User};
use repositories::user_repository::UserRepository;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,        // user_id
    pub email: String,
    pub exp: usize,
    pub iat: usize,
}

pub struct AuthService {
    user_repo: UserRepository,
    jwt_secret: String,
    access_token_ttl_minutes: i64,
}

impl AuthService {
    pub fn new(pool: PgPool, jwt_secret: String) -> Self {
        Self {
            user_repo: UserRepository::new(pool),
            jwt_secret,
            access_token_ttl_minutes: 15,
        }
    }

    pub async fn register(
        &self,
        email: &str,
        password: &str,
        display_name: Option<&str>,
    ) -> Result<User, AuthError> {
        if self.user_repo.find_by_email(email).await?.is_some() {
            return Err(AuthError::EmailAlreadyRegistered);
        }
        let password_hash = hash(password, DEFAULT_COST)
            .map_err(|e| AuthError::Internal(e.to_string()))?;
        let user = self.user_repo.create(email, &password_hash, display_name).await?;
        Ok(user)
    }

    pub async fn login(
        &self,
        email: &str,
        password: &str,
    ) -> Result<(String, String, User), AuthError> {
        let user = self
            .user_repo
            .find_by_email(email)
            .await?
            .ok_or(AuthError::InvalidCredentials)?;

        let valid = verify(password, &user.password_hash)
            .map_err(|_| AuthError::InvalidCredentials)?;

        if !valid {
            return Err(AuthError::InvalidCredentials);
        }

        let access_token = self.mint_access_token(&user)?;
        let refresh_token = self.store_refresh_token(&user).await?;

        Ok((access_token, refresh_token, user))
    }

    /// Validate a refresh token and issue a new access token.
    /// The raw refresh token string is hashed and looked up in the DB.
    pub async fn refresh(&self, raw_refresh_token: &str) -> Result<(String, User), AuthError> {
        // Hash the incoming token and look it up
        let records = self.user_repo.find_all_active_refresh_tokens().await?;

        // Linear scan — acceptable because refresh tokens are rare and N is small per user
        let record = records
            .into_iter()
            .find(|r| bcrypt::verify(raw_refresh_token, &r.token_hash).unwrap_or(false))
            .ok_or(AuthError::InvalidToken)?;

        if record.expires_at < Utc::now() {
            return Err(AuthError::InvalidToken);
        }

        let user = self
            .user_repo
            .find_by_id(record.user_id)
            .await?
            .ok_or(AuthError::InvalidToken)?;

        let access_token = self.mint_access_token(&user)?;
        Ok((access_token, user))
    }

    /// Revoke a refresh token (logout from a single device).
    pub async fn logout(&self, raw_refresh_token: &str) -> Result<(), AuthError> {
        // Find matching token record by verifying bcrypt hash
        let records = self.user_repo.find_all_active_refresh_tokens().await?;

        let record = records
            .into_iter()
            .find(|r| bcrypt::verify(raw_refresh_token, &r.token_hash).unwrap_or(false))
            .ok_or(AuthError::InvalidToken)?;

        self.user_repo.revoke_refresh_token(&record.token_hash).await?;
        Ok(())
    }

    fn mint_access_token(&self, user: &User) -> Result<String, AuthError> {
        let now = Utc::now();
        let exp = (now + Duration::minutes(self.access_token_ttl_minutes)).timestamp() as usize;
        let claims = Claims {
            sub: user.id.to_string(),
            email: user.email.clone(),
            exp,
            iat: now.timestamp() as usize,
        };
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.jwt_secret.as_bytes()),
        )
        .map_err(|e| AuthError::Internal(e.to_string()))
    }

    async fn store_refresh_token(&self, user: &User) -> Result<String, AuthError> {
        let raw_token = Uuid::new_v4().to_string();
        let token_hash = bcrypt::hash(&raw_token, 4)
            .map_err(|e| AuthError::Internal(e.to_string()))?;

        let record = RefreshToken {
            id: Uuid::new_v4(),
            user_id: user.id,
            token_hash,
            expires_at: Utc::now() + Duration::days(30),
            revoked: false,
            created_at: Utc::now(),
        };
        self.user_repo.store_refresh_token(&record).await?;
        Ok(raw_token)
    }

    pub fn verify_access_token(&self, token: &str) -> Result<Claims, AuthError> {
        decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.jwt_secret.as_bytes()),
            &Validation::default(),
        )
        .map(|d| d.claims)
        .map_err(|_| AuthError::InvalidToken)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("Invalid credentials")]
    InvalidCredentials,
    #[error("Email already registered")]
    EmailAlreadyRegistered,
    #[error("Invalid or expired token")]
    InvalidToken,
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Internal error: {0}")]
    Internal(String),
}
