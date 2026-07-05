use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};

use crate::app_state::AppState;
use common::errors::{AppError, AppResult};

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub email: String,
    pub password: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

#[derive(Debug, Deserialize)]
pub struct LogoutRequest {
    pub refresh_token: String,
}

#[derive(Debug, Serialize)]
pub struct AuthResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub user_id: String,
    pub email: String,
}

/// POST /v1/auth/register
pub async fn register(
    State(state): State<AppState>,
    Json(req): Json<RegisterRequest>,
) -> AppResult<Json<serde_json::Value>> {
    let user = state
        .auth_service
        .register(&req.email, &req.password, req.display_name.as_deref())
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;

    Ok(Json(serde_json::json!({
        "user_id": user.id,
        "email": user.email,
        "message": "Registration successful"
    })))
}

/// POST /v1/auth/login
pub async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> AppResult<Json<AuthResponse>> {
    let (access_token, refresh_token, user) = state
        .auth_service
        .login(&req.email, &req.password)
        .await
        .map_err(|_| AppError::Unauthorized)?;

    Ok(Json(AuthResponse {
        access_token,
        refresh_token,
        user_id: user.id.to_string(),
        email: user.email,
    }))
}

/// POST /v1/auth/refresh — exchange a valid refresh token for a new access token
pub async fn refresh(
    State(state): State<AppState>,
    Json(req): Json<RefreshRequest>,
) -> AppResult<Json<serde_json::Value>> {
    let (access_token, user) = state
        .auth_service
        .refresh(&req.refresh_token)
        .await
        .map_err(|_| AppError::Unauthorized)?;

    Ok(Json(serde_json::json!({
        "access_token": access_token,
        "user_id": user.id,
        "email": user.email,
    })))
}

/// POST /v1/auth/logout — revoke the supplied refresh token
pub async fn logout(
    State(state): State<AppState>,
    Json(req): Json<LogoutRequest>,
) -> AppResult<Json<serde_json::Value>> {
    state
        .auth_service
        .logout(&req.refresh_token)
        .await
        .map_err(|_| AppError::Unauthorized)?;

    Ok(Json(serde_json::json!({ "message": "Logged out successfully" })))
}
