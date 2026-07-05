use axum::{
    extract::{Extension, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::app_state::AppState;
use common::errors::{AppError, AppResult};
use services::auth_service::Claims;

#[derive(Debug, Deserialize)]
pub struct CreateProjectRequest {
    pub organization_name: String,
    pub organization_slug: String,
    pub name: String,
    pub slug: String,
    pub description: Option<String>,
}

/// GET /v1/projects
pub async fn list_projects(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
) -> AppResult<Json<serde_json::Value>> {
    let user_id = Uuid::parse_str(&claims.sub).map_err(|_| AppError::Unauthorized)?;
    let projects = state
        .project_service
        .list_for_user(user_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    Ok(Json(serde_json::json!({ "projects": projects })))
}

/// POST /v1/projects
pub async fn create_project(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Json(req): Json<CreateProjectRequest>,
) -> AppResult<(StatusCode, Json<serde_json::Value>)> {
    let user_id = Uuid::parse_str(&claims.sub).map_err(|_| AppError::Unauthorized)?;

    let project = state
        .project_service
        .create_project(
            user_id,
            &req.organization_name,
            &req.organization_slug,
            &req.name,
            &req.slug,
            req.description.as_deref(),
        )
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;

    Ok((StatusCode::CREATED, Json(serde_json::to_value(project).unwrap())))
}
