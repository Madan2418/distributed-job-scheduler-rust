use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::app_state::AppState;
use common::errors::{AppError, AppResult};
use services::{auth_service::Claims, queue_service::QueueServiceError};

#[derive(Debug, Deserialize)]
pub struct CreateQueueRequest {
    pub project_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub concurrency_limit: Option<i32>,
    pub retry_policy_id: Option<Uuid>,
}

/// GET /v1/projects/:project_id/queues
pub async fn list_queues(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(project_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let user_id = Uuid::parse_str(&claims.sub).map_err(|_| AppError::Unauthorized)?;
    let queues = state
        .queue_service
        .list_by_project(user_id, project_id)
        .await
        .map_err(map_queue_error)?;

    Ok(Json(serde_json::json!({ "queues": queues })))
}

/// POST /v1/queues
pub async fn create_queue(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Json(req): Json<CreateQueueRequest>,
) -> AppResult<(StatusCode, Json<serde_json::Value>)> {
    let user_id = Uuid::parse_str(&claims.sub).map_err(|_| AppError::Unauthorized)?;
    let queue = state
        .queue_service
        .create_queue(
            user_id,
            req.project_id,
            &req.name,
            req.description.as_deref(),
            req.concurrency_limit.unwrap_or(10),
            req.retry_policy_id,
        )
        .await
        .map_err(map_queue_error)?;

    Ok((StatusCode::CREATED, Json(serde_json::to_value(&queue).unwrap())))
}

/// GET /v1/queues/:queue_id/stats
pub async fn queue_stats(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(queue_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let user_id = Uuid::parse_str(&claims.sub).map_err(|_| AppError::Unauthorized)?;
    let stats = state
        .queue_service
        .get_stats(user_id, queue_id)
        .await
        .map_err(map_queue_error)?;

    Ok(Json(serde_json::to_value(&stats).unwrap()))
}

/// POST /v1/queues/:queue_id/pause
pub async fn pause_queue(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(queue_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let user_id = Uuid::parse_str(&claims.sub).map_err(|_| AppError::Unauthorized)?;
    state
        .queue_service
        .pause_queue(user_id, queue_id)
        .await
        .map_err(map_queue_error)?;

    Ok(Json(serde_json::json!({ "status": "paused", "queue_id": queue_id })))
}

/// POST /v1/queues/:queue_id/resume
pub async fn resume_queue(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(queue_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let user_id = Uuid::parse_str(&claims.sub).map_err(|_| AppError::Unauthorized)?;
    state
        .queue_service
        .resume_queue(user_id, queue_id)
        .await
        .map_err(map_queue_error)?;

    Ok(Json(serde_json::json!({ "status": "resumed", "queue_id": queue_id })))
}

fn map_queue_error(error: QueueServiceError) -> AppError {
    match error {
        QueueServiceError::QueueNotFound(queue_id) => {
            AppError::NotFound(format!("Queue {} not found", queue_id))
        }
        QueueServiceError::Forbidden => {
            AppError::Forbidden("User is not allowed to access this queue".to_string())
        }
        QueueServiceError::Database(error) => AppError::Database(error),
    }
}
