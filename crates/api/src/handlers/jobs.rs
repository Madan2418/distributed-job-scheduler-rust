use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::app_state::AppState;
use common::errors::{AppError, AppResult};
use services::{
    auth_service::Claims,
    job_service::{CreateJobRequest, JobServiceError},
};

#[derive(Debug, Deserialize)]
pub struct ListJobsQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    20
}

/// POST /v1/queues/:queue_id/jobs
pub async fn create_job(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(queue_id): Path<Uuid>,
    Json(req): Json<CreateJobRequest>,
) -> AppResult<(StatusCode, Json<serde_json::Value>)> {
    let user_id = Uuid::parse_str(&claims.sub).map_err(|_| AppError::Unauthorized)?;

    let job = state
        .job_service
        .create_job(queue_id, user_id, req)
        .await
        .map_err(map_create_job_error)?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "job_id": job.id,
            "correlation_id": job.correlation_id,
            "status": job.status,
            "name": job.name,
            "created_at": job.created_at,
        })),
    ))
}

fn map_create_job_error(error: JobServiceError) -> AppError {
    match error {
        JobServiceError::QueueNotFound(queue_id) => {
            AppError::NotFound(format!("Queue {} not found", queue_id))
        }
        JobServiceError::QueuePaused(queue_id) => {
            AppError::Conflict(format!("Queue {} is paused", queue_id))
        }
        JobServiceError::Forbidden => {
            AppError::Forbidden("User is not allowed to create jobs in this queue".to_string())
        }
        JobServiceError::InvalidCronExpression(expr) => {
            AppError::BadRequest(format!("Invalid cron expression: {}", expr))
        }
        JobServiceError::Database(error) => AppError::Database(error),
    }
}

/// GET /v1/queues/:queue_id/jobs
pub async fn list_jobs(
    State(state): State<AppState>,
    Path(queue_id): Path<Uuid>,
    Query(params): Query<ListJobsQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let jobs = state
        .job_service
        .list_jobs(queue_id, params.limit, params.offset)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    Ok(Json(serde_json::json!({
        "jobs": jobs,
        "limit": params.limit,
        "offset": params.offset,
    })))
}

/// GET /v1/jobs/:job_id
pub async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let job = state
        .job_service
        .get_job(job_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?
        .ok_or_else(|| AppError::NotFound(format!("Job {} not found", job_id)))?;

    Ok(Json(serde_json::to_value(&job).unwrap()))
}

/// GET /v1/jobs/:job_id/executions — execution history for a job
pub async fn list_job_executions(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let executions = state
        .job_service
        .list_executions(job_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    Ok(Json(serde_json::json!({ "executions": executions })))
}

/// POST /v1/jobs/:job_id/cancel — cancel a pending/queued job
pub async fn cancel_job(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(job_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let user_id = Uuid::parse_str(&claims.sub).map_err(|_| AppError::Unauthorized)?;

    state
        .job_service
        .cancel_job(job_id, user_id)
        .await
        .map_err(map_create_job_error)?;

    Ok(Json(serde_json::json!({ "status": "cancelled", "job_id": job_id })))
}
