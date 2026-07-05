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
pub struct CreateDependencyRequest {
    /// The job that depends on the other.
    pub dependent_job_id: Uuid,
    /// The job that must complete first.
    pub dependency_job_id: Uuid,
}

/// POST /v1/workflows/dependencies — register a DAG edge between two jobs
pub async fn create_dependency(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Json(req): Json<CreateDependencyRequest>,
) -> AppResult<(StatusCode, Json<serde_json::Value>)> {
    // Ensure the caller is a project member (check via dependent job's project)
    let user_id = Uuid::parse_str(&claims.sub).map_err(|_| AppError::Unauthorized)?;

    // Verify both jobs exist and the user can access the project
    let dependent_job = sqlx::query_as::<_, (Uuid,)>(
        "SELECT project_id FROM jobs WHERE id = $1"
    )
    .bind(req.dependent_job_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(AppError::Database)?
    .ok_or_else(|| AppError::NotFound(format!("Job {} not found", req.dependent_job_id)))?;

    // Verify user membership in that project
    let member: Option<(String,)> = sqlx::query_as(
        "SELECT role::text FROM project_members WHERE project_id = $1 AND user_id = $2"
    )
    .bind(dependent_job.0)
    .bind(user_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(AppError::Database)?;

    if member.is_none() {
        return Err(AppError::Forbidden("User is not a member of this project".into()));
    }

    state
        .workflow_service
        .create_dependency(req.dependent_job_id, req.dependency_job_id)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "dependent_job_id": req.dependent_job_id,
            "dependency_job_id": req.dependency_job_id,
            "message": "Dependency registered",
        })),
    ))
}
