use axum::{
    extract::{Extension, Path, Query, State},
    Json,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::app_state::AppState;
use common::errors::{AppError, AppResult};
use services::{auth_service::Claims, dlq_service::DlqServiceError};

#[derive(Debug, Deserialize)]
pub struct ListDlqQuery {
    pub project_id: Uuid,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    20
}

/// GET /v1/dlq
pub async fn list_dlq(
    State(state): State<AppState>,
    Query(params): Query<ListDlqQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let entries = state
        .dlq_service
        .list(params.project_id, params.limit, params.offset)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;

    Ok(Json(serde_json::json!({ "entries": entries })))
}

/// POST /v1/dlq/:dlq_id/retry
pub async fn retry_dlq_job(
    State(state): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(dlq_id): Path<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let retried_by = Uuid::parse_str(&claims.sub).map_err(|_| AppError::Unauthorized)?;

    state
        .dlq_service
        .retry_job(dlq_id, retried_by)
        .await
        .map_err(map_retry_error)?;

    Ok(Json(serde_json::json!({ "status": "requeued", "dlq_id": dlq_id })))
}

fn map_retry_error(error: DlqServiceError) -> AppError {
    match error {
        DlqServiceError::DlqEntryNotFound(dlq_id) => {
            AppError::NotFound(format!("DLQ entry {} not found", dlq_id))
        }
        DlqServiceError::AlreadyRetried(dlq_id) => {
            AppError::Conflict(format!("DLQ entry {} was already retried", dlq_id))
        }
        DlqServiceError::JobNotFound(job_id) => {
            AppError::NotFound(format!("Original job {} not found", job_id))
        }
        DlqServiceError::Forbidden => {
            AppError::Forbidden("User is not allowed to retry this DLQ entry".to_string())
        }
        DlqServiceError::Database(error) => AppError::Database(error),
    }
}
