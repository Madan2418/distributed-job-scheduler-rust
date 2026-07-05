use axum::{extract::State, Json};

use crate::app_state::AppState;
use repositories::worker_repository::WorkerRepository;

/// GET /health — overall health check
pub async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

/// GET /ready — readiness: DB must be reachable
pub async fn ready(State(state): State<AppState>) -> Json<serde_json::Value> {
    let db_ok = sqlx::query("SELECT 1")
        .execute(&state.pool)
        .await
        .is_ok();

    let status = if db_ok { "ready" } else { "not_ready" };
    Json(serde_json::json!({ "status": status, "db": db_ok }))
}

/// GET /live — liveness: process is alive
pub async fn live() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "alive" }))
}

/// GET /v1/metrics — dashboard metrics: jobs by status + DLQ count
pub async fn metrics(State(state): State<AppState>) -> Json<serde_json::Value> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT status::text, COUNT(*) FROM jobs GROUP BY status"
    )
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();

    let mut result = serde_json::Map::new();
    for (status, count) in rows {
        result.insert(format!("{}_jobs", status), count.into());
    }

    let dlq_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM dead_letter_queue")
        .fetch_one(&state.pool)
        .await
        .unwrap_or((0,));

    result.insert("dlq_count".into(), dlq_count.0.into());

    // Also include active worker count
    let worker_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM workers WHERE is_active = true"
    )
    .fetch_one(&state.pool)
    .await
    .unwrap_or((0,));

    result.insert("active_workers".into(), worker_count.0.into());

    Json(serde_json::Value::Object(result))
}

/// GET /v1/workers — list active workers and their heartbeat status
pub async fn list_workers(State(state): State<AppState>) -> Json<serde_json::Value> {
    let repo = WorkerRepository::new(state.pool.clone());
    let heartbeats = repo.list_active().await.unwrap_or_default();

    let workers: Vec<serde_json::Value> = heartbeats
        .iter()
        .map(|hb| serde_json::json!({
            "worker_id": hb.worker_id,
            "last_seen": hb.last_seen,
            "active_jobs": hb.active_jobs,
        }))
        .collect();

    Json(serde_json::json!({ "workers": workers }))
}
