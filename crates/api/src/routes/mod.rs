use axum::{
    middleware,
    routing::{get, post},
    Router,
};

use crate::app_state::AppState;
use crate::handlers::{auth, dlq, health, jobs, projects, queues, websocket, workflows};
use crate::middleware::{
    auth_middleware::auth_middleware,
    correlation_id::correlation_id_middleware,
    rate_limit::rate_limit_middleware,
};

pub fn build_router(state: AppState) -> Router {
    // Public routes (no auth required)
    let public = Router::new()
        .route("/health", get(health::health))
        .route("/ready", get(health::ready))
        .route("/live", get(health::live))
        .route("/v1/auth/register", post(auth::register))
        .route("/v1/auth/login", post(auth::login))
        // Refresh and logout require the raw refresh token body, not a Bearer JWT
        .route("/v1/auth/refresh", post(auth::refresh))
        .route("/v1/auth/logout", post(auth::logout))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            rate_limit_middleware,
        ));

    // Protected routes — require valid JWT
    let protected = Router::new()
        // Jobs
        .route("/v1/queues/{queue_id}/jobs", post(jobs::create_job).get(jobs::list_jobs))
        .route("/v1/jobs/{job_id}", get(jobs::get_job))
        .route("/v1/jobs/{job_id}/executions", get(jobs::list_job_executions))
        .route("/v1/jobs/{job_id}/cancel", post(jobs::cancel_job))
        // Projects
        .route("/v1/projects", get(projects::list_projects).post(projects::create_project))
        .route("/v1/projects/{project_id}/queues", get(queues::list_queues))
        // Queues
        .route("/v1/queues", post(queues::create_queue))
        .route("/v1/queues/{queue_id}/stats", get(queues::queue_stats))
        .route("/v1/queues/{queue_id}/pause", post(queues::pause_queue))
        .route("/v1/queues/{queue_id}/resume", post(queues::resume_queue))
        // DLQ
        .route("/v1/dlq", get(dlq::list_dlq))
        .route("/v1/dlq/{dlq_id}/retry", post(dlq::retry_dlq_job))
        // Workflow dependencies (Saga / DAG)
        .route("/v1/workflows/dependencies", post(workflows::create_dependency))
        // Metrics
        .route("/v1/metrics", get(health::metrics))
        // Workers (admin visibility)
        .route("/v1/workers", get(health::list_workers))
        // WebSocket (auth handled inside handler)
        .route("/v1/ws/events", get(websocket::ws_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            rate_limit_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    Router::new()
        .merge(public)
        .merge(protected)
        .layer(middleware::from_fn(correlation_id_middleware))
        .with_state(state)
}
