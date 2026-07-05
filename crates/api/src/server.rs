use std::net::SocketAddr;
use tower_http::cors::{CorsLayer, Any};
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::app_state::AppState;
use crate::routes::build_router;

pub async fn run(state: AppState, port: u16) {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = build_router(state)
        .layer(TraceLayer::new_for_http())
        .layer(cors);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("API server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
