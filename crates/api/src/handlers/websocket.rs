use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures_util::StreamExt;
use tracing::{info, warn};

use crate::app_state::AppState;
use infrastructure::redis_client::JOBS_CHANNEL;

/// GET /v1/ws/events — WebSocket endpoint for live dashboard updates.
/// Subscribes to the Redis Pub/Sub channel and forwards events to the client.
/// If the WebSocket drops, the dashboard falls back to polling (graceful degradation).
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, _state: AppState) {
    info!("WebSocket client connected");

    // Use the same Redis URL as the rest of the app
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://localhost:6379".to_string());

    let client = match redis::Client::open(redis_url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "Failed to open Redis client for WebSocket");
            return;
        }
    };

    let mut pubsub = match client.get_async_pubsub().await {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Failed to get pubsub connection");
            return;
        }
    };

    if let Err(e) = pubsub.subscribe(JOBS_CHANNEL).await {
        warn!(error = %e, "Failed to subscribe to jobs channel");
        return;
    }

    let mut stream = pubsub.on_message();

    loop {
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    Some(m) => {
                        let payload: String = m.get_payload().unwrap_or_default();
                        if socket.send(Message::Text(payload.into())).await.is_err() {
                            info!("WebSocket client disconnected");
                            break;
                        }
                    }
                    None => break,
                }
            }
            // Keepalive ping every 30 seconds
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                if socket.send(Message::Ping(vec![].into())).await.is_err() {
                    break;
                }
            }
        }
    }
}
