use axum::{
    body::Body,
    http::{Request, Response},
    middleware::Next,
};
use uuid::Uuid;

/// Correlation ID middleware: injects a correlation ID into every request,
/// and echoes it back in the response header.
pub async fn correlation_id_middleware(
    mut req: Request<Body>,
    next: Next,
) -> Response<Body> {
    let correlation_id = req
        .headers()
        .get("x-correlation-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    req.extensions_mut().insert(correlation_id.clone());

    let mut response = next.run(req).await;
    response.headers_mut().insert(
        "x-correlation-id",
        correlation_id.parse().unwrap(),
    );
    response
}
