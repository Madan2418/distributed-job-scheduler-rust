use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};

/// Chain of Responsibility: Auth middleware (link 1 of chain).
/// Extracts and validates the Bearer JWT, attaches user claims to request extensions.
/// Short-circuits with 401 if token is missing or invalid.
pub async fn auth_middleware(
    State(state): State<crate::app_state::AppState>,
    mut req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let token = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    let token = match token {
        Some(t) => t.to_string(),
        None => return Err(StatusCode::UNAUTHORIZED),
    };

    let claims = state
        .auth_service
        .verify_access_token(&token)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    req.extensions_mut().insert(claims);
    Ok(next.run(req).await)
}
