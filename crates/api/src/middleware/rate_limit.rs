use axum::{
    body::Body,
    extract::State,
    http::{header, Request, StatusCode},
    middleware::Next,
    response::Response,
};
use redis::Script;
use services::auth_service::Claims;
use std::time::{SystemTime, UNIX_EPOCH};

const BUCKET_CAPACITY: u32 = 60;
const REFILL_PER_SECOND: u32 = 1;
const REQUEST_COST: u32 = 1;
const BUCKET_TTL_SECONDS: u32 = 120;

/// Redis-backed token bucket rate limiting middleware.
///
/// Authenticated requests are keyed by user id. Public requests fall back to
/// common forwarding headers so auth endpoints can be limited before login.
pub async fn rate_limit_middleware(
    State(state): State<crate::app_state::AppState>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let client_key = rate_limit_key(&req);
    let tokens_key = format!("rate_limit:{client_key}:tokens");
    let timestamp_key = format!("rate_limit:{client_key}:ts");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .as_secs();

    let mut conn = state.redis.clone();
    let script = Script::new(
        r#"
        local tokens_key = KEYS[1]
        local timestamp_key = KEYS[2]
        local capacity = tonumber(ARGV[1])
        local refill_per_second = tonumber(ARGV[2])
        local now = tonumber(ARGV[3])
        local requested = tonumber(ARGV[4])
        local ttl = tonumber(ARGV[5])

        local tokens = tonumber(redis.call("GET", tokens_key))
        if tokens == nil then
            tokens = capacity
        end

        local last_refill = tonumber(redis.call("GET", timestamp_key))
        if last_refill == nil then
            last_refill = now
        end

        local elapsed = math.max(0, now - last_refill)
        tokens = math.min(capacity, tokens + (elapsed * refill_per_second))

        local allowed = 0
        if tokens >= requested then
            tokens = tokens - requested
            allowed = 1
        end

        redis.call("SETEX", tokens_key, ttl, tokens)
        redis.call("SETEX", timestamp_key, ttl, now)

        return allowed
        "#,
    );

    let allowed = script
        .key(tokens_key)
        .key(timestamp_key)
        .arg(BUCKET_CAPACITY)
        .arg(REFILL_PER_SECOND)
        .arg(now)
        .arg(REQUEST_COST)
        .arg(BUCKET_TTL_SECONDS)
        .invoke_async::<_, i32>(&mut conn)
        .await;

    match allowed {
        Ok(1) => Ok(next.run(req).await),
        Ok(_) => Err(StatusCode::TOO_MANY_REQUESTS),
        Err(error) => {
            tracing::warn!(%error, "Rate limit check failed; allowing request");
            Ok(next.run(req).await)
        }
    }
}

fn rate_limit_key(req: &Request<Body>) -> String {
    if let Some(claims) = req.extensions().get::<Claims>() {
        return format!("user:{}", claims.sub);
    }

    let raw_key = req
        .headers()
        .get("x-forwarded-for")
        .or_else(|| req.headers().get("x-real-ip"))
        .or_else(|| req.headers().get(header::USER_AGENT))
        .and_then(|value| value.to_str().ok())
        .unwrap_or("anonymous")
        .split(',')
        .next()
        .unwrap_or("anonymous")
        .trim();

    format!("ip:{raw_key}")
}

#[cfg(test)]
mod tests {
    use super::rate_limit_key;
    use axum::body::Body;
    use axum::http::Request;
    use services::auth_service::Claims;

    #[test]
    fn uses_authenticated_user_id_when_claims_exist() {
        let mut req = Request::builder().body(Body::empty()).unwrap();
        req.extensions_mut().insert(Claims {
            sub: "user-123".to_string(),
            email: "test@example.com".to_string(),
            exp: 1,
            iat: 1,
        });

        assert_eq!(rate_limit_key(&req), "user:user-123");
    }

    #[test]
    fn falls_back_to_forwarded_ip() {
        let req = Request::builder()
            .header("x-forwarded-for", "203.0.113.10, 10.0.0.1")
            .body(Body::empty())
            .unwrap();

        assert_eq!(rate_limit_key(&req), "ip:203.0.113.10");
    }
}
