use uuid::Uuid;

/// Generate a new correlation ID for a job or request.
pub fn new_correlation_id() -> Uuid {
    Uuid::new_v4()
}

/// Extract correlation ID from request headers, or generate one.
pub fn from_header(value: Option<&str>) -> Uuid {
    value
        .and_then(|v| Uuid::parse_str(v).ok())
        .unwrap_or_else(Uuid::new_v4)
}
