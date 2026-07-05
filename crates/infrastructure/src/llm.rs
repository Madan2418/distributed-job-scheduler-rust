use reqwest::Client;
use serde_json::{json, Value};
use tracing::warn;

use crate::circuit_breaker::{CircuitBreaker, CircuitError};

pub struct LlmClient {
    http: Client,
    api_key: String,
    api_url: String,
    circuit_breaker: CircuitBreaker,
}

impl LlmClient {
    pub fn new(api_key: String, api_url: String) -> Self {
        Self {
            http: Client::new(),
            api_key,
            api_url,
            circuit_breaker: CircuitBreaker::new(3, std::time::Duration::from_secs(60)),
        }
    }

    /// Generate an AI failure summary for a job. Guarded by Circuit Breaker.
    pub async fn generate_failure_summary(
        &self,
        job_name: &str,
        error_message: &str,
        payload: &Value,
    ) -> Option<String> {
        let prompt = format!(
            "A background job named '{}' failed with the following error:\n\n\
            Error: {}\n\nPayload: {}\n\n\
            Provide a concise 2-3 sentence explanation of what likely went wrong \
            and how to fix it. Be specific and technical.",
            job_name,
            error_message,
            payload
        );

        let request_body = json!({
            "model": "gemini-2.0-flash",
            "contents": [{
                "parts": [{"text": prompt}]
            }]
        });

        let url = format!("{}/models/gemini-2.0-flash:generateContent?key={}", self.api_url, self.api_key);
        let http = self.http.clone();
        let body = request_body.clone();

        let result = self
            .circuit_breaker
            .call(async move {
                http.post(&url)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?
                    .json::<Value>()
                    .await
                    .map_err(|e| e.to_string())
            })
            .await;

        match result {
            Ok(response) => {
                response["candidates"][0]["content"]["parts"][0]["text"]
                    .as_str()
                    .map(|s| s.to_string())
            }
            Err(CircuitError::Open) => {
                warn!("LLM circuit breaker is open — skipping AI summary");
                None
            }
            Err(CircuitError::Inner(e)) => {
                warn!(error = %e, "LLM API call failed");
                None
            }
        }
    }
}
