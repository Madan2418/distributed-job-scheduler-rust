use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,   // Normal operation
    Open,     // Failing — reject calls immediately
    HalfOpen, // Probing — allow one call through to test recovery
}

/// Circuit Breaker guarding external calls (e.g., LLM API).
/// Prevents LLM downtime from blocking core job processing.
pub struct CircuitBreaker {
    state: Arc<Mutex<CircuitState>>,
    failure_count: Arc<AtomicU64>,
    failure_threshold: u64,
    reset_timeout: Duration,
    last_failure_at: Arc<Mutex<Option<Instant>>>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u64, reset_timeout: Duration) -> Self {
        Self {
            state: Arc::new(Mutex::new(CircuitState::Closed)),
            failure_count: Arc::new(AtomicU64::new(0)),
            failure_threshold,
            reset_timeout,
            last_failure_at: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn call<F, T, E>(&self, f: F) -> Result<T, CircuitError<E>>
    where
        F: std::future::Future<Output = Result<T, E>>,
    {
        // Check state before calling
        {
            let mut state = self.state.lock().await;
            match *state {
                CircuitState::Open => {
                    // Check if reset timeout has elapsed — if so, go HalfOpen
                    let last = self.last_failure_at.lock().await;
                    if let Some(t) = *last {
                        if t.elapsed() >= self.reset_timeout {
                            *state = CircuitState::HalfOpen;
                        } else {
                            return Err(CircuitError::Open);
                        }
                    } else {
                        return Err(CircuitError::Open);
                    }
                }
                CircuitState::HalfOpen | CircuitState::Closed => {}
            }
        }

        // Execute the call
        match f.await {
            Ok(result) => {
                // Success: reset circuit
                self.failure_count.store(0, Ordering::SeqCst);
                let mut state = self.state.lock().await;
                *state = CircuitState::Closed;
                Ok(result)
            }
            Err(e) => {
                let count = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
                if count >= self.failure_threshold {
                    let mut state = self.state.lock().await;
                    *state = CircuitState::Open;
                    let mut last = self.last_failure_at.lock().await;
                    *last = Some(Instant::now());
                    tracing::warn!(
                        failures = count,
                        "Circuit breaker OPENED after {} failures", count
                    );
                }
                Err(CircuitError::Inner(e))
            }
        }
    }
}

#[derive(Debug)]
pub enum CircuitError<E> {
    Open,
    Inner(E),
}
