use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

#[derive(Clone)]
pub struct RateLimiter {
    bytes_per_second: Arc<AtomicU64>,
    state: Arc<Mutex<LimiterState>>,
}

struct LimiterState {
    last_check: Instant,
    available_tokens: f64,
}

impl RateLimiter {
    pub fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second: Arc::new(AtomicU64::new(bytes_per_second)),
            state: Arc::new(Mutex::new(LimiterState {
                last_check: Instant::now(),
                available_tokens: bytes_per_second as f64,
            })),
        }
    }

    pub fn set_rate(&self, bytes_per_sec: u64) {
        self.bytes_per_second.store(bytes_per_sec, Ordering::Relaxed);
    }

    pub async fn consume(&self, amount: u64) {
        let max_rate = self.bytes_per_second.load(Ordering::Relaxed);
        if max_rate == 0 {
            return; // No limit
        }

        let mut state = self.state.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_check).as_secs_f64();
        state.last_check = now;

        state.available_tokens += elapsed * (max_rate as f64);
        if state.available_tokens > max_rate as f64 {
            state.available_tokens = max_rate as f64;
        }

        if state.available_tokens >= amount as f64 {
            state.available_tokens -= amount as f64;
        } else {
            let deficit = (amount as f64) - state.available_tokens;
            let wait_secs = deficit / (max_rate as f64);
            state.available_tokens = 0.0;
            drop(state);
            sleep(Duration::from_secs_f64(wait_secs)).await;
        }
    }
}
