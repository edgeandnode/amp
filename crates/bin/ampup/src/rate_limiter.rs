use std::time::{Duration, Instant, SystemTime};

use tokio::sync::Mutex;

/// Shared rate limiter that respects GitHub API rate-limit headers.
///
/// All concurrent downloads share one `GitHubRateLimiter` so that a 429
/// response pauses every in-flight request, not just the one that triggered it.
pub struct GitHubRateLimiter {
    inner: Mutex<RateLimiterState>,
    has_token: bool,
}

struct RateLimiterState {
    paused_until: Option<Instant>,
    remaining: Option<u64>,
}

impl GitHubRateLimiter {
    /// Create a new rate limiter.
    pub fn new(has_token: bool) -> Self {
        Self {
            inner: Mutex::new(RateLimiterState {
                paused_until: None,
                remaining: None,
            }),
            has_token,
        }
    }

    /// Whether the client has an authentication token.
    pub fn has_token(&self) -> bool {
        self.has_token
    }

    /// Block until any active rate-limit pause has expired.
    pub async fn wait_if_paused(&self) {
        let wait_duration = {
            let state = self.inner.lock().await;
            state.paused_until.and_then(|until| {
                let now = Instant::now();
                if until > now { Some(until - now) } else { None }
            })
        };

        if let Some(duration) = wait_duration {
            tokio::time::sleep(duration).await;
        }
    }

    /// Inspect a response and update rate-limit state.
    ///
    /// Parses `X-RateLimit-Remaining`, `X-RateLimit-Reset`, and `Retry-After`
    /// headers. On HTTP 429, sets a global pause and returns
    /// `Some(retry_after_secs)`. When remaining hits 0, preemptively pauses
    /// until the reset timestamp. Returns `None` for non-429 responses.
    ///
    /// Header names per GitHub REST API docs:
    /// https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api
    pub async fn update_from_response(&self, response: &reqwest::Response) -> Option<u64> {
        let status = response.status();
        let remaining = Self::parse_header_u64(response, "x-ratelimit-remaining");
        let reset_at = Self::parse_header_u64(response, "x-ratelimit-reset");
        let retry_after = Self::parse_header_u64(response, "retry-after");

        self.update_state(status, remaining, reset_at, retry_after)
            .await
    }

    /// Core rate-limit state machine, separated for testability.
    async fn update_state(
        &self,
        status: reqwest::StatusCode,
        remaining: Option<u64>,
        reset_at: Option<u64>,
        retry_after: Option<u64>,
    ) -> Option<u64> {
        let mut state = self.inner.lock().await;

        if let Some(rem) = remaining {
            state.remaining = Some(rem);
        }

        // GitHub returns 429 or 403 for rate limiting. Treat 403 as rate-limited
        // only when there's a clear signal (retry-after present or remaining is 0)
        // to avoid confusing it with a permissions error.
        let is_rate_limited = status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || (status == reqwest::StatusCode::FORBIDDEN
                && (retry_after.is_some() || remaining == Some(0)));

        if is_rate_limited {
            // Retry-After is not guaranteed to be present on rate-limit responses.
            // GitHub docs recommend waiting at least one minute when absent.
            let secs = retry_after.unwrap_or(60);
            let pause_until = Instant::now() + Duration::from_secs(secs);
            Self::extend_pause(&mut state, pause_until);
            return Some(secs);
        }

        // Preemptive pause when remaining hits 0
        if remaining == Some(0)
            && let Some(reset) = reset_at
        {
            let now_unix = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if reset > now_unix {
                let pause_until = Instant::now() + Duration::from_secs(reset - now_unix);
                Self::extend_pause(&mut state, pause_until);
            }
        }

        None
    }

    /// Extend the pause window, never shortening an existing one.
    fn extend_pause(state: &mut RateLimiterState, pause_until: Instant) {
        match state.paused_until {
            Some(existing) if existing > pause_until => {}
            _ => state.paused_until = Some(pause_until),
        }
    }

    /// Current remaining API calls, if known.
    pub async fn remaining(&self) -> Option<u64> {
        self.inner.lock().await.remaining
    }

    fn parse_header_u64(response: &reqwest::Response, name: &str) -> Option<u64> {
        response
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
    }
}

#[cfg(test)]
mod tests {
    //! Tests are organized into nested modules by the method under test,
    //! following the project convention for modules with 10+ tests (see
    //! docs/code/test-files.md "Module Structure Within cfg(test)").

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    /// Spawn a one-shot TCP server that returns a raw HTTP response.
    /// Accepts one connection, drains the request, writes `response_bytes`, then closes.
    async fn mock_http_response(response_bytes: Vec<u8>) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind to a random port");
        let addr = listener.local_addr().expect("should have a local address");

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("should accept a connection");
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            stream
                .write_all(&response_bytes)
                .await
                .expect("should write response");
        });

        addr
    }

    /// Tests for the blocking gate that callers use before making HTTP requests.
    mod wait_if_paused {
        use super::*;

        #[tokio::test]
        async fn with_no_active_pause_returns_immediately() {
            //* Given
            let limiter = GitHubRateLimiter::new(true);

            //* When
            let start = Instant::now();
            limiter.wait_if_paused().await;

            //* Then
            assert!(
                start.elapsed() < Duration::from_millis(50),
                "should return immediately when no pause is set"
            );
        }

        #[tokio::test]
        async fn with_expired_pause_returns_immediately() {
            //* Given
            let limiter = GitHubRateLimiter::new(false);
            {
                let mut state = limiter.inner.lock().await;
                state.paused_until = Some(Instant::now() - Duration::from_secs(1));
            }

            //* When
            let start = Instant::now();
            limiter.wait_if_paused().await;

            //* Then
            assert!(
                start.elapsed() < Duration::from_millis(50),
                "should return immediately when pause has already expired"
            );
        }

        #[tokio::test]
        async fn with_active_pause_blocks_until_expiry() {
            //* Given
            let limiter = GitHubRateLimiter::new(true);
            {
                let mut state = limiter.inner.lock().await;
                state.paused_until = Some(Instant::now() + Duration::from_millis(100));
            }

            //* When
            let start = Instant::now();
            limiter.wait_if_paused().await;

            //* Then
            assert!(
                start.elapsed() >= Duration::from_millis(90),
                "should block for approximately the pause duration"
            );
        }
    }

    /// Tests for the core state machine using direct values (no HTTP involved).
    /// Covers edge cases like missing headers, 403 vs 429 disambiguation, and
    /// expired reset timestamps.
    mod update_state {
        use super::*;

        #[tokio::test]
        async fn with_429_and_no_retry_after_defaults_to_60s() {
            //* Given
            let limiter = GitHubRateLimiter::new(false);

            //* When
            let result = limiter
                .update_state(reqwest::StatusCode::TOO_MANY_REQUESTS, None, None, None)
                .await;

            //* Then
            assert_eq!(
                result,
                Some(60),
                "should default to 60 seconds when Retry-After header is absent"
            );
        }

        #[tokio::test]
        async fn with_403_and_remaining_zero_treats_as_rate_limited() {
            //* Given
            let limiter = GitHubRateLimiter::new(true);

            //* When
            let result = limiter
                .update_state(reqwest::StatusCode::FORBIDDEN, Some(0), None, None)
                .await;

            //* Then
            assert_eq!(
                result,
                Some(60),
                "should treat 403 with remaining=0 as rate-limited"
            );
        }

        #[tokio::test]
        async fn with_403_and_no_rate_limit_signal_ignores() {
            //* Given
            let limiter = GitHubRateLimiter::new(true);

            //* When — 403 without retry-after or remaining=0 is a permissions error, not rate limiting
            let result = limiter
                .update_state(reqwest::StatusCode::FORBIDDEN, None, None, None)
                .await;

            //* Then
            assert_eq!(result, None, "should not treat a plain 403 as rate-limited");
        }

        #[tokio::test]
        async fn with_remaining_zero_and_past_reset_skips_pause() {
            //* Given
            let limiter = GitHubRateLimiter::new(true);
            let past_reset = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("system time should be after epoch")
                .as_secs()
                - 60;

            //* When
            limiter
                .update_state(reqwest::StatusCode::OK, Some(0), Some(past_reset), None)
                .await;

            //* Then
            let state = limiter.inner.lock().await;
            assert!(
                state.paused_until.is_none(),
                "should not pause when the reset timestamp is already in the past"
            );
        }
    }

    /// End-to-end tests that send real HTTP through reqwest to verify that the
    /// header names (`X-RateLimit-Remaining`, `X-RateLimit-Reset`, `Retry-After`)
    /// are parsed correctly from actual HTTP responses.
    mod update_from_response {
        use super::*;

        #[tokio::test]
        async fn with_ok_status_parses_remaining_header() {
            //* Given
            let addr = mock_http_response(
                b"HTTP/1.1 200 OK\r\n\
                  X-RateLimit-Remaining: 42\r\n\
                  X-RateLimit-Reset: 1700000000\r\n\
                  Content-Length: 0\r\n\
                  \r\n"
                    .to_vec(),
            )
            .await;

            let limiter = GitHubRateLimiter::new(true);
            let client = reqwest::Client::new();
            let response = client
                .get(format!("http://{}", addr))
                .send()
                .await
                .expect("request to mock server should succeed");

            //* When
            let result = limiter.update_from_response(&response).await;

            //* Then
            assert_eq!(result, None, "should not signal retry for 200 OK");
            assert_eq!(
                limiter.remaining().await,
                Some(42),
                "should parse X-RateLimit-Remaining header"
            );
        }

        #[tokio::test]
        async fn with_429_status_returns_retry_after() {
            //* Given
            let addr = mock_http_response(
                b"HTTP/1.1 429 Too Many Requests\r\n\
                  Retry-After: 30\r\n\
                  X-RateLimit-Remaining: 0\r\n\
                  Content-Length: 0\r\n\
                  \r\n"
                    .to_vec(),
            )
            .await;

            let limiter = GitHubRateLimiter::new(true);
            let client = reqwest::Client::new();
            let response = client
                .get(format!("http://{}", addr))
                .send()
                .await
                .expect("request to mock server should succeed");

            //* When
            let result = limiter.update_from_response(&response).await;

            //* Then
            assert_eq!(result, Some(30), "should return Retry-After value on 429");
            assert_eq!(
                limiter.remaining().await,
                Some(0),
                "should parse X-RateLimit-Remaining from 429 response"
            );
        }

        #[tokio::test]
        async fn with_remaining_zero_sets_preemptive_pause() {
            //* Given
            let future_reset = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("system time should be after epoch")
                .as_secs()
                + 120;
            let response_str = format!(
                "HTTP/1.1 200 OK\r\n\
                 X-RateLimit-Remaining: 0\r\n\
                 X-RateLimit-Reset: {}\r\n\
                 Content-Length: 0\r\n\
                 \r\n",
                future_reset
            );

            let addr = mock_http_response(response_str.into_bytes()).await;

            let limiter = GitHubRateLimiter::new(true);
            let client = reqwest::Client::new();
            let response = client
                .get(format!("http://{}", addr))
                .send()
                .await
                .expect("request to mock server should succeed");

            //* When
            let result = limiter.update_from_response(&response).await;

            //* Then
            assert_eq!(result, None, "should not signal retry for 200 OK");
            let state = limiter.inner.lock().await;
            assert!(
                state.paused_until.is_some(),
                "should set preemptive pause when remaining is 0 with future reset"
            );
        }
    }
}
