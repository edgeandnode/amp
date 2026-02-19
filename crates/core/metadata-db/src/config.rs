//! Connection pool configuration for the metadata database.

use std::time::Duration;

/// Default maximum number of connections in the pool.
///
/// A pool of 10 connections is sufficient for most workloads. Combined with
/// [`DEFAULT_POOL_MIN_CONNECTIONS`], this prevents bulk connection recycling
/// (thundering herd) by ensuring a baseline of connections is always ready.
pub const DEFAULT_POOL_MAX_CONNECTIONS: u32 = 10;

/// Default minimum number of connections to keep alive in the pool.
///
/// Set to 25% of [`DEFAULT_POOL_MAX_CONNECTIONS`] (rounded up, minimum 1):
/// `DEFAULT_POOL_MAX_CONNECTIONS.div_ceil(4).max(1)` = 3.
///
/// Keeping a floor of ready connections avoids connection establishment latency
/// on the hot path and staggers `max_lifetime` expiration — rather than all 10
/// connections expiring at the same time (thundering herd), the pool holds at
/// least 3 open, so only excess connections are cycled per interval.
pub const DEFAULT_POOL_MIN_CONNECTIONS: u32 = {
    let v = DEFAULT_POOL_MAX_CONNECTIONS.div_ceil(4);
    if v < 1 { 1 } else { v }
};

/// Default maximum lifetime for a pooled connection (30 minutes).
///
/// Connections are recycled before they hit server-side idle timeouts
/// (typically 1–8 hours on managed PostgreSQL). Combined with
/// [`DEFAULT_POOL_MIN_CONNECTIONS`], individual connections are staggered
/// rather than expiring all at once.
pub const DEFAULT_MAX_LIFETIME: Duration = Duration::from_secs(1800);

/// Default idle timeout for a pooled connection (10 minutes).
///
/// Connections idle longer than this are closed and removed from the pool,
/// down to the [`DEFAULT_POOL_MIN_CONNECTIONS`] floor.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// Default acquire timeout when checking out a connection (5 seconds).
///
/// Requests that cannot acquire a connection within this window fail fast
/// rather than queuing indefinitely.
pub const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// Connection pool configuration.
///
/// Controls pool sizing and connection lifecycle. The defaults prevent bulk
/// connection recycling (thundering herd) by maintaining a baseline of ready
/// connections and staggering expiration.
///
/// # Defaults
///
/// | Field              | Default                                         |
/// |--------------------|--------------------------------------------------|
/// | `max_connections`  | [`DEFAULT_POOL_MAX_CONNECTIONS`] (10)            |
/// | `min_connections`  | [`DEFAULT_POOL_MIN_CONNECTIONS`] (3, i.e. 25%)  |
/// | `acquire_timeout`  | [`DEFAULT_ACQUIRE_TIMEOUT`] (5 s)               |
/// | `max_lifetime`     | [`DEFAULT_MAX_LIFETIME`] (30 min)               |
/// | `idle_timeout`     | [`DEFAULT_IDLE_TIMEOUT`] (10 min)               |
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of connections in the pool.
    pub max_connections: u32,
    /// Minimum number of connections to keep alive in the pool.
    ///
    /// Keeping a floor of ready connections avoids connection establishment
    /// latency on the hot path and prevents all connections from expiring
    /// simultaneously (thundering herd).
    pub min_connections: u32,
    /// Maximum time to wait for a connection from the pool before failing.
    pub acquire_timeout: Duration,
    /// Maximum lifetime of a connection before it is recycled.
    pub max_lifetime: Duration,
    /// How long a connection may sit idle before being closed.
    pub idle_timeout: Duration,
}

impl PoolConfig {
    /// Creates a `PoolConfig` with the given pool size and sensible defaults.
    ///
    /// `min_connections` is derived from `size` using the heuristic
    /// `size.div_ceil(4).max(1)` (≈ 25% of max, minimum 1). This keeps a
    /// baseline of ready connections and staggers `max_lifetime` expiration
    /// so the pool never recycles all connections at once.
    pub fn with_size(size: u32) -> Self {
        Self {
            max_connections: size,
            min_connections: size.div_ceil(4).max(1),
            ..Self::default()
        }
    }
}

impl Default for PoolConfig {
    /// Returns a `PoolConfig` using the crate-level defaults.
    ///
    /// | Field              | Value                                                        |
    /// |--------------------|--------------------------------------------------------------|
    /// | `max_connections`  | [`DEFAULT_POOL_MAX_CONNECTIONS`] (10)                        |
    /// | `min_connections`  | [`DEFAULT_POOL_MIN_CONNECTIONS`] (3) — 25% of max, ≥ 1      |
    /// | `acquire_timeout`  | [`DEFAULT_ACQUIRE_TIMEOUT`] (5 s)                           |
    /// | `max_lifetime`     | [`DEFAULT_MAX_LIFETIME`] (30 min)                           |
    /// | `idle_timeout`     | [`DEFAULT_IDLE_TIMEOUT`] (10 min)                           |
    ///
    /// The `min_connections` floor (25% of max) prevents the pool from
    /// expiring all connections simultaneously when `max_lifetime` is reached,
    /// avoiding a thundering-herd reconnect spike under load.
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_POOL_MAX_CONNECTIONS,
            min_connections: DEFAULT_POOL_MIN_CONNECTIONS,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
            max_lifetime: DEFAULT_MAX_LIFETIME,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
        }
    }
}
