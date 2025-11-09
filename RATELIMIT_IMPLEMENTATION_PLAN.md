# Envoy Rate Limiting Service Implementation Plan

**Status:** Planning Phase  
**Created:** 2025-11-09  
**Target:** Standalone rate limiting service for Envoy proxy integration

## Executive Summary

This document outlines the implementation plan for a standalone rate limiting service (`ampratelimit`) that implements the Envoy Rate Limit Service (RLS) v3 protocol.

**Key Decisions:**
- **Storage:** In-memory using the `governor` crate (v0.10.0, already in workspace)
- **Algorithm:** Token bucket (via governor's GCRA implementation)
- **Configuration:** Static TOML files
- **Deployment:** Standalone binary (`ampratelimit`)
- **Protocol:** Envoy RLS v3 gRPC

## Architecture Overview

```
┌─────────────┐        gRPC         ┌──────────────────┐
│   Envoy     │ ◄─────────────────► │  ampratelimit    │
│   Proxy     │   ShouldRateLimit   │   (standalone)   │
└─────────────┘                     └──────────────────┘
                                            │
                                            │ uses
                                            ▼
                                    ┌──────────────────┐
                                    │  governor crate  │
                                    │  (in-memory)     │
                                    │  GCRA algorithm  │
                                    └──────────────────┘
```

### Data Flow

1. Envoy receives HTTP request
2. Envoy extracts rate limit descriptors (e.g., client_id, api_key)
3. Envoy calls `ShouldRateLimit` gRPC method on ampratelimit
4. ampratelimit checks rate limit using governor
5. ampratelimit returns OK or OVER_LIMIT
6. Envoy allows/denies request based on response

## Project Structure

```
crates/
├── services/
│   └── ratelimit/                    # Service library crate
│       ├── Cargo.toml
│       ├── build.rs                  # Proto compilation
│       ├── proto/
│       │   ├── envoy_rls_v3.proto   # Envoy RLS v3 service
│       │   ├── ratelimit_descriptor.proto
│       │   └── base.proto
│       └── src/
│           ├── lib.rs                # Public API exports
│           ├── service.rs            # Two-phase service entry point
│           ├── config.rs             # Configuration types and parsing
│           ├── limiter.rs            # Governor-based rate limiter
│           ├── descriptor.rs         # Envoy descriptor handling
│           ├── grpc.rs               # gRPC service implementation
│           └── metrics.rs            # OpenTelemetry metrics
│
└── bin/
    └── ampratelimit/                 # Standalone binary
        ├── Cargo.toml
        ├── build.rs                  # Version info (vergen)
        └── src/
            └── main.rs               # CLI entry point
```

## Component Specifications

### 1. Proto Definitions

**Files:**
- `proto/envoy_rls_v3.proto` - Main RLS service definition
- `proto/ratelimit_descriptor.proto` - Descriptor types
- `proto/base.proto` - Common types

**Key Protocol Buffer Messages:**

```protobuf
syntax = "proto3";

package envoy.service.ratelimit.v3;

service RateLimitService {
  rpc ShouldRateLimit(RateLimitRequest) returns (RateLimitResponse);
}

message RateLimitRequest {
  string domain = 1;                          // e.g., "graphql"
  repeated RateLimitDescriptor descriptors = 2;
  uint32 hits_addend = 3;                     // Default: 1
}

message RateLimitResponse {
  enum Code {
    UNKNOWN = 0;
    OK = 1;
    OVER_LIMIT = 2;
  }
  Code overall_code = 1;
  repeated DescriptorStatus statuses = 2;
  repeated ResponseHeadersToAdd response_headers_to_add = 3;
  repeated HeaderToAdd request_headers_to_add = 4;
  RawBody raw_body = 5;
}

message RateLimitDescriptor {
  repeated Entry entries = 1;
  
  message Entry {
    string key = 1;
    string value = 2;
  }
}

message DescriptorStatus {
  enum Code {
    UNKNOWN = 0;
    OK = 1;
    OVER_LIMIT = 2;
  }
  Code code = 1;
  RateLimit current_limit = 2;
  uint32 limit_remaining = 3;
  google.protobuf.Duration duration_until_reset = 4;
}
```

**Source:** Based on https://github.com/envoyproxy/data-plane-api/blob/main/envoy/service/ratelimit/v3/rls.proto

### 2. Configuration Schema

**File:** `src/config.rs`

**TOML Configuration Format:**

```toml
# config.toml

[ratelimit]
# gRPC server binding address
addr = "0.0.0.0:8081"

# Global settings
default_hits_addend = 1

# Rate limit policies by domain
[[ratelimit.domains]]
name = "graphql"

  # Simple descriptor: limit by client_id
  [[ratelimit.domains.descriptors]]
  key = "client_id"
  rate_limit = { requests_per_unit = 100, unit = "second" }

  # Additional limit on same key (multiple limits enforced)
  [[ratelimit.domains.descriptors]]
  key = "client_id"
  rate_limit = { requests_per_unit = 1000, unit = "minute" }

  # Composite descriptor: limit by client_id + path
  [[ratelimit.domains.descriptors]]
  key = "client_id"
  value = "premium_user"
  rate_limit = { requests_per_unit = 500, unit = "second" }

[[ratelimit.domains]]
name = "api"

  [[ratelimit.domains.descriptors]]
  key = "api_key"
  rate_limit = { requests_per_unit = 10, unit = "second" }

  [[ratelimit.domains.descriptors]]
  key = "api_key"
  rate_limit = { requests_per_unit = 100, unit = "minute" }
```

**Rust Configuration Types:**

```rust
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RateLimitConfig {
    pub addr: SocketAddr,
    #[serde(default = "default_hits_addend")]
    pub default_hits_addend: u32,
    pub domains: Vec<DomainConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DomainConfig {
    pub name: String,
    pub descriptors: Vec<DescriptorConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DescriptorConfig {
    pub key: String,
    #[serde(default)]
    pub value: Option<String>,
    pub rate_limit: RateLimit,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RateLimit {
    pub requests_per_unit: u32,
    pub unit: TimeUnit,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeUnit {
    Second,
    Minute,
    Hour,
    Day,
}

impl TimeUnit {
    pub fn as_duration(&self) -> std::time::Duration {
        match self {
            TimeUnit::Second => Duration::from_secs(1),
            TimeUnit::Minute => Duration::from_secs(60),
            TimeUnit::Hour => Duration::from_secs(3600),
            TimeUnit::Day => Duration::from_secs(86400),
        }
    }
}

fn default_hits_addend() -> u32 { 1 }

impl RateLimitConfig {
    /// Load configuration from TOML file
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)
            .map_err(|source| ConfigError::ReadFile { path: path.to_string(), source })?;
        
        toml::from_str(&content)
            .map_err(|source| ConfigError::Parse { path: path.to_string(), source })
    }
    
    /// Validate configuration
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Ensure at least one domain
        if self.domains.is_empty() {
            return Err(ConfigError::Validation("No domains configured".to_string()));
        }
        
        // Validate each domain
        for domain in &self.domains {
            if domain.descriptors.is_empty() {
                return Err(ConfigError::Validation(
                    format!("Domain '{}' has no descriptors", domain.name)
                ));
            }
            
            for desc in &domain.descriptors {
                if desc.rate_limit.requests_per_unit == 0 {
                    return Err(ConfigError::Validation(
                        "Rate limit requests_per_unit must be > 0".to_string()
                    ));
                }
            }
        }
        
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read configuration file {path}: {source}")]
    ReadFile {
        path: String,
        source: std::io::Error,
    },
    
    #[error("failed to parse configuration file {path}: {source}")]
    Parse {
        path: String,
        source: toml::de::Error,
    },
    
    #[error("configuration validation failed: {0}")]
    Validation(String),
}
```

### 3. Rate Limiter Implementation

**File:** `src/limiter.rs`

**Design:**
- Use `governor::RateLimiter` with keyed state (per descriptor combination)
- Use `DashMap` for concurrent access to limiters
- Cache key format: `"{domain}:{key1}={value1}:{key2}={value2}"`
- Support multiple limits per descriptor (all must pass)

```rust
use governor::{Quota, RateLimiter};
use dashmap::DashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

pub struct RateLimiterStore {
    /// Map from cache key to rate limiter
    /// Key format: "{domain}:{descriptor_key}={descriptor_value}"
    limiters: DashMap<String, Vec<RateLimiter<String, governor::DefaultKeyedRateLimiter>>>,
    config: Arc<RateLimitConfig>,
}

impl RateLimiterStore {
    pub fn new(config: Arc<RateLimitConfig>) -> Self {
        Self {
            limiters: DashMap::new(),
            config,
        }
    }
    
    /// Check rate limit for given domain and descriptors
    pub fn check_rate_limit(
        &self,
        domain: &str,
        descriptors: &[proto::RateLimitDescriptor],
        hits_addend: u32,
    ) -> Result<RateLimitDecision, RateLimitError> {
        // Find domain config
        let domain_config = self.config.domains.iter()
            .find(|d| d.name == domain)
            .ok_or_else(|| RateLimitError::NoMatchingPolicy { 
                domain: domain.to_string() 
            })?;
        
        // For each descriptor in the request
        let mut all_allowed = true;
        let mut descriptor_statuses = Vec::new();
        
        for descriptor in descriptors {
            // Build cache key from descriptor entries
            let cache_key = build_cache_key(domain, descriptor);
            
            // Find matching policies
            let matching_policies = find_matching_policies(
                domain_config, 
                descriptor
            );
            
            if matching_policies.is_empty() {
                // No matching policy - allow by default
                descriptor_statuses.push(DescriptorStatus::ok());
                continue;
            }
            
            // Get or create rate limiters for this cache key
            let limiters = self.limiters.entry(cache_key.clone())
                .or_insert_with(|| {
                    create_limiters_for_policies(&matching_policies)
                });
            
            // Check all limiters (ALL must allow)
            let descriptor_allowed = limiters.iter()
                .all(|limiter| {
                    limiter.check_n(
                        NonZeroU32::new(hits_addend).unwrap()
                    ).is_ok()
                });
            
            if !descriptor_allowed {
                all_allowed = false;
            }
            
            descriptor_statuses.push(if descriptor_allowed {
                DescriptorStatus::ok()
            } else {
                DescriptorStatus::over_limit()
            });
        }
        
        Ok(RateLimitDecision {
            allowed: all_allowed,
            descriptor_statuses,
        })
    }
}

#[derive(Debug)]
pub struct RateLimitDecision {
    pub allowed: bool,
    pub descriptor_statuses: Vec<DescriptorStatus>,
}

#[derive(Debug)]
pub struct DescriptorStatus {
    pub allowed: bool,
    pub current_limit: Option<RateLimit>,
    pub limit_remaining: Option<u32>,
}

impl DescriptorStatus {
    fn ok() -> Self {
        Self {
            allowed: true,
            current_limit: None,
            limit_remaining: None,
        }
    }
    
    fn over_limit() -> Self {
        Self {
            allowed: false,
            current_limit: None,
            limit_remaining: Some(0),
        }
    }
}

fn build_cache_key(domain: &str, descriptor: &proto::RateLimitDescriptor) -> String {
    let mut parts = vec![domain.to_string()];
    
    for entry in &descriptor.entries {
        parts.push(format!("{}={}", entry.key, entry.value));
    }
    
    parts.join(":")
}

fn find_matching_policies(
    domain_config: &DomainConfig,
    descriptor: &proto::RateLimitDescriptor,
) -> Vec<&DescriptorConfig> {
    domain_config.descriptors.iter()
        .filter(|policy| {
            // Match if policy key exists in descriptor entries
            descriptor.entries.iter().any(|entry| {
                entry.key == policy.key && 
                (policy.value.is_none() || policy.value.as_ref() == Some(&entry.value))
            })
        })
        .collect()
}

fn create_limiters_for_policies(
    policies: &[&DescriptorConfig]
) -> Vec<RateLimiter<String, governor::DefaultKeyedRateLimiter>> {
    policies.iter()
        .map(|policy| {
            let quota = Quota::with_period(
                Duration::from_secs(policy.rate_limit.unit.as_duration().as_secs())
            )
            .unwrap()
            .allow_burst(NonZeroU32::new(policy.rate_limit.requests_per_unit).unwrap());
            
            RateLimiter::keyed(quota)
        })
        .collect()
}

#[derive(Debug, thiserror::Error)]
pub enum RateLimitError {
    #[error("no matching rate limit policy for domain: {domain}")]
    NoMatchingPolicy { domain: String },
    
    #[error("invalid descriptor: {reason}")]
    InvalidDescriptor { reason: String },
}
```

### 4. gRPC Service Implementation

**File:** `src/grpc.rs`

```rust
use tonic::{Request, Response, Status};
use std::sync::Arc;
use std::time::Instant;

use crate::{
    proto::{
        rate_limit_service_server::RateLimitService,
        RateLimitRequest, RateLimitResponse,
        rate_limit_response::{Code, DescriptorStatus},
    },
    limiter::RateLimiterStore,
    metrics::Metrics,
};

pub struct RateLimitServiceImpl {
    limiter_store: Arc<RateLimiterStore>,
    metrics: Arc<Metrics>,
}

impl RateLimitServiceImpl {
    pub fn new(limiter_store: Arc<RateLimiterStore>, metrics: Arc<Metrics>) -> Self {
        Self {
            limiter_store,
            metrics,
        }
    }
}

#[tonic::async_trait]
impl RateLimitService for RateLimitServiceImpl {
    async fn should_rate_limit(
        &self,
        request: Request<RateLimitRequest>,
    ) -> Result<Response<RateLimitResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        
        tracing::debug!(
            domain = %req.domain,
            descriptor_count = req.descriptors.len(),
            hits_addend = req.hits_addend,
            "Processing rate limit request"
        );
        
        // Increment request counter
        self.metrics.check_count_total.add(1, &[]);
        
        // Use default hits_addend if not specified
        let hits_addend = if req.hits_addend == 0 {
            1
        } else {
            req.hits_addend
        };
        
        // Check rate limits
        let decision = self.limiter_store
            .check_rate_limit(&req.domain, &req.descriptors, hits_addend)
            .map_err(|e| {
                tracing::error!(error = %e, "Rate limit check failed");
                Status::internal(e.to_string())
            })?;
        
        // Record metrics
        let duration_ms = start.elapsed().as_millis() as f64;
        self.metrics.check_duration_ms.record(duration_ms, &[]);
        
        let overall_code = if decision.allowed {
            self.metrics.decision_total.add(1, &[
                opentelemetry::KeyValue::new("decision", "allowed")
            ]);
            Code::Ok
        } else {
            self.metrics.decision_total.add(1, &[
                opentelemetry::KeyValue::new("decision", "denied")
            ]);
            Code::OverLimit
        };
        
        tracing::info!(
            domain = %req.domain,
            overall_code = ?overall_code,
            duration_ms = duration_ms,
            "Rate limit decision"
        );
        
        // Build response
        let response = RateLimitResponse {
            overall_code: overall_code as i32,
            statuses: decision.descriptor_statuses.iter()
                .map(|status| DescriptorStatus {
                    code: if status.allowed { Code::Ok } else { Code::OverLimit } as i32,
                    current_limit: None,  // TODO: populate from governor state
                    limit_remaining: status.limit_remaining.unwrap_or(0),
                    duration_until_reset: None,  // TODO: calculate from governor
                })
                .collect(),
            response_headers_to_add: vec![],
            request_headers_to_add: vec![],
            raw_body: None,
        };
        
        Ok(Response::new(response))
    }
}
```

### 5. Two-Phase Service Pattern

**File:** `src/service.rs`

Following `.patterns/services-pattern.md`:

```rust
use std::{future::Future, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use common::BoxError;
use monitoring::telemetry::metrics::Meter;

use crate::{
    config::RateLimitConfig,
    grpc::RateLimitServiceImpl,
    limiter::RateLimiterStore,
    metrics::Metrics,
    proto::rate_limit_service_server::RateLimitServiceServer,
};

/// Create and initialize the rate limit service
///
/// Sets up a gRPC server implementing the Envoy Rate Limit Service v3 protocol
/// with in-memory rate limiting using the governor crate.
///
/// # Phase 1: Initialization
/// - Creates rate limiter store with configured policies
/// - Binds TCP listener to the configured address
/// - Sets up OpenTelemetry metrics (if meter provided)
///
/// # Phase 2: Service Future
/// - Returns a future that runs the gRPC server
/// - Server runs until shutdown or error
///
/// # Returns
/// - Bound socket address
/// - Future that runs the service
pub async fn new(
    config: Arc<RateLimitConfig>,
    meter: Option<&Meter>,
) -> Result<(SocketAddr, impl Future<Output = Result<(), BoxError>>), InitError> {
    // Validate configuration
    config.validate().map_err(InitError::Config)?;
    
    // Phase 1: Initialization
    tracing::info!("Initializing rate limit service");
    
    // Create rate limiter store
    let limiter_store = Arc::new(RateLimiterStore::new(config.clone()));
    tracing::info!(
        domains = config.domains.len(),
        "Rate limiter store initialized"
    );
    
    // Create metrics
    let metrics = Arc::new(Metrics::new(meter));
    
    // Create gRPC service implementation
    let rls_service = RateLimitServiceImpl::new(limiter_store, metrics.clone());
    
    // Bind TCP listener
    let listener = TcpListener::bind(config.addr)
        .await
        .map_err(|source| InitError::Bind { addr: config.addr, source })?;
    
    let addr = listener.local_addr().map_err(InitError::LocalAddr)?;
    tracing::info!(addr = %addr, "Rate limit service bound to address");
    
    // Phase 2: Service future
    let fut = async move {
        tracing::info!("Starting gRPC server");
        
        Server::builder()
            .add_service(RateLimitServiceServer::new(rls_service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "gRPC server error");
                err.into()
            })
    };
    
    Ok((addr, fut))
}

/// Errors that can occur when creating the rate limit service
#[derive(Debug, thiserror::Error)]
pub enum InitError {
    /// Failed to bind gRPC server to the specified address
    ///
    /// This occurs when:
    /// - The address is already in use by another process
    /// - The port requires elevated privileges (e.g., port < 1024)
    /// - The address is not available on this system
    #[error("failed to bind gRPC server to {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },
    
    /// Failed to get local address from TCP listener
    ///
    /// This occurs when:
    /// - Socket state is invalid after binding
    #[error("failed to get local address: {0}")]
    LocalAddr(#[source] std::io::Error),
    
    /// Failed to load or validate configuration
    ///
    /// This occurs when:
    /// - Configuration file is invalid
    /// - Required fields are missing
    /// - Validation rules are violated
    #[error("failed to load configuration: {0}")]
    Config(#[source] crate::config::ConfigError),
}
```

### 6. Metrics Implementation

**File:** `src/metrics.rs`

Following patterns from `docs/metrics.md`:

```rust
use monitoring::telemetry::metrics::Meter;
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};

pub struct Metrics {
    /// Total rate limit checks performed since service start
    ///
    /// Use to track overall query load and system utilization
    pub check_count_total: Counter<u64>,
    
    /// Duration of rate limit check operations
    ///
    /// Histogram enables percentile calculations for SLA monitoring
    pub check_duration_ms: Histogram<f64>,
    
    /// Rate limit decisions by outcome
    ///
    /// Labels: decision={allowed,denied}
    pub decision_total: Counter<u64>,
    
    /// Active gRPC connections
    ///
    /// UpDownCounter increments on connect, decrements on disconnect
    pub active_connections: UpDownCounter<i64>,
    
    /// Rate limiter cache operations
    ///
    /// Labels: operation={hit,miss,create}
    pub cache_operations_total: Counter<u64>,
    
    /// Number of cached rate limiters
    ///
    /// Gauge of current cache size
    pub cached_limiters: UpDownCounter<i64>,
}

impl Metrics {
    pub fn new(meter: Option<&Meter>) -> Self {
        let meter = match meter {
            Some(m) => m,
            None => return Self::noop(),
        };
        
        Self {
            check_count_total: meter
                .u64_counter("ratelimit.check.count.total")
                .with_description("Total rate limit checks performed")
                .with_unit("checks")
                .build(),
            
            check_duration_ms: meter
                .f64_histogram("ratelimit.check.duration.milliseconds")
                .with_description("Duration of rate limit check operations")
                .with_unit("ms")
                .build(),
            
            decision_total: meter
                .u64_counter("ratelimit.decision.total")
                .with_description("Rate limit decisions by outcome")
                .with_unit("decisions")
                .build(),
            
            active_connections: meter
                .i64_up_down_counter("ratelimit.connections.active")
                .with_description("Active gRPC connections")
                .with_unit("connections")
                .build(),
            
            cache_operations_total: meter
                .u64_counter("ratelimit.cache.operations.total")
                .with_description("Rate limiter cache operations")
                .with_unit("operations")
                .build(),
            
            cached_limiters: meter
                .i64_up_down_counter("ratelimit.cache.limiters")
                .with_description("Number of cached rate limiters")
                .with_unit("limiters")
                .build(),
        }
    }
    
    fn noop() -> Self {
        // Return metrics that do nothing when meter is not provided
        Self {
            check_count_total: Counter::new(),  // TODO: Use actual noop implementation
            check_duration_ms: Histogram::new(),
            decision_total: Counter::new(),
            active_connections: UpDownCounter::new(),
            cache_operations_total: Counter::new(),
            cached_limiters: UpDownCounter::new(),
        }
    }
}
```

### 7. Standalone Binary

**File:** `crates/bin/ampratelimit/src/main.rs`

```rust
use clap::Parser;
use std::sync::Arc;
use common::BoxError;
use monitoring::telemetry;

#[derive(Debug, Parser)]
#[command(
    name = "ampratelimit",
    version,
    about = "Envoy-compatible rate limiting service",
    long_about = "A standalone rate limiting service implementing Envoy's Rate Limit Service (RLS) v3 protocol.\n\
                  Uses in-memory rate limiting with the governor crate for high performance."
)]
struct Args {
    /// Path to configuration file
    #[arg(long, env = "AMP_RATELIMIT_CONFIG", default_value = "ratelimit.toml")]
    config: String,
    
    /// Enable OpenTelemetry metrics export
    #[arg(long, env = "AMP_RATELIMIT_METRICS")]
    metrics: bool,
    
    /// OpenTelemetry collector endpoint
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otel_endpoint: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // Parse CLI arguments
    let args = Args::parse();
    
    // Initialize tracing/logging
    telemetry::logging::init();
    
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        "Starting ampratelimit"
    );
    
    // Load configuration
    tracing::info!(config_path = %args.config, "Loading configuration");
    let config = Arc::new(
        ratelimit::config::RateLimitConfig::load(&args.config)
            .map_err(|e| {
                tracing::error!(error = %e, "Failed to load configuration");
                e
            })?
    );
    
    tracing::info!(
        domains = config.domains.len(),
        addr = %config.addr,
        "Configuration loaded"
    );
    
    // Setup telemetry if enabled
    let _telemetry_guard = if args.metrics {
        tracing::info!("Initializing OpenTelemetry metrics");
        let guard = telemetry::metrics::init(args.otel_endpoint.as_deref())?;
        Some(guard)
    } else {
        tracing::info!("Metrics disabled");
        None
    };
    
    let meter = _telemetry_guard.as_ref()
        .map(|g| g.meter());
    
    // Create and run rate limit service
    let (addr, service_fut) = ratelimit::service::new(config, meter).await?;
    
    tracing::info!(addr = %addr, "Rate limit service listening");
    println!("Rate limit service listening on {}", addr);
    
    // Run service until shutdown
    service_fut.await?;
    
    tracing::info!("Rate limit service shutting down");
    Ok(())
}
```

**File:** `crates/bin/ampratelimit/build.rs`

```rust
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    vergen_gitcl::Emitter::default()
        .add_instructions(&vergen_gitcl::BuildBuilder::default().build()?)?
        .emit()?;
    Ok(())
}
```

## Dependencies

### Workspace `Cargo.toml` Updates

**No changes needed** - `governor` already present at v0.10.0

### Service Crate Dependencies

**File:** `crates/services/ratelimit/Cargo.toml`

```toml
[package]
name = "ratelimit"
edition.workspace = true
version.workspace = true
license-file.workspace = true
build = "build.rs"

[dependencies]
common = { path = "../../core/common" }
monitoring = { path = "../../core/monitoring" }
dashmap.workspace = true
governor.workspace = true
prost.workspace = true
serde.workspace = true
thiserror.workspace = true
tokio = { workspace = true, features = ["net"] }
tokio-stream.workspace = true
toml.workspace = true
tonic.workspace = true
tracing.workspace = true

[build-dependencies]
prost-build.workspace = true
tonic-build.workspace = true
```

### Binary Crate Dependencies

**File:** `crates/bin/ampratelimit/Cargo.toml`

```toml
[package]
name = "ampratelimit"
edition.workspace = true
version.workspace = true
license-file.workspace = true
build = "build.rs"

[dependencies]
ratelimit = { path = "../../services/ratelimit" }
common = { path = "../../core/common" }
monitoring = { path = "../../core/monitoring" }
clap.workspace = true
tokio.workspace = true
tracing.workspace = true

[build-dependencies]
vergen-gitcl = { version = "1.0.8", features = ["build"] }
```

### Workspace Member Updates

**File:** `Cargo.toml` (root)

Add to `[workspace.members]`:
```toml
members = [
    # ... existing members ...
    "crates/bin/ampratelimit",
    "crates/services/ratelimit",
]
```

## Testing Strategy

Following `.patterns/testing-patterns.md`:

### Unit Tests

**File:** `src/limiter.rs` (inline tests)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_config() -> RateLimitConfig {
        RateLimitConfig {
            addr: "127.0.0.1:8081".parse().unwrap(),
            default_hits_addend: 1,
            domains: vec![
                DomainConfig {
                    name: "test".to_string(),
                    descriptors: vec![
                        DescriptorConfig {
                            key: "client_id".to_string(),
                            value: None,
                            rate_limit: RateLimit {
                                requests_per_unit: 10,
                                unit: TimeUnit::Second,
                            },
                        },
                    ],
                },
            ],
        }
    }

    #[test]
    fn test_rate_limit_allows_within_quota() {
        // GIVEN a rate limiter with 10 requests per second
        let config = Arc::new(test_config());
        let store = RateLimiterStore::new(config);
        
        let descriptor = create_test_descriptor("client_id", "user123");
        
        // WHEN checking 5 requests
        let mut results = vec![];
        for _ in 0..5 {
            results.push(store.check_rate_limit("test", &[descriptor.clone()], 1));
        }
        
        // THEN all should be allowed
        assert!(results.iter().all(|r| r.as_ref().unwrap().allowed));
    }

    #[test]
    fn test_rate_limit_blocks_over_quota() {
        // GIVEN a rate limiter with 5 requests per second
        let config = Arc::new(RateLimitConfig {
            addr: "127.0.0.1:8081".parse().unwrap(),
            default_hits_addend: 1,
            domains: vec![
                DomainConfig {
                    name: "test".to_string(),
                    descriptors: vec![
                        DescriptorConfig {
                            key: "client_id".to_string(),
                            value: None,
                            rate_limit: RateLimit {
                                requests_per_unit: 5,
                                unit: TimeUnit::Second,
                            },
                        },
                    ],
                },
            ],
        });
        let store = RateLimiterStore::new(config);
        
        let descriptor = create_test_descriptor("client_id", "user456");
        
        // WHEN checking 10 requests
        let mut allowed = 0;
        let mut denied = 0;
        for _ in 0..10 {
            let result = store.check_rate_limit("test", &[descriptor.clone()], 1).unwrap();
            if result.allowed {
                allowed += 1;
            } else {
                denied += 1;
            }
        }
        
        // THEN first 5 allowed, rest denied
        assert_eq!(allowed, 5);
        assert_eq!(denied, 5);
    }

    #[test]
    fn test_multiple_descriptors_all_must_pass() {
        // GIVEN rate limiters for two keys
        // WHEN one is over limit
        // THEN overall decision is OVER_LIMIT
    }

    #[test]
    fn test_cache_key_generation() {
        // GIVEN various descriptor combinations
        // WHEN building cache keys
        // THEN keys should be unique and deterministic
    }

    fn create_test_descriptor(key: &str, value: &str) -> proto::RateLimitDescriptor {
        proto::RateLimitDescriptor {
            entries: vec![
                proto::rate_limit_descriptor::Entry {
                    key: key.to_string(),
                    value: value.to_string(),
                },
            ],
        }
    }
}
```

### Integration Tests

**File:** `tests/grpc_integration.rs`

```rust
use ratelimit::proto::{
    rate_limit_service_client::RateLimitServiceClient,
    RateLimitRequest, RateLimitDescriptor,
    rate_limit_descriptor::Entry,
    rate_limit_response::Code,
};

#[tokio::test]
async fn test_envoy_grpc_integration() {
    // GIVEN a running rate limit service
    let config = create_test_config();
    let (addr, service_fut) = ratelimit::service::new(Arc::new(config), None)
        .await
        .unwrap();
    
    // Run service in background
    tokio::spawn(service_fut);
    
    // WHEN Envoy sends a ShouldRateLimit request
    let mut client = RateLimitServiceClient::connect(
        format!("http://{}", addr)
    ).await.unwrap();
    
    let request = RateLimitRequest {
        domain: "test".to_string(),
        descriptors: vec![
            RateLimitDescriptor {
                entries: vec![
                    Entry {
                        key: "client_id".to_string(),
                        value: "test_client".to_string(),
                    },
                ],
            },
        ],
        hits_addend: 1,
    };
    
    let response = client.should_rate_limit(request).await.unwrap();
    
    // THEN response should be OK
    let response = response.into_inner();
    assert_eq!(response.overall_code, Code::Ok as i32);
}

#[tokio::test]
async fn test_rate_limit_enforcement() {
    // GIVEN a service with 5 req/sec limit
    // WHEN sending 10 rapid requests
    // THEN first 5 OK, rest OVER_LIMIT
}
```

### Property-Based Tests

**File:** `tests/property_tests.rs`

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_rate_limit_never_exceeds_quota(
        request_count in 1u32..1000,
        quota in 1u32..100,
    ) {
        // GIVEN a rate limiter with arbitrary quota
        // WHEN sending arbitrary number of requests
        // THEN never more than quota requests allowed in time window
    }
}
```

## Documentation

### 1. Service README

**File:** `crates/services/ratelimit/README.md`

```markdown
# Rate Limiting Service

Standalone gRPC service implementing the [Envoy Rate Limit Service (RLS) v3 protocol](https://www.envoyproxy.io/docs/envoy/latest/api-v3/service/ratelimit/v3/rls.proto).

## Features

- **In-memory rate limiting** using the `governor` crate (GCRA algorithm)
- **Envoy RLS v3 protocol** for seamless integration
- **Static configuration** via TOML files
- **OpenTelemetry metrics** for observability
- **High performance** with concurrent request handling

## Usage

### Configuration

Create a `ratelimit.toml` configuration file:

\`\`\`toml
[ratelimit]
addr = "0.0.0.0:8081"

[[ratelimit.domains]]
name = "api"
  [[ratelimit.domains.descriptors]]
  key = "client_id"
  rate_limit = { requests_per_unit = 100, unit = "second" }
\`\`\`

### Running the Service

\`\`\`bash
ampratelimit --config ratelimit.toml
\`\`\`

With metrics:

\`\`\`bash
ampratelimit --config ratelimit.toml --metrics --otel-endpoint http://localhost:4317
\`\`\`

## Rate Limiting Algorithm

Uses the [Generic Cell Rate Algorithm (GCRA)](https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm) 
via the `governor` crate. This provides:

- **Token bucket** semantics
- **Atomic** operations for thread safety
- **Low overhead** in-memory state
- **Burst handling** with configurable limits

## Configuration Reference

See [Configuration Guide](../../docs/ratelimit-config.md) for full details.

## Metrics

See [Metrics Documentation](../../docs/metrics.md#rate-limiting-metrics) for exported metrics.

## Integration with Envoy

See [Envoy Integration Guide](../../docs/envoy-ratelimit-integration.md).
```

### 2. Envoy Integration Guide

**File:** `docs/envoy-ratelimit-integration.md`

```markdown
# Envoy Rate Limiting Integration Guide

This guide explains how to configure Envoy to use the `ampratelimit` service.

## Architecture

\`\`\`
┌─────────┐     HTTP      ┌─────────┐      gRPC       ┌──────────────┐
│ Client  │ ─────────────► │  Envoy  │ ───────────────► │ ampratelimit │
└─────────┘                └─────────┘  ShouldRateLimit └──────────────┘
                                │
                                │ Check rate limit
                                │ before forwarding
                                ▼
                         ┌─────────────┐
                         │   Backend   │
                         └─────────────┘
\`\`\`

## Example Envoy Configuration

\`\`\`yaml
static_resources:
  listeners:
  - address:
      socket_address:
        address: 0.0.0.0
        port_value: 8080
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: ingress_http
          route_config:
            name: local_route
            virtual_hosts:
            - name: backend
              domains: ["*"]
              routes:
              - match: { prefix: "/" }
                route: { cluster: backend_service }
                rate_limits:
                - actions:
                  - request_headers:
                      header_name: "x-client-id"
                      descriptor_key: "client_id"
          http_filters:
          - name: envoy.filters.http.ratelimit
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.ratelimit.v3.RateLimit
              domain: api
              failure_mode_deny: true
              rate_limit_service:
                grpc_service:
                  envoy_grpc:
                    cluster_name: ratelimit_cluster
                transport_api_version: V3
          - name: envoy.filters.http.router

  clusters:
  - name: ratelimit_cluster
    type: STRICT_DNS
    connect_timeout: 0.25s
    lb_policy: ROUND_ROBIN
    load_assignment:
      cluster_name: ratelimit_cluster
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: ampratelimit
                port_value: 8081
    http2_protocol_options: {}
\`\`\`

## Rate Limit Descriptor Examples

### By Client ID

\`\`\`yaml
rate_limits:
- actions:
  - request_headers:
      header_name: "x-client-id"
      descriptor_key: "client_id"
\`\`\`

### By API Key

\`\`\`yaml
rate_limits:
- actions:
  - request_headers:
      header_name: "x-api-key"
      descriptor_key: "api_key"
\`\`\`

### By Remote Address

\`\`\`yaml
rate_limits:
- actions:
  - remote_address: {}
\`\`\`

### Composite (Client ID + Path)

\`\`\`yaml
rate_limits:
- actions:
  - request_headers:
      header_name: "x-client-id"
      descriptor_key: "client_id"
  - request_headers:
      header_name: ":path"
      descriptor_key: "path"
\`\`\`

## Testing

Test with curl:

\`\`\`bash
# Send request with client ID header
curl -H "x-client-id: test_client" http://localhost:8080/

# Rapid requests to trigger rate limit
for i in {1..100}; do
  curl -H "x-client-id: test_client" http://localhost:8080/
done
\`\`\`

## Monitoring

Key metrics to watch:
- `ratelimit.check.count.total` - Total checks
- `ratelimit.decision.total{decision="denied"}` - Rate limit hits
- `ratelimit.check.duration.milliseconds` - Latency

## Troubleshooting

### Envoy can't connect to rate limit service

Check that ampratelimit is running and accessible:
\`\`\`bash
grpcurl -plaintext localhost:8081 list
\`\`\`

### Rate limits not being enforced

1. Check Envoy logs for rate limit filter activation
2. Verify descriptor configuration matches between Envoy and ampratelimit
3. Check ampratelimit logs for incoming requests

### High latency

1. Check `ratelimit.check.duration.milliseconds` metric
2. Verify network latency between Envoy and ampratelimit
3. Consider running ampratelimit closer to Envoy (same node/pod)
```

### 3. Configuration Reference

**File:** `docs/ratelimit-config.md`

Full configuration reference with all options, examples, and best practices.

### 4. Metrics Documentation Update

**File:** `docs/metrics.md` (add section)

```markdown
## Rate Limiting Metrics

**Location:** `crates/services/ratelimit/src/metrics.rs`

### ratelimit.check.count.total

**Type:** Counter
**Labels:** None

Total number of rate limit checks performed since service start.

### ratelimit.check.duration.milliseconds

**Type:** Histogram
**Unit:** milliseconds
**Labels:** None

Duration of rate limit check operations from request receipt to response.

### ratelimit.decision.total

**Type:** Counter
**Labels:** `decision={allowed,denied}`

Rate limit decisions by outcome. Use to calculate denial rate.

### ratelimit.connections.active

**Type:** UpDownCounter
**Labels:** None

Current number of active gRPC connections from Envoy proxies.

### ratelimit.cache.operations.total

**Type:** Counter
**Labels:** `operation={hit,miss,create}`

Rate limiter cache operations for performance monitoring.

### ratelimit.cache.limiters

**Type:** UpDownCounter
**Labels:** None

Current number of cached rate limiters in memory.
```

## Development Workflow

Following `AGENTS.md` development workflow:

### Pre-Implementation Checklist

- [x] Research Envoy RLS v3 protocol
- [x] Research governor crate API
- [x] Review `.patterns/services-pattern.md`
- [x] Review `.patterns/error-reporting.md`
- [x] Review `.patterns/cargo-workspace-patterns.md`
- [x] Create detailed implementation plan

### Implementation Workflow

For **each file** created/modified:

1. **Write code** following patterns
2. **Format immediately**: `just fmt-file <file>` (MANDATORY)
3. **Check compilation**: `just check-crate ratelimit`
4. **Fix errors**: Resolve all compilation errors
5. **Lint**: `just clippy-crate ratelimit`
6. **Fix warnings**: Resolve ALL clippy warnings (zero tolerance)
7. **Test**: `just test-local`
8. **Fix failures**: Resolve all test failures

### Validation Gates

Before proceeding to next component:
- [ ] All files formatted (`just fmt-rs-check` passes)
- [ ] Compilation succeeds (`just check-crate ratelimit` passes)
- [ ] No clippy warnings (`just clippy-crate ratelimit` clean)
- [ ] All tests passing (`just test-local` passes)

### Final Checklist

Before marking as complete:
- [ ] All automated checks pass
- [ ] Documentation complete
- [ ] Example configurations provided
- [ ] Integration tests passing
- [ ] Metrics properly instrumented
- [ ] Error handling follows patterns
- [ ] Code follows workspace conventions

## Implementation Order

Recommended order to minimize dependencies and enable incremental testing:

1. **Create crate structure**
   - Add to workspace members
   - Create `Cargo.toml` files
   - Create directory structure

2. **Proto definitions**
   - Add proto files
   - Create `build.rs`
   - Test proto compilation

3. **Configuration**
   - Implement config types
   - Add validation
   - Test config loading

4. **Rate limiter**
   - Implement `RateLimiterStore`
   - Add governor integration
   - Write unit tests

5. **Descriptor handling**
   - Implement descriptor matching
   - Add cache key generation
   - Test matching logic

6. **Metrics**
   - Implement metrics struct
   - Add OpenTelemetry integration
   - Test metric recording

7. **gRPC service**
   - Implement RLS service
   - Wire up limiter and metrics
   - Write integration tests

8. **Two-phase service**
   - Implement service::new()
   - Add initialization logic
   - Test service lifecycle

9. **Standalone binary**
   - Create CLI
   - Add telemetry setup
   - Test end-to-end

10. **Documentation**
    - Write README files
    - Create integration guide
    - Add configuration examples

## Security Considerations

### Input Validation

- Validate all descriptor keys and values
- Limit descriptor depth/complexity
- Reject malformed requests

### Resource Limits

- Limit number of cached limiters (prevent memory exhaustion)
- Set max concurrent connections
- Configure request timeout

### Network Security

- Support TLS for gRPC (future enhancement)
- Rate limit the rate limiter itself (prevent DoS)
- Log suspicious patterns

### Configuration Security

- Validate all config values
- Prevent integer overflows in rate calculations
- Secure config file permissions

## Performance Considerations

### Memory Usage

- In-memory state scales with unique descriptor combinations
- Each cached limiter uses ~100 bytes
- Monitor `ratelimit.cache.limiters` metric

### Latency

- Target: < 1ms p99 for rate limit checks
- Governor operations are O(1)
- DashMap provides lock-free reads

### Concurrency

- DashMap supports concurrent access
- Governor rate limiters are thread-safe
- gRPC server handles concurrent requests

### Scalability

- Single instance can handle 10k+ req/sec
- Horizontal scaling: run multiple instances
- State is local (no coordination overhead)

## Future Enhancements

### Phase 2 Features (Not in Initial Implementation)

1. **Distributed state** - Redis/Memcached backend
2. **Dynamic configuration** - Hot reload without restart
3. **Advanced rate limit headers** - `X-RateLimit-*` headers
4. **Quota caching** - RLS protocol quota response
5. **Custom algorithms** - Sliding window, leaky bucket
6. **Per-route limits** - Fine-grained policy matching
7. **TLS support** - Secure gRPC communication
8. **Admin API** - Runtime config inspection/modification

### Monitoring Enhancements

1. Per-domain metrics
2. Per-descriptor metrics
3. Rate limit hit distribution
4. Predictive alerting on quota exhaustion

## References

### External Documentation

- [Envoy Rate Limit Service v3](https://www.envoyproxy.io/docs/envoy/latest/api-v3/service/ratelimit/v3/rls.proto)
- [Envoy Rate Limiting](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/other_features/global_rate_limiting)
- [Governor Crate Documentation](https://docs.rs/governor/latest/governor/)
- [GCRA Algorithm](https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm)

### Internal Documentation

- `.patterns/services-pattern.md` - Two-phase service creation
- `.patterns/error-reporting.md` - Error handling patterns
- `.patterns/cargo-workspace-patterns.md` - Workspace management
- `docs/metrics.md` - Metrics standards

## Appendix: Proto File Dependencies

The Envoy RLS v3 protocol requires several dependent proto files. These will need to be obtained from the Envoy data-plane-api repository or vendored:

```
proto/
├── envoy/
│   ├── service/
│   │   └── ratelimit/
│   │       └── v3/
│   │           └── rls.proto
│   ├── extensions/
│   │   └── common/
│   │       └── ratelimit/
│   │           └── v3/
│   │               └── ratelimit.proto
│   └── config/
│       └── core/
│           └── v3/
│               └── base.proto
└── google/
    └── protobuf/
        ├── duration.proto
        ├── timestamp.proto
        └── struct.proto
```

**Build Script Strategy:**

1. Use `tonic-build` with include paths
2. Vendor required proto files in `proto/` directory
3. Generate Rust code during build

## Conclusion

This implementation plan provides a complete roadmap for building a production-ready Envoy rate limiting service. The service will:

- ✅ Implement Envoy RLS v3 protocol
- ✅ Use in-memory rate limiting with governor
- ✅ Follow Amp project patterns and conventions
- ✅ Provide comprehensive observability
- ✅ Include thorough testing
- ✅ Deliver complete documentation

The modular design allows for future enhancements while maintaining a clean, maintainable codebase following all Amp project standards.
