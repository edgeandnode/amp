use std::{
    future::Future,
    num::NonZeroU32,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use alloy::{
    network::AnyNetwork,
    providers::{ProviderBuilder as AlloyProviderBuilder, RootProvider as AlloyRootProvider},
    rpc::client::ClientBuilder,
};
use amp_providers_common::config::{InvalidConfigError, ProviderResolvedConfigRaw, TryIntoConfig};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use tower::{Layer, Service};

use crate::config::EvmRpcProviderConfig;

/// Type alias for an EVM RPC provider that works with any network.
///
/// This is an alloy `RootProvider` configured with `AnyNetwork`, allowing it to work
/// with any EVM-compatible blockchain network. The provider is constructed by
/// `create` based on the URL scheme in the provider configuration.
pub type EvmRpcAlloyProvider = AlloyRootProvider<AnyNetwork>;

/// Create an EVM RPC provider from configuration.
///
/// This function parses the provider configuration and constructs an EVM RPC provider.
/// It automatically detects whether to use IPC or HTTP/HTTPS based on the URL scheme
/// in the provider configuration.
///
/// The configuration must already have environment variable substitution applied
/// (i.e., it should be a `ProviderResolvedConfigRaw` from `with_env_substitution()`).
pub async fn create(
    provider_name: String,
    config: ProviderResolvedConfigRaw,
) -> Result<EvmRpcAlloyProvider, CreateEvmRpcClientError> {
    let typed_config = config
        .try_into_config::<EvmRpcProviderConfig>()
        .map_err(|source| CreateEvmRpcClientError::ConfigParse {
            name: provider_name,
            source,
        })?;

    // Construct the provider based on URL scheme
    let client_builder = ClientBuilder::default();

    let client = if typed_config.url.scheme() == "ipc" {
        // IPC connection
        let path = PathBuf::from(typed_config.url.path());
        match typed_config.rate_limit_per_minute {
            Some(rate_limit) => client_builder
                .layer(RateLimitLayer::new(rate_limit))
                .ipc(path.into())
                .await
                .map_err(CreateEvmRpcClientError::IpcConnection)?,
            None => client_builder
                .ipc(path.into())
                .await
                .map_err(CreateEvmRpcClientError::IpcConnection)?,
        }
    } else {
        // HTTP/HTTPS connection
        match typed_config.rate_limit_per_minute {
            Some(rate_limit) => client_builder
                .layer(RateLimitLayer::new(rate_limit))
                .http(typed_config.url),
            None => client_builder.http(typed_config.url),
        }
    };

    let provider = AlloyProviderBuilder::new()
        .disable_recommended_fillers()
        .network::<AnyNetwork>()
        .connect_client(client);

    Ok(provider)
}

/// Errors that occur when creating an EVM RPC client.
#[derive(Debug, thiserror::Error)]
pub enum CreateEvmRpcClientError {
    /// Failed to parse the provider configuration into the expected type.
    ///
    /// This occurs when a provider was found for the network but its configuration
    /// could not be deserialized into `EvmRpcProviderConfig`.
    ///
    /// Possible causes:
    /// - Missing required `url` field in provider configuration
    /// - Invalid URL format in the `url` field
    /// - Invalid value type for `rate_limit_per_minute` (must be positive integer)
    #[error("failed to parse provider configuration for '{name}'")]
    ConfigParse {
        name: String,
        #[source]
        source: InvalidConfigError,
    },

    /// Failed to establish IPC connection.
    ///
    /// This occurs when using an IPC URL scheme and the connection to the provider socket
    /// fails due to socket unavailability, permission issues, or other IPC-specific problems.
    #[error("failed to establish IPC connection")]
    IpcConnection(#[source] alloy::transports::TransportError),

    /// Failed to substitute environment variables in provider configuration.
    ///
    /// This occurs when a provider configuration references environment variables using
    /// ${VAR} syntax, but the variable is not set in the environment or has invalid syntax.
    ///
    /// Possible causes:
    /// - Environment variable referenced in config is not set
    /// - Invalid ${} syntax in configuration value
    /// - Empty variable name in ${} placeholder
    #[error("failed to substitute environment variables in provider configuration")]
    EnvSubstitution(#[source] amp_providers_common::envsub::Error),
}

struct RateLimitLayer {
    limiter: Arc<DefaultDirectRateLimiter>,
}

impl RateLimitLayer {
    fn new(rate_limit: NonZeroU32) -> Self {
        let quota = Quota::per_minute(rate_limit);
        let limiter = Arc::new(RateLimiter::direct(quota));
        RateLimitLayer { limiter }
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            limiter: Arc::clone(&self.limiter),
        }
    }
}

#[derive(Clone)]
struct RateLimitService<S> {
    inner: S,
    limiter: Arc<DefaultDirectRateLimiter>,
}

impl<S, Request> Service<Request> for RateLimitService<S>
where
    S: Service<Request> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let inner_fut = self.inner.call(req);
        let limiter = Arc::clone(&self.limiter);

        Box::pin(async move {
            limiter.until_ready().await;
            inner_fut.await
        })
    }
}
