use std::{
    future::Future,
    num::NonZeroU32,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use alloy::{
    network::AnyNetwork,
    providers::{
        ProviderBuilder as AlloyProviderBuilder, RootProvider as AlloyRootProvider, WsConnect,
    },
    rpc::client::ClientBuilder,
    transports::{Authorization as TransportAuthorization, TransportError, http::reqwest::Client},
};
use amp_providers_common::config::{InvalidConfigError, ProviderResolvedConfigRaw, TryIntoConfig};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use headers::{Authorization, HeaderMap, HeaderMapExt, HeaderName, HeaderValue};
use tower::{Layer, Service};
use url::Url;

use crate::config::{AuthHeaderName, AuthToken, EvmRpcProviderConfig};

/// Type alias for an EVM RPC provider that works with any network.
///
/// This is an alloy `RootProvider` configured with `AnyNetwork`, allowing it to work
/// with any EVM-compatible blockchain network. The provider is constructed by
/// `create` based on the URL scheme in the provider configuration.
pub type EvmRpcAlloyProvider = AlloyRootProvider<AnyNetwork>;

/// Create an EVM RPC provider from configuration.
///
/// This function parses the provider configuration and constructs an EVM RPC provider.
/// It selects the transport based on the URL scheme:
/// - `http` / `https` → HTTP
/// - `ws` / `wss` → WebSocket
/// - `ipc` → IPC socket
///
/// Any other scheme returns [`CreateEvmRpcClientError::UnsupportedScheme`].
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

    // Resolve authentication configuration, if present
    let auth = typed_config
        .auth_token
        .map(|token| match typed_config.auth_header {
            Some(header) => Auth::CustomHeader {
                name: header,
                value: token,
            },
            None => Auth::Bearer(token),
        });

    match typed_config.url.scheme() {
        "http" | "https" => Ok(new_http(
            Url::clone(&typed_config.url),
            auth,
            typed_config.rate_limit_per_minute,
        )),
        "ws" | "wss" => new_ws(
            Url::clone(&typed_config.url),
            auth,
            typed_config.rate_limit_per_minute,
        )
        .await
        .map_err(CreateEvmRpcClientError::WsConnection),
        "ipc" => new_ipc(
            PathBuf::from(typed_config.url.path()),
            typed_config.rate_limit_per_minute,
        )
        .await
        .map_err(CreateEvmRpcClientError::IpcConnection),
        scheme => Err(CreateEvmRpcClientError::UnsupportedScheme(
            scheme.to_string(),
        )),
    }
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
    IpcConnection(#[source] TransportError),

    /// Failed to establish WebSocket connection.
    ///
    /// This occurs when using a `ws` or `wss` URL scheme and the WebSocket handshake fails
    /// due to network issues, invalid URL, authentication failure, or server unavailability.
    #[error("failed to establish WebSocket connection")]
    WsConnection(#[source] TransportError),

    /// Unsupported URL scheme in provider configuration.
    ///
    /// The URL scheme must be one of: `ipc`, `ws`, `wss`, `http`, `https`.
    #[error("unsupported URL scheme '{0}'")]
    UnsupportedScheme(String),
}

/// Authentication configuration for EVM RPC providers.
pub enum Auth {
    /// Standard `Authorization: Bearer <token>` header.
    Bearer(AuthToken),
    /// Custom header: `<name>: <value>` (e.g. `Drpc-Key: <token>`).
    CustomHeader {
        /// Header name (e.g. `Drpc-Key`).
        name: AuthHeaderName,
        /// Header value (raw token, no `Bearer` prefix).
        value: AuthToken,
    },
}

/// Create an HTTP/HTTPS EVM RPC provider.
pub fn new_http(
    url: Url,
    auth: Option<Auth>,
    rate_limit: Option<NonZeroU32>,
) -> EvmRpcAlloyProvider {
    let mut http_client_builder = Client::builder();
    http_client_builder = if let Some(auth) = auth {
        let mut headers = HeaderMap::new();
        match auth {
            Auth::Bearer(token) => {
                headers.typed_insert(
                    Authorization::bearer(&token.into_inner())
                        .expect("validated at config parse time"),
                );
            }
            Auth::CustomHeader { name, value: token } => {
                let name = HeaderName::try_from(name.into_inner())
                    .expect("validated at config parse time");
                let mut value = HeaderValue::try_from(token.into_inner())
                    .expect("validated at config parse time");
                value.set_sensitive(true);
                headers.insert(name, value);
            }
        }
        http_client_builder.default_headers(headers)
    } else {
        http_client_builder
    };
    let http_client = http_client_builder
        .build()
        .expect("http client with auth header");

    let client_builder = ClientBuilder::default();
    let client = if let Some(rl) = rate_limit {
        client_builder
            .layer(RateLimitLayer::new(rl))
            .http_with_client(http_client, url)
    } else {
        client_builder.http_with_client(http_client, url)
    };

    AlloyProviderBuilder::new()
        .disable_recommended_fillers()
        .network::<AnyNetwork>()
        .connect_client(client)
}

/// Create an IPC EVM RPC provider.
pub async fn new_ipc<P: AsRef<Path>>(
    path: P,
    rate_limit: Option<NonZeroU32>,
) -> Result<EvmRpcAlloyProvider, TransportError> {
    let client_builder = ClientBuilder::default();
    let client = if let Some(rl) = rate_limit {
        client_builder
            .layer(RateLimitLayer::new(rl))
            .ipc(path.as_ref().to_path_buf().into())
            .await?
    } else {
        client_builder
            .ipc(path.as_ref().to_path_buf().into())
            .await?
    };

    Ok(AlloyProviderBuilder::new()
        .disable_recommended_fillers()
        .network::<AnyNetwork>()
        .connect_client(client))
}

/// Create a WebSocket EVM RPC provider.
pub async fn new_ws(
    url: Url,
    auth: Option<Auth>,
    rate_limit: Option<NonZeroU32>,
) -> Result<EvmRpcAlloyProvider, TransportError> {
    let mut ws_connect = WsConnect::new(url);
    ws_connect = if let Some(a) = auth {
        let token = match a {
            Auth::Bearer(token) => token,
            Auth::CustomHeader { value: token, .. } => {
                tracing::warn!(
                    "custom auth header names are not supported for WebSocket connections; \
                     falling back to raw Authorization header"
                );
                token
            }
        };
        ws_connect.with_auth(TransportAuthorization::raw(token.into_inner()))
    } else {
        ws_connect
    };

    let client_builder = ClientBuilder::default();
    let client = if let Some(rl) = rate_limit {
        client_builder
            .layer(RateLimitLayer::new(rl))
            .ws(ws_connect)
            .await?
    } else {
        client_builder.ws(ws_connect).await?
    };

    Ok(AlloyProviderBuilder::new()
        .disable_recommended_fillers()
        .network::<AnyNetwork>()
        .connect_client(client))
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
