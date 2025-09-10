use std::{
    future::Future,
    num::NonZeroU32,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use alloy::{
    network::AnyNetwork,
    providers::{ProviderBuilder, RootProvider},
    rpc::client::ClientBuilder,
};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use tower::{Layer, Service};
use url::Url;

use crate::BoxError;

pub fn new(url: Url, rate_limit: Option<NonZeroU32>) -> RootProvider<AnyNetwork> {
    let client_builder = ClientBuilder::default();

    let client = match rate_limit {
        Some(rate_limit) => client_builder
            .layer(RateLimitLayer::new(rate_limit))
            .http(url),
        None => client_builder.http(url),
    };

    ProviderBuilder::new()
        .disable_recommended_fillers()
        .network::<AnyNetwork>()
        .connect_client(client)
}

pub async fn new_ipc<P: AsRef<Path>>(
    path: P,
    rate_limit: Option<NonZeroU32>,
) -> Result<RootProvider<AnyNetwork>, BoxError> {
    let client_builder = ClientBuilder::default();

    let client = match rate_limit {
        Some(rate_limit) => {
            client_builder
                .layer(RateLimitLayer::new(rate_limit))
                .ipc(path.as_ref().to_path_buf().into())
                .await?
        }
        None => {
            client_builder
                .ipc(path.as_ref().to_path_buf().into())
                .await?
        }
    };

    Ok(ProviderBuilder::new()
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
