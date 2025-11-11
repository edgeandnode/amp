//! Shared command-line arguments for ampctl commands.

use url::Url;

/// Global arguments shared across all commands that interact with the admin API.
///
/// This struct contains common options that are used by most ampctl commands.
/// Commands can include these options by using `#[command(flatten)]` in their Args struct.
#[derive(Debug, clap::Args)]
pub struct GlobalArgs {
    /// The URL of the engine admin interface
    #[arg(long, env = "AMP_ADMIN_URL", default_value = "http://localhost:1610", value_parser = clap::value_parser!(Url))]
    pub admin_url: Url,

    /// Bearer token for authenticating requests to the admin API
    #[arg(long, env = "AMP_AUTH_TOKEN")]
    pub auth_token: Option<String>,
}

impl GlobalArgs {
    /// Create a client using the admin URL and optional authentication token.
    ///
    /// # Errors
    ///
    /// Returns an error if the token is invalid or the client cannot be built.
    pub fn build_client(&self) -> Result<crate::client::Client, BuildClientError> {
        let mut client_builder = crate::client::build(self.admin_url.clone());

        if let Some(token) = self.auth_token.as_deref() {
            client_builder = client_builder
                .with_bearer_token(token.parse().map_err(BuildClientError::InvalidAuthToken)?);
        }

        client_builder
            .build()
            .map_err(BuildClientError::ClientBuildError)
    }
}

/// Errors that can occur when building a client from [`GlobalArgs`].
#[derive(Debug, thiserror::Error)]
pub enum BuildClientError {
    /// Invalid authentication token
    #[error("invalid authentication token")]
    InvalidAuthToken(#[source] crate::client::auth::BearerTokenError),

    /// Failed to build client
    #[error("failed to build admin API client")]
    ClientBuildError(#[source] crate::client::BuildError),
}
