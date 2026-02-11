//! Firehose provider configuration types.
//!
//! This module defines the configuration structure for Firehose providers,
//! including connection parameters and authentication tokens.

use amp_providers_common::{network_id::NetworkId, redacted::Redacted};
use url::Url;

use crate::kind::FirehoseProviderKind;

/// Configuration for a Firehose provider.
///
/// This structure defines the parameters required to connect to a Firehose
/// streaming endpoint for blockchain data extraction. The `kind` field validates
/// that the config belongs to a `firehose` provider at deserialization time.
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FirehoseProviderConfig {
    /// The provider kind, must be `"firehose"`.
    pub kind: FirehoseProviderKind,

    /// The network this provider serves.
    pub network: NetworkId,

    /// The URL of the Firehose endpoint.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub url: Redacted<Url>,

    /// Optional authentication token for the Firehose endpoint.
    pub token: Option<Redacted<String>>,
}
