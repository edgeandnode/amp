//! Test step for registering streams with the Flight client.

use anyhow::Result;

use crate::testlib::fixtures::FlightClient;

/// Test step that registers a stream with the Flight client.
///
/// This step sets up a named stream that can be referenced in subsequent
/// test steps for streaming query operations.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    /// The name to assign to the registered stream.
    pub name: String,
    /// The stream identifier or query to register.
    pub stream: String,
}

impl Step {
    /// Registers the stream with the Flight client.
    ///
    /// Uses the Flight client to register a named stream that can be
    /// referenced in subsequent streaming operations.
    pub async fn run(&self, client: &mut FlightClient) -> Result<()> {
        tracing::debug!("Registering stream '{}'", self.stream);

        client.register_stream(&self.name, &self.stream).await
    }
}
