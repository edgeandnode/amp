//! Scenario builder for declarative streaming test setup.

use std::{
    collections::HashMap,
    future::{Future, IntoFuture},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common::arrow::datatypes::{DataType, Field, Schema};
use futures::{Stream as FuturesStream, StreamExt};

use crate::{
    client::ResponseBatch,
    error::Error,
    store::{InMemoryStateStore, StateStore},
    tests::utils::response::Step,
    transactional::{TransactionEvent, TransactionalStream},
};

/// Builder for creating test scenarios.
///
/// # Example
/// ```ignore
/// use amp_client::testlib::response::Step;
///
/// let events = Scenario::builder()
///     .push(Step::Data { range: 0..=10 })
///     .push(Step::Watermark { end: 10 })
///     .build()
///     .run().await?
///     .collect().await?;
/// ```
pub struct ScenarioBuilder {
    responses: Vec<ResponseBatch>,
    store: Option<Box<dyn StateStore>>,
    retention: u64,
    /// Track epoch per network for independent reorg simulation
    epochs: HashMap<String, u8>,
}

impl Scenario {
    /// Create a new scenario builder.
    pub fn builder() -> ScenarioBuilder {
        ScenarioBuilder {
            responses: Vec::new(),
            store: None,
            retention: 128,
            epochs: HashMap::new(),
        }
    }
}

impl ScenarioBuilder {
    /// Add a step to the scenario.
    ///
    /// If the step is marked with `.reorg()`, increments the epoch for those networks
    /// to generate different hashes, simulating a blockchain reorganization.
    pub fn push(mut self, step: Step) -> Self {
        // Get networks that should have epoch incremented
        let reorgs = step.get_reorg_networks();

        // Increment epoch for each reorg network
        for network in reorgs {
            let epoch = self.epochs.entry(network.clone()).or_insert(0);
            *epoch = epoch.wrapping_add(1);
        }

        // Convert step to batch using per-network epochs
        self.responses
            .push(step.into_batch_with_epochs(&self.epochs));

        self
    }

    /// Set a custom state store (default: InMemoryStateStore).
    pub fn with_store(mut self, store: impl StateStore + 'static) -> Self {
        self.store = Some(Box::new(store));
        self
    }

    /// Set the retention window in blocks (default: 128).
    pub fn with_retention(mut self, blocks: u64) -> Self {
        self.retention = blocks;
        self
    }

    /// Build the scenario into an executable.
    pub fn build(self) -> ScenarioRun {
        // Create state store with provided or default
        let store = self
            .store
            .unwrap_or_else(|| Box::new(InMemoryStateStore::new()));

        ScenarioRun {
            store,
            responses: self.responses,
            retention: self.retention,
        }
    }
}

/// Implement IntoFuture to allow `.await` on ScenarioBuilder directly.
///
/// # Example
/// ```ignore
/// let mut builder = Scenario::builder();
/// for i in 0..50 {
///     builder = builder.push(Step::data(i * 10..=(i * 10 + 9)));
/// }
/// let events = builder.await?; // Automatically builds, runs, and collects
/// ```
impl IntoFuture for ScenarioBuilder {
    type Output = Result<Vec<TransactionEvent>, Error>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.build().run().await?.collect().await })
    }
}

/// Executable scenario that can be run.
pub struct ScenarioRun {
    store: Box<dyn StateStore>,
    responses: Vec<ResponseBatch>,
    retention: u64,
}

impl ScenarioRun {
    /// Run the scenario and return the stream.
    pub async fn run(self) -> Result<ScenarioStream, Error> {
        // Capture responses for closure
        let responses = self.responses;
        // Use the generic constructor with mock stream (ignores resume parameter)
        let stream = TransactionalStream::create(self.store, self.retention, move |_| {
            let responses = responses.clone();
            async move { Ok(MockResponseStream::new(responses).into_raw_stream()) }
        })
        .await?;

        Ok(ScenarioStream { stream })
    }
}

/// Stream wrapper that provides different execution strategies.
pub struct ScenarioStream {
    stream: TransactionalStream,
}

impl ScenarioStream {
    /// Auto-commit all events and collect them (most common use case).
    ///
    /// # Example
    /// ```ignore
    /// let events = scenario.run().await?.collect().await?;
    /// assert_eq!(events.len(), 3);
    /// ```
    pub async fn collect(mut self) -> Result<Vec<TransactionEvent>, Error> {
        let mut events = Vec::new();
        while let Some(result) = futures::StreamExt::next(&mut self.stream).await {
            let (event, commit) = result?;
            events.push(event);
            commit.await?;
        }
        Ok(events)
    }

    /// Get raw stream with commit handles for manual control.
    ///
    /// # Example
    /// ```ignore
    /// let mut stream = scenario.run().await?.stream();
    /// while let Some((event, commit)) = stream.next().await {
    ///     // Manual processing
    /// }
    /// ```
    pub fn stream(self) -> TransactionalStream {
        self.stream
    }
}

/// Placeholder for Scenario struct (for builder() method).
pub struct Scenario;

/// Mock response stream that yields ResponseBatch from a Vec.
pub struct MockResponseStream {
    responses: Vec<ResponseBatch>,
    index: usize,
}

impl MockResponseStream {
    pub fn new(responses: Vec<ResponseBatch>) -> Self {
        Self {
            responses,
            index: 0,
        }
    }

    /// Create a mock RawStream for testing with a default schema.
    pub fn into_raw_stream(self) -> crate::client::RawStream {
        // Create a simple test schema matching the mock batches
        let schema = Arc::new(Schema::new(vec![Field::new("label", DataType::Utf8, true)]));

        crate::client::RawStream {
            inner: self.boxed(),
            schema,
        }
    }
}

impl FuturesStream for MockResponseStream {
    type Item = Result<ResponseBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.index < self.responses.len() {
            let response = self.responses[self.index].clone();
            self.index += 1;
            Poll::Ready(Some(Ok(response)))
        } else {
            Poll::Ready(None)
        }
    }
}
