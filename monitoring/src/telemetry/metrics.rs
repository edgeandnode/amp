use std::{
    borrow::Cow,
    sync::{
        LazyLock,
        atomic::{AtomicU64, Ordering},
    },
};

use opentelemetry_otlp::{ExporterBuildError, Protocol, WithExportConfig};
pub use opentelemetry_sdk::metrics::SdkMeterProvider;

pub type Result = std::result::Result<SdkMeterProvider, ExporterBuildError>;

static METER: LazyLock<opentelemetry::metrics::Meter> =
    LazyLock::new(|| opentelemetry::global::meter("nozzle_meter"));

/// Starts a periodic OpenTelemetry metrics exporter over binary HTTP transport.
pub fn start(url: String) -> Result {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(url)
        .build()?;

    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    Ok(meter_provider)
}

/// An OpenTelemetry counter.
///
/// As this type does not allow reading the current value, use a [ReadableCounter] if this
/// functionality is needed.
pub struct Counter(opentelemetry::metrics::Counter<u64>);

impl Counter {
    /// Create a new OpenTelemetry counter.
    pub fn new(
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self(
            METER
                .u64_counter(name)
                .with_description(description)
                .build(),
        )
    }

    /// Increment the OpenTelemetry counter.
    pub fn inc(&self) {
        self.0.add(1, &[]);
    }

    /// Increment the OpenTelemetry counter by the given amount.
    pub fn inc_by(&self, value: u64) {
        self.0.add(value, &[]);
    }
}

/// An OpenTelemetry counter that can also be read from.
///
/// If reading the counter value isn't needed, users can create a regular [Counter].
pub struct ReadableCounter {
    counter: opentelemetry::metrics::Counter<u64>,
    /// A local copy used to enable reading the counter value.
    copy: AtomicU64,
}

impl ReadableCounter {
    /// Creates a new readable OpenTelemetry counter.
    pub fn new(
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
    ) -> Self {
        let otpl_counter = METER
            .u64_counter(name)
            .with_description(description)
            .build();
        Self {
            counter: otpl_counter,
            copy: AtomicU64::new(0),
        }
    }

    /// Increment the OpenTelemetry counter.
    pub fn inc(&self) {
        self.counter.add(1, &[]);
        self.copy.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the OpenTelemetry counter by the given amount.
    pub fn inc_by(&self, value: u64) {
        self.counter.add(value, &[]);
        self.copy.fetch_add(value, Ordering::Relaxed);
    }

    /// Get the current counter value.
    pub fn get(&self) -> u64 {
        self.copy.load(Ordering::Relaxed)
    }
}

/// Flushes the OpenTelemetry metrics provider and shuts it down. This ensures that all
/// metrics are sent before the application exits. Note that during normal operation, metrics
/// are sent periodically.
pub fn provider_flush_shutdown(
    provider: SdkMeterProvider,
) -> std::result::Result<(), opentelemetry_sdk::error::OTelSdkError> {
    provider.force_flush()?;
    provider.shutdown()
}
