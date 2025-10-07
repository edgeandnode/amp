use monitoring::telemetry;

#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    /// Total number of queries executed
    pub query_count: telemetry::metrics::Counter,

    /// Query execution duration in milliseconds
    pub query_duration: telemetry::metrics::Histogram<f64>,

    /// Total number of rows returned by queries
    pub query_rows_returned: telemetry::metrics::Counter,

    /// Total bytes sent to clients
    pub query_bytes_egress: telemetry::metrics::Counter,

    /// Total number of query errors
    pub query_errors: telemetry::metrics::Counter,

    /// Total number of streaming queries started
    pub streaming_queries_started: telemetry::metrics::Counter,

    /// Total number of streaming queries completed
    pub streaming_queries_completed: telemetry::metrics::Counter,

    /// Number of currently active streaming queries
    pub streaming_queries_active: telemetry::metrics::UpDownCounter,

    /// Duration that streaming queries remain active in milliseconds
    pub streaming_query_lifetime: telemetry::metrics::Histogram<f64>,

    /// Number of rows per streaming microbatch
    pub streaming_microbatch_size: telemetry::metrics::Histogram<u64>,

    /// Total rows sent incrementally via streaming queries
    pub streaming_rows_sent: telemetry::metrics::Counter,

    /// Total bytes sent incrementally via streaming queries
    pub streaming_bytes_sent: telemetry::metrics::Counter,
}

impl MetricsRegistry {
    pub fn new(meter: &telemetry::metrics::Meter) -> Self {
        Self {
            query_count: telemetry::metrics::Counter::new(
                meter,
                "query_count_total",
                "Total number of queries executed against datasets",
            ),
            query_duration: telemetry::metrics::Histogram::new_f64(
                meter,
                "query_duration_milliseconds",
                "Query execution duration (planning + execution)",
                "milliseconds",
            ),
            query_rows_returned: telemetry::metrics::Counter::new(
                meter,
                "query_rows_returned_total",
                "Total number of rows returned by queries",
            ),
            query_bytes_egress: telemetry::metrics::Counter::new(
                meter,
                "query_bytes_egress_total",
                "Total bytes sent to clients (network egress)",
            ),
            query_errors: telemetry::metrics::Counter::new(
                meter,
                "query_errors_total",
                "Total number of query errors",
            ),
            streaming_queries_started: telemetry::metrics::Counter::new(
                meter,
                "streaming_queries_started_total",
                "Total number of streaming queries started",
            ),
            streaming_queries_completed: telemetry::metrics::Counter::new(
                meter,
                "streaming_queries_completed_total",
                "Total number of streaming queries completed (successfully or via error/disconnect)",
            ),
            streaming_queries_active: telemetry::metrics::UpDownCounter::new(
                meter,
                "streaming_queries_active",
                "Number of currently active streaming queries in this process",
                "queries",
            ),
            streaming_query_lifetime: telemetry::metrics::Histogram::new_f64(
                meter,
                "streaming_query_lifetime_milliseconds",
                "Duration that streaming queries remain active before completion",
                "milliseconds",
            ),
            streaming_microbatch_size: telemetry::metrics::Histogram::new_u64(
                meter,
                "streaming_microbatch_size_rows",
                "Number of rows per streaming microbatch",
                "rows",
            ),
            streaming_rows_sent: telemetry::metrics::Counter::new(
                meter,
                "streaming_rows_sent_total",
                "Total rows sent incrementally via streaming queries",
            ),
            streaming_bytes_sent: telemetry::metrics::Counter::new(
                meter,
                "streaming_bytes_sent_total",
                "Total bytes sent incrementally via streaming queries",
            ),
        }
    }

    /// Record query execution metrics
    pub fn record_query_execution(
        &self,
        duration_millis: f64,
        rows_returned: u64,
        bytes_egress: u64,
    ) {
        self.query_count.inc();
        self.query_duration.record(duration_millis);
        self.query_rows_returned.inc_by(rows_returned);
        self.query_bytes_egress.inc_by(bytes_egress);
    }

    /// Record query error
    pub fn record_query_error(&self, error_code: &str) {
        let labels = [telemetry::metrics::KeyValue::new(
            "error_code",
            error_code.to_string(),
        )];

        self.query_errors.inc_with_kvs(&labels);
    }

    /// Record streaming microbatch size and throughput
    pub fn record_streaming_batch(&self, batch_rows: u64, batch_bytes: u64) {
        self.streaming_microbatch_size.record(batch_rows);
        self.streaming_rows_sent.inc_by(batch_rows);
        self.streaming_bytes_sent.inc_by(batch_bytes);
    }

    /// Record streaming query lifetime
    pub fn record_streaming_lifetime(&self, duration_millis: f64) {
        self.streaming_query_lifetime.record(duration_millis);
    }
}
