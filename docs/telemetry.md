### Telemetry

Amp has an OpenTelemetry setup to track various metrics and traces. For local testing, a Grafana telemetry stack
is already configured and can be run through `docker-compose`:

```
docker-compose up -d
```

This will (among other things) run the `grafana/otel-lgmt` image. More info about the image can be found [here](https://github.com/grafana/docker-otel-lgtm/).

#### Connecting to the telemetry stack

In order to connect to the telemetry stack when running Amp, you need to add the following to your config file:

```toml
[opentelemetry]
trace_url = "http://localhost:4317/v1/traces"
metrics_url = "http://localhost:4318/v1/metrics"
# Optional: Compression method for OpenTelemetry data export
# Supported values: "none", "gzip" (default: "none")
compression = "none"
```

#### Compression Configuration

The `compression` field controls how OpenTelemetry data is compressed before transmission:

- `"none"` (default): Disables compression (useful for debugging and development)
- `"gzip"`: Enables gzip compression for optimal performance and reduced bandwidth usage

You can also control compression via environment variables:
- `AMP_CONFIG_OPENTELEMETRY_COMPRESSION`: Override the compression setting
- `OTLP_TRACES_COMPRESSION`: Control compression for traces only
- `OTLP_METRICS_COMPRESSION`: Control compression for metrics only
- `OTEL_EXPORTER_OTLP_COMPRESSION`: Standard OpenTelemetry environment variable (affects all signals)

where `4317` and `4318` are the default gRPC and HTTP (respectively) ports for the `grafana/otel-lgmt` image (make sure
that your `dockere-compose` setup is using the same ports).

#### Viewing the telemetry data

To view the telemetry data, open your browser and go to [http://localhost:3000](http://localhost:3000) which is the default Grafana port.

#### Custom dashboards

It is possible to load custom dashboards in Grafana for an easier overview of the metrics and traces. To do this,
go to [http://localhost:3000/dashboards/import](http://localhost:3000/dashboards/import) and upload the dashboard JSON file or copy its contents.

You can find some pre-configured dashboards in the `grafana/dashboards` folder of this repository. Any new dashboards
must also be placed in this directory, as per the current setup in our [docker-compose.yaml](../docker-compose.yaml)
file.

If you've created/modified a Grafana dashboard and you wish to save it, click on the `Export` button in the Grafana
dashboard UI and save the dashboard as a JSON file in `grafana/dashboards/`.
