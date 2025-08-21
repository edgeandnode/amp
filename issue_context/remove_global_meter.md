# Plan to Remove Global METER

## Problem
Currently, all metrics use `opentelemetry::global::meter(NOZZLE_METER)` which creates metrics tied to a global meter. This makes testing difficult as tests can't have isolated meter instances.

## Solution - Complete Removal of Global Meter

### 1. Modify metric creation methods in `monitoring/src/telemetry/metrics.rs`
- Add a required `Meter` parameter to all metric creation methods:
  - `Gauge::new_u64(meter: Meter, name, description, unit)`
  - `Gauge::new_f64(meter: Meter, name, description, unit)`
  - `Counter::new(meter: Meter, name, description)`
  - `Histogram::new_u64(meter: Meter, name, description, unit)`
  - `Histogram::new_f64(meter: Meter, name, description, unit)`
  - `ReadableCounter::new(meter: Meter, name, description)`
- Remove all calls to `opentelemetry::global::meter(NOZZLE_METER)`
- Remove the `NOZZLE_METER` constant

### 2. Update the `start()` function
- Continue returning `SdkMeterProvider`
- Add a helper method to get a meter from the provider

### 3. Update metric registries to require a meter
- `dump/src/metrics.rs`: 
  - Change `MetricsRegistry::new()` to `MetricsRegistry::new(meter: Meter)`
  - Update the static `METRICS: LazyLock` to use a function that takes a meter
- `dump-check/src/metrics.rs`: 
  - Change the static initialization to use a function that takes a meter
  - Consider changing from static to instance-based metrics

### 4. Update initialization in main binaries
- `nozzle/src/main.rs`: 
  - Create a meter from the provider using `provider.meter("nozzle")`
  - Pass it to metric registries during initialization
- `dump-check/src/main.rs`: 
  - Create a meter from the provider
  - Initialize metrics with it

## Implementation Steps

1. **Update metric constructors** in `monitoring/src/telemetry/metrics.rs`
   - Add `meter: opentelemetry::metrics::Meter` as first parameter to all `new` methods
   - Replace `opentelemetry::global::meter(NOZZLE_METER)` with the passed meter parameter
   - Remove `NOZZLE_METER` constant

2. **Update MetricsRegistry in dump**
   - Change `MetricsRegistry::new()` to accept a meter parameter
   - Pass the meter to all Counter/Gauge/Histogram constructors
   - Update the static initialization to handle the meter requirement

3. **Update MetricsRegistry in dump-check**
   - Change from static `LazyLock` to a function-based initialization
   - Or create an init function that sets up the static with a provided meter

4. **Update main binaries**
   - After calling `telemetry::metrics::start()`, get a meter from the returned provider
   - Pass this meter to all metric registry initializations

5. **Testing benefits**
   - Tests can now create their own `SdkMeterProvider` instances
   - Each test can have isolated metrics without global state interference
   - Metrics can be observed and verified in tests

## Example Code Changes

### Before (in metrics.rs):
```rust
pub fn new_u64(name: impl Into<Cow<'static, str>>, ...) -> Self {
    let inner = opentelemetry::global::meter(NOZZLE_METER)
        .u64_gauge(name)
        ...
}
```

### After:
```rust
pub fn new_u64(
    meter: opentelemetry::metrics::Meter,
    name: impl Into<Cow<'static, str>>, 
    ...
) -> Self {
    let inner = meter.u64_gauge(name)
        ...
}
```

### Usage in main:
```rust
let provider = telemetry::metrics::start(url, interval)?;
let meter = provider.meter("nozzle");
// Pass meter to metric registries
```

This approach completely removes the global meter dependency, making metrics fully testable with isolated instances.