# The [telemetry] configuration

The telemetry block is optional. This block configures open telemetry and prometheus.

## Optional Fields

### telemetry.prometheus.addr

Address for the prometheus exporter binds to. `GET` requests to this address will return prometheus metrics.

```toml
[telemetry]
prometheus.addr = "0.0.0.0:9090"
```

You can read more about the Prometheus metrics that corrosion exposes [here](../telemetry/prometheus.md).

### telemetry.open-telemetry

This block configures how the open telemetry exporter.

Configure open telemetry exporter using environment variables. The environment variables and their default are listed [here](https://opentelemetry.io/docs/specs/otel/protocol/exporter/).

```toml
[telemetry]
open-telemetry = "from_env"
```

### telemetry.open-telemetry.exporter

Address that open telemetry exporter should send traces to.

```toml
[telemetry]
open-telemetry.exporter = { endpoint = "10.0.0.0:9999"}
```