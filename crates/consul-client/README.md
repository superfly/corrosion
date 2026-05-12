# consul-client

Minimal HTTP client for the local [HashiCorp Consul] agent.

[Corrosion] uses this crate to read the list of services and health checks
registered against the Consul agent running on the same host, then propagates
that state through the cluster. The client only implements the handful of
agent endpoints required for that purpose (`agent_services` and
`agent_checks`); it is not a general-purpose Consul SDK. If you just want to 
sync consul checks and services to Corrosion, use `corrosion consul sync` command.

The client supports both plain HTTP and mutual TLS, configured via `Config`
and `config::TlsConfig`. When TLS is enabled the CA bundle, client
certificate, and private key are loaded from disk at construction time.

## Example

```rust
use consul_client::{Client, Config};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::new(Config {
    address: "127.0.0.1:8500".to_string(),
    tls: None,
})?;

let services = client.agent_services().await?;
for (id, svc) in services {
    println!("{id}: {}@{}:{}", svc.name, svc.address, svc.port);
}
# Ok(()) }
```

## License

Licensed under the [Apache License, Version 2.0](../../LICENSE).

[HashiCorp Consul]: https://developer.hashicorp.com/consul
[Corrosion]: https://github.com/superfly/corrosion
