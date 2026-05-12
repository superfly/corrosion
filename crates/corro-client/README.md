# corro-client

Async Rust client for the [Corrosion] HTTP API.

This crate is the recommended way to talk to a running Corrosion agent from
Rust. It speaks the JSON over HTTP/2 protocol exposed at `/v1/queries`,
`/v1/subscriptions`, `/v1/updates`, `/v1/transactions` and `/v1/migrations`,
and re-exports the wire-level types from
[`corro-api-types`](../corro-api-types).

## Clients

Three client types are provided:

- **`CorrosionApiClient`** — the lowest-level handle, wrapping a single
  `reqwest::Client` pointed at one `SocketAddr`.
- **`CorrosionClient`** — adds a local `sqlite_pool::RusqlitePool` for
  reading directly from the on-disk SQLite database without going through
  the HTTP API. Dereferences to `CorrosionApiClient`.
- **`CorrosionPooledClient`** — accepts a list of agent addresses, resolves
  them through DNS and retries failed requests against the next available
  peer with configurable stickiness.

All read endpoints return [`futures::Stream`]s (`QueryStream`,
`SubscriptionStream`, `UpdatesStream`) that yield
`corro_api_types::TypedQueryEvent` / `corro_api_types::TypedNotifyEvent`
frames. Subscriptions automatically reconnect with the last seen `ChangeId`
when the underlying HTTP connection drops, so most callers can simply
iterate the stream and ignore transient network failures.

## Example

```rust
use corro_client::CorrosionApiClient;
use corro_api_types::Statement;
use futures::StreamExt;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = CorrosionApiClient::new("127.0.0.1:8081".parse()?)?;

let mut rows = client
    .query(&Statement::Simple("SELECT id, name FROM users".into()), None)
    .await?;

while let Some(event) = rows.next().await {
    println!("{:?}", event?);
}
# Ok(()) }
```

## License

Licensed under the [Apache License, Version 2.0](../../LICENSE).

[Corrosion]: https://github.com/superfly/corrosion
[`futures::Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
