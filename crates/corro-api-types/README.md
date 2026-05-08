# corro-api-types

Wire-format types shared between [Corrosion]'s HTTP API server and clients.

This crate is intentionally lightweight: it only contains types that need to
be (de)serialized across the API boundary, plus the `SqliteValue` /
`SqliteParam` helpers used to round-trip SQLite values through JSON and the
[Speedy] binary format. It is consumed by both
[`corro-client`](../corro-client) (the public Rust client) and the agent
itself.

## Highlights

- `TypedQueryEvent` / `QueryEvent` — frames returned by the `/v1/queries`
  and `/v1/subscriptions` streaming endpoints.
- `TypedNotifyEvent` / `NotifyEvent` — frames returned by the `/v1/updates`
  streaming endpoint.
- `ExecResponse` / `ExecResult` — responses from `/v1/transactions` and
  `/v1/migrations`.
- `SqliteValue` / `SqliteValueRef` / `SqliteParam` — typed wrappers around
  SQLite values for reading rows and binding parameters.
- `RowId`, `ChangeId`, `TableName`, `ColumnName` — strongly typed newtypes
  used throughout the API.
- `Statement` — the JSON-serialized form of a SQL statement plus its bound
  parameters.

Most types implement `Serialize`/`Deserialize` and (where appropriate)
`speedy::Readable`/`speedy::Writable` so they can be sent over the cluster's
binary protocol as well as the HTTP API.

## License

Licensed under the [Apache License, Version 2.0](../../LICENSE).

[Corrosion]: https://github.com/superfly/corrosion
[Speedy]: https://github.com/koute/speedy
