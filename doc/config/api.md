# The [api] block

The `[api]` block configures the local Corrosion HTTP API and, optionally, a PostgreSQL wire-protocol listener.

## Required fields

### `api.addr`

Address for the Corrosion HTTP API to listen on. Accepts either a single socket address or an array of addresses if you want to listen on multiple interfaces.

`addr` is an alias for `bind_addr`; either name works in the config file.

```toml
[api]
addr = "0.0.0.0:9000"
```

## api.authz.bearer-token

### Optional fields

#### `api.endpoint_name`

This is a label used to identify nodes in the same cluster, used mostly to ensure requests aren't processed
by the wrong one. An incoming request with a different label in the `x-corrosion-requested-endpoint-name` header is rejected.

```toml
[api]
endpoint_name = "corrosion-iad-1"
```

#### `api.authz.bearer-token`

Bearer token used to authenticate HTTP requests. Clients must set this token in the `Authorization` header (`Authorization: Bearer <token>`).

```toml
[api]
authz.bearer-token = "<token>"
```

## PostgreSQL wire protocol

Corrosion can additionally expose its database over the [PostgreSQL wire protocol](../api/pg.md) for ad-hoc SQL access. The `pg` field accepts either a single listener config or an array of listener configs.

#### `api.pg.addr`

Address to listen on for PostgreSQL connections.

```toml
[api]
pg.addr = "127.0.0.1:5470"
```

Multiple listeners (e.g. one read-write, one read-only):

```toml
[[api.pg]]
addr = "127.0.0.1:5470"

[[api.pg]]
addr = "127.0.0.1:5471"
readonly = true
```

#### `api.pg.readonly`

When `true`, the listener rejects statements that would mutate the database. Defaults to `false`.

```toml
[api.pg]
addr     = "127.0.0.1:5471"
readonly = true
```

#### `api.pg.tls`

Enable TLS for incoming PostgreSQL connections.

```toml
[api.pg]
addr = "0.0.0.0:5470"

[api.pg.tls]
cert_file     = "/path/to/server_cert.pem"
key_file      = "/path/to/server_key.pem"
ca_file       = "/path/to/ca_cert.pem"   # optional
verify_client = false                    # optional, set true to require client certs
```

When `verify_client = true`, only clients presenting a certificate signed by `ca_file` will be accepted (mutual TLS).