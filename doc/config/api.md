# The [api] block

## api.addr

Address for the Corrosion HTTP API to listen on.

```toml
[api]
addr = "0.0.0.0:9000"
```

## api.authz.bearer-token

Bearer token that will be used to authenticate HTTP requests.
The client should set this token in the `Authorization` header.

```toml
[api]
authz.bearer-token = "<token>"
```

## api.pg.addr

Address to listen on for PostgresQL connections.
This allows you to query the sqlite databases using SQL.

```toml
[api]
pg.addr = ""
```