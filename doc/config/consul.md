# The [consul] block

For the **`corrosion consul sync`** helper that uses this configuration, see [Consul CLI](../cli/consul.md).

## consul.client.addr

Local address of consul server.

## consul.client.tls

TLS configuration to use when communicating with Consul.

```toml
[consul.client.tls]
ca_file = ""
cert_file = ""
key_file = ""
```