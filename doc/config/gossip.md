# The `[gossip]` configuration

The `[gossip]` block configures the peer-to-peer API. Corrosion uses QUIC (UDP) to exchange information between nodes of a cluster.

### Required fields

#### `gossip.addr`

Socket address reachable from other nodes in the cluster. Listens on UDP for QUIC packets.

### Optional fields

#### `gossip.bootstrap`

Partial list of node addresses from the cluster for the initial join.

```toml
bootstrap = ["127.0.0.1:3333", "127.0.0.1:3334"]
```

It can resolve names (using the system's DNS resolver):

```toml
bootstrap = ["my-fly-app.internal:3333"]
```

It can resolve names w/ a custom DNS server:

```toml
bootstrap = ["my-fly-app.internal:3333@[fdaa::3]:53"]
```

## Example config (w/ default values)

```toml
[gossip]
addr = "" # required, no default value

bootstrap = []

plaintext = false  # optional
max_mtu = 1200  # optional
disable_gso = false  # optional

[gossip.tls] # optional
cert_file = "/path/to/server_cert.pem"
key_file = "/path/to/server_key.pem"
ca_file = "/path/to/ca_cert.pem" # optional
insecure = false # optional

[gossip.tls.client] # optional
cert_file = "/path/to/client_cert.pem"
key_file = "/path/to/client_key.pem"
```