# The `[gossip]` configuration

The `[gossip]` block configures the peer-to-peer API. Corrosion uses QUIC (UDP) to exchange information between nodes of a cluster.

### Required fields

#### `gossip.addr`

Socket address reachable from other nodes in the cluster. Listens on UDP for QUIC packets.

### Optional fields

#### `gossip.bootstrap`

List of node addresses from the cluster for the initial join. Defaults to an empty array.

It's recommended to use a partial list of nodes that overlap. The cluster discover nodes it doesn't know about automatically via SWIM.

Simple example:

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

#### `gossip.plaintext`

Allows using QUIC without encryption. The only reason to set this to `true` is if you're running a toy cluster or if the underlying transport is already handling cryptography (such as WireGuard) AND authorization is bound by the network (such is the case for a [Fly.io](https://fly.io) app's private network).

```admonish warning
It's highly recommended to use the `gossip.tls` configuration block to setup encryption and `gossip.tls.client` to setup authorization.
```

#### `gossip.max_mtu`

Define the max MTU for QUIC. Instead of attempting to discover the best MTU value automatically, you can define this upper bound.

This should be your "effective" MTU: `network interface's MTU - IP header size - UDP header size`. For example, if the MTU on your network interface is `1500` and you're listening on IPv6, you'll need to subtract `40` bytes for the IP header and `8` bytes for the UDP header (you'd set `max_mtu = 1452`).

#### `gossip.disable_gso`

Certain environments don't support GSO (Generic Segmentation Offload). This is detected by the QUIC implementation, but it's possible to pre-emptively disable it to avoid re-trying the initial packets without GSO as it is detected as unavailable.

#### `gossip.tls`

Strong encryption is highly recommended for any non-development usage of Corrosion.

You can easily generate the necessary certificates using [`corrosion tls`](../cli/tls.md).

Using `gossip.tls.insecure = true` means the certificate's signing authority won't be checked.

```toml
[gossip.tls] # optional
cert_file = "/path/to/server_cert.pem"
key_file = "/path/to/server_key.pem"
ca_file = "/path/to/ca_cert.pem" # optional
insecure = false # optional
```

It's also possible to specify client certification authorization (mutual TLS or mTLS):

```toml
[gossip.tls.client] # optional
cert_file = "/path/to/client_cert.pem"
key_file = "/path/to/client_key.pem"
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