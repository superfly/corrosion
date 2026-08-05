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

#### `gossip.idle_timeout_secs`

The max idle timeout in seconds for QUIC connection.

Defaults to 30 seconds.

#### `gossip.member_id`

Specifies a member_id which identify nodes of the same Corrosion cluster. Nodes with different member_id would be unable to share changes with each other.

#### `gossip.max_mtu`

Define the max MTU for QUIC. Instead of attempting to discover the best MTU value automatically, you can define this upper bound.

This should be your "effective" MTU: `network interface's MTU - IP header size - UDP header size`. For example, if the MTU on your network interface is `1500` and you're listening on IPv6, you'll need to subtract `40` bytes for the IP header and `8` bytes for the UDP header (you'd set `max_mtu = 1452`).

#### `gossip.disable_gso`

Certain environments don't support GSO (Generic Segmentation Offload). This is detected by the QUIC implementation, but it's possible to pre-emptively disable it to avoid re-trying the initial packets without GSO as it is detected as unavailable.

#### `gossip.broadcast`

Selects the algorithm used to disseminate changes across the cluster. Please note that all nodes in a cluster need to use the same broadcast
algorithm.

Two methods are available:

- `gossip` (**default**): SWIM-style gossip. Each change is re-broadcast to a random subset of peers. Robust and simple, but every hop carries the full payload, so bandwidth grows with cluster size.
- `plumtree`: [Plumtree](https://asc.di.fct.unl.pt/~jleitao/pdf/srds07-leitao.pdf) epidemic broadcast. Nodes build a spanning tree of "eager" peers that forward full payloads, while the remaining "lazy" peers only exchange lightweight `IHave` announcements and pull (`GRAFT`) the payload on demand. This cuts redundant payload traffic on larger clusters, at the cost of some protocol complexity.

If unset, Corrosion uses `gossip`. To enable Plumtree, add a `[gossip.broadcast.plumtree]` table. All of its fields are optional and fall back to the defaults below:

```toml
[gossip.broadcast.plumtree]
prune_threshold = 5         # optional
optimization_threshold = 7  # optional
batch_gossip = false        # optional
```

##### `prune_threshold`

Number of times a message may be received from the same eager peer before that peer is pruned (moved from the eager set to the lazy set with a `PRUNE`). Duplicate deliveries mean the tree has a redundant edge, so pruning trims it.

Higher values tolerate more duplication in exchange for a more stable tree; lower values prune more aggressively, reducing redundant payload traffic but causing more tree churn. Defaults to `5`.

##### `optimization_threshold`

Threshold, in gossip rounds, for switching to a shorter path. When a node learns via a lazy `IHave` that a peer could have delivered a message at least this many rounds sooner than the eager peer that actually delivered it, it grafts the shorter path and prunes the longer one.

Lower values optimize the tree more aggressively (more `GRAFT`/`PRUNE` churn); higher values leave the tree alone unless the improvement is large. Defaults to `7`.

##### `batch_gossip`

When `true`, coalesce multiple outgoing Plumtree gossip and membership messages instead of sending each one individually. Reduces per-packet overhead on busy clusters at the cost of a small amount of added latency. Defaults to `false`.

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

plaintext = false           # optional
idle_timeout_secs = 30      # optional
disable_gso = false         # optional

# max_mtu = 1452            # optional; unset = autodetect, must be >= 1200
# external_addr = ""        # optional, defaults to gossip.addr
# client_addr   = "[::]:0"  # optional

member_id = 1 # optional

[gossip.tls] # optional
cert_file = "/path/to/server_cert.pem"
key_file = "/path/to/server_key.pem"
ca_file = "/path/to/ca_cert.pem" # optional
insecure = false # optional

[gossip.tls.client] # optional
cert_file = "/path/to/client_cert.pem"
key_file = "/path/to/client_key.pem"
```