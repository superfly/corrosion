# Corrosion
Corrosion: Gossip-based service discovery (and more) for large distributed systems, written in Rust.

## Why we built Corrosion

We built Corrosion specifically for service discovery across a large global network, replacing Consul’s central state database with eventually consistent state distributed across our hosts.

Our new tool needed to deliver the following:

### Fast reads and writes

Getting state (data) from a central remote source can be incredibly expensive (at least 300ms for a round-trip to something on the other side of the world), but usually takes less than 1ms from a local source.

### Fast consistency

Strong consistency by RAFT consensus can be too slow. Eventual consistency works, if it's fast.

### Flexibility

Global state for a distributed system isn't one-size-fits-all. Flexible schemas and queries are essential.

## How Corrosion works

In a nutshell, Corrosion:

- Hosts a SQLite database on each node, for fast local reads and writes
- Uses [CR-SQLite](https://github.com/vlcn-io/cr-sqlite) for conflict resolution with CRDTs
- Uses [Foca](https://github.com/caio/foca) to manage cluster membership using a SWIM protocol
- Gossips changes from the local database throughout the cluster
- Periodically synchronizes with a subset of other cluster nodes, to ensure consistency

## Features

- A flexible API to read from and write to Corrosion's store using SQL statements
- File-based schemas with on-the-fly updates
- HTTP streaming subscriptions based on SQL queries
- Live population of configuration files from Corrosion state with user-defined [Rhai](https://rhai.rs/) templates
- Storage and propagation of state from locally registered Consul services, replacing the central database with Corrosion's distributed state
- Vanilla SQLite storage for host-local data
- Secure peer-to-peer communication with the [QUIC](https://datatracker.ietf.org/doc/html/rfc9000) transport protocol (using [Quinn](https://github.com/quinn-rs/quinn))

## Usage overview

Run the Corrosion agent on every node/host in the cluster. Other programs running on the node use [Corrosion's HTTP API](/doc/api/README.md) to query the local Corrosion SQLite database, add and update data, and subscribe to change notifications.

The [Corrosion CLI](/doc/cli/README.md) provides commands for administration and access to database and features.

### Quick start

- [Prepare the Corrosion configuration file](config/README.md)
- [Specify the initial database schema](schema.md)
- [Start the Corrosion agent](cli/agent.md)

See the WIP [Corrosion documentation](/doc/SUMMARY.md) for more details.

## Building Corrosion

Clone [https://github.com/superfly/corrosion2.git](https://github.com/superfly/corrosion2.git).

From within the repo directory:

```
cargo build --release && mv target/release/corrosion ./
```
