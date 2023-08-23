# Corrosion

**corrosion** is a distributed system for propagating SQLite state across a cluster of nodes.

**Features**:
- Just SQLite
- Multi-writer via CRDTs (uses the [cr-sqlite](https://github.com/vlcn-io/cr-sqlite) extension)
- Eventually consistent
- RESTful HTTP API
- Subscribe to SQL queries
- SWIM cluster formation
- Fast, gossip-based, updates dissimination
