# The `corrosion cluster` command

Inspect and manage cluster membership (SWIM / Foca) for the local Corrosion agent. 

## Subcommands

### `corrosion cluster rejoin`

Forces the node to leave and rejoin the gossip cluster with a **new identity (renewed Foca identity).

```bash
corrosion cluster rejoin
```

### `corrosion cluster members`

Output current members (id, state, and rtt information).

```bash
corrosion cluster members
```

### `corrosion cluster membership-states`

Output SWIM cluster membership states

```bash
corrosion cluster membership-states
```

### `corrosion cluster set-id <CLUSTER_ID>`

Sets a new `cluster_id` for this node, it command will persist the new cluster_id in the database (`__corro_state` table with key `cluster_id`). Remember, corrosion would reject any changes from a node with a different cluster-id.

```bash
corrosion cluster set-id 42
```
