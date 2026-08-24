# The `[perf]` configuration

The `[perf]` block exposes internal tuning knobs for the agent: in-process channel sizes, the WAL truncation threshold, sync backoff bounds, and the change-apply batching strategy.

This is advanced configuration. Most operators don't need to touch any of it — the defaults are tuned for typical Corrosion clusters. Only override these values if you have a specific symptom (e.g. dropped changes under heavy load, runaway WAL growth, slow sync convergence) and a clear hypothesis about which knob will help.

The whole block is optional; omitting it (or any field within it) keeps the default values shown below.

## Channel buffer sizes

Corrosion uses bounded in-process channels between its background tasks. These knobs set those channel capacities. Larger channels can absorb bigger bursts before back-pressure kicks in, at the cost of memory; smaller channels are stricter about back-pressure.

If a channel fills up, the affected sender will either block (slowing the producer) or, in a few hot paths, drop the message and log an error. The Prometheus gauge `corro_runtime_channel_capacity{channel_name="<name>"}` reports the remaining headroom for each channel.

| Field | Default | Channel name (in metrics) | What it carries |
| --- | --- | --- | --- |
| `apply_channel_len` | `2048` | `apply` | Fully-buffered versions that should to be processed. |
| `changes_channel_len` | `1024` | `changes` | Inbound changes received from peers (broadcasts and syncs) waiting to be processed. |
| `bcast_channel_len` | `512` | `bcast` | Local writes waiting to be broadcast out to peers. |
| `clearbuf_channel_len` | `512` | `clear_buf` | Fully-processed buffered version ranges that should be cleaned up. |
| `to_send_channel_len` | `512` | `to_send` | Outgoing broadcast packets queued for the transport layer. |
| `notifications_channel_len` | `512` | `notifications` | SWIM membership notifications (member up / down). |
| `schedule_channel_len` | `512` | `to_schedule` | Foca timer schedule events. |
| `foca_channel_len` | `256` | `foca` | Inbound Foca/SWIM protocol events. |

```toml
[perf]
apply_channel_len         = 2048
changes_channel_len       = 1024
bcast_channel_len         = 512
clearbuf_channel_len      = 512
to_send_channel_len       = 512
notifications_channel_len = 512
schedule_channel_len      = 512
foca_channel_len          = 256
```

## Apply queue (change batching)

Inbound changes are not written to the local database one at a time — they are accumulated and applied in batches inside a single SQLite transaction. These knobs control that batching.

Roughly, the agent waits up to `apply_queue_timeout` or at least `apply_queue_min_batch_size` changes to accumulate, then applies the batch. Under sustained load the batch size grows geometrically (controlled by `apply_queue_step_base`) up to `apply_queue_max_batch_size`. When the buffered queue passes `apply_queue_batch_threshold_ratio` of the current target batch size, a batch is spawned immediately rather than waiting for the timer.

#### `processing_queue_len`

Maximum number of unapplied changesets the agent will buffer before it starts dropping old entries. Defaults to `20000`. A small number of dropped changes isn't worrisome, corrosion will request them through sync. Before
tuning this, check first that changes are being processed quickly enough.

```toml
[perf]
processing_queue_len = 20000
```

#### `plumtree_send_queue_len`

Maximum number of outgoing Plumtree messages the agent will buffer before shedding one per intake. Defaults to `40000`. Only used when `gossip.broadcast.method` is `plumtree`.

This is deliberately separate from `processing_queue_len`, which also sizes the Plumtree payload and seen caches — widening the send queue should not force those caches to grow with it.

Each buffered entry holds one encoded message plus its target peer list, so memory scales with queue length times message size (change gossip is capped at 8 KiB per chunk). Raise it if `corro_plumtree_send_queue_len` sits near the cap and `corro_plumtree_send_dropped` is non-zero; lower it if the agent is memory-constrained.

The agent keeps two send queues, split by locality. The *local* queue holds round-0 gossip, which is gossip this node originated — no other node holds a copy of it. The *remote* queue holds everything else: forwarded gossip, `IHAVE`, `GRAFT` and `PRUNE`. The local queue is sent first, and is only shed from once the remote queue is empty.

When the queue is over capacity the agent sheds the message that is cheapest to lose, rather than the most recently queued one:

1. `PRUNE` always, regardless of round — it is pure tree optimization and the next duplicate regenerates it.
2. Otherwise by **descending round**. A high round means the message has already travelled many hops, so more peers hold a copy and a node that misses it can recover by grafting a lazy peer that announced it.
3. At equal round, `IHAVE` first (a node has up to `max_lazy` peers announcing the same id, so any one announcement is redundant), then `GRAFT`, then gossip last.
4. Ties are broken toward the oldest entry, which has waited longest and is least likely to still be useful.

Round is a proxy for how widely a message has spread, not a measurement of it — a message that travelled a long thin chain has a high round but few holders. The locality split, not the round ordering, is what guarantees a node's own writes survive shedding.

```toml
[perf]
plumtree_send_queue_len = 40000
```

#### `apply_queue_timeout`

Maximum time, in **milliseconds**, the agent will wait for more changes to accumulate before applying whatever it has. Defaults to `10` ms.

Lower values reduce write latency at the cost of more, smaller transactions. Higher values improve write throughput by amortizing transaction overhead.

#### `apply_queue_min_batch_size`

Minimum number of changes the agent will try to apply per transaction. Defaults to `100`.

#### `apply_queue_max_batch_size`

Maximum number of changes the agent will apply in a single transaction. Defaults to `16000`.

#### `apply_queue_step_base`

Base used by the geometric batch-size selector. Effective batch size is approximately:

```
batch_size = clamp(
    apply_queue_min_batch_size,
    apply_queue_step_base * 2 ** floor(log2(queue_len / apply_queue_step_base)),
    apply_queue_max_batch_size,
)
```

Defaults to `500`. Larger values make the batch size grow faster as the queue fills.

#### `apply_queue_batch_threshold_ratio`

Fraction (`0.0`–`1.0`) of the current target batch size at which a batch is spawned immediately, without waiting for `apply_queue_timeout`. Defaults to `0.9`.

Raise it (closer to `1.0`) to favor larger batches; lower it to favor lower latency.

```toml
[perf]
apply_queue_timeout                 = 10      # milliseconds
apply_queue_min_batch_size          = 100
apply_queue_max_batch_size          = 16000
apply_queue_step_base               = 500
apply_queue_batch_threshold_ratio   = 0.9
```

## Synchronization

#### `min_sync_backoff` / `max_sync_backoff`

Lower and upper bound, in **seconds**, of the randomized backoff between rounds of sync with other peers. Defaults: `min_sync_backoff = 1`, `max_sync_backoff = 15`.

A larger range reduces sync chatter on healthy clusters; a smaller range converges faster but produces more cross-node traffic.

```toml
[perf]
min_sync_backoff = 1
max_sync_backoff = 15
```

## Database

#### `wal_threshold_mb`

Size in **megabytes** above which the agent will attempt to checkpoint and truncate the SQLite WAL. Defaults to `5120` (5 GiB).

```toml
[perf]
wal_threshold_mb = 5120
```

#### `sql_tx_timeout`

Maximum duration, in **seconds**, that internal SQL transactions (sync ingestion, change apply, etc.) are allowed to run. Defaults to `60`. Raising this is useful when applying very large batches on slow storage;

```toml
[perf]
sql_tx_timeout = 60
```
