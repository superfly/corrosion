# Changelog

## v1.0.0

v1.0.0 is packed with a lot of new features and bug fixes. Some high-level details include:

- **PostgreSQL compatible API:** Implement a PostgreSQL wire protocol (v3) compatible API, including read-only endpoints, tls configuration, and addition of unnest table-valued function (similar to `rarray`) that accepts postgres arrays into tables for bulk queries ([#83](https://github.com/superfly/corrosion/pull/83), [#337](https://github.com/superfly/corrosion/pull/337), [#405](https://github.com/superfly/corrosion/pull/383),[#174](https://github.com/superfly/corrosion/pull/174), [#283](https://github.com/superfly/corrosion/pull/283))
- **HTTP API:** Add `v1/updates/:table` endpoint for streaming lightweight primary key updates for a table, `v1/health` endpoint to query current health of corrosion node, and accept _all_ JSON types for SQLite params input ([#262](https://github.com/superfly/corrosion/pull/262), [#423](https://github.com/superfly/corrosion/pull/423))
- **State synchronization:** Parallel synchronization with many deadlock and bug fixes ([#78](https://github.com/superfly/corrosion/pull/78), [#201](https://github.com/superfly/corrosion/pull/201)), more efficient format for sending changes between nodes and prioritization of more recent changes ([#373](https://github.com/superfly/corrosion/pull/363)), burstable change processing ([#431](https://github.com/superfly/corrosion/pull/431)).
- **Bookkeeping & change processing:** Reduced locks around internal bookie structure ([#433](https://github.com/superfly/corrosion/pull/433)), and reduce info kept in-memory for bookkeeping ([#189](https://github.com/superfly/corrosion/pull/189)) and many fixes around partials, gaps, and buffered versions.
- **Rust client (`corro-client`):** typed query events and related API cleanups ([#126](https://github.com/superfly/corrosion/pull/126))
- **CLI**: `corrosion consul sync` will now bundle services and checks in a single transaction (changeset) ([#73](../../pull/73))
- **Schema migration:** Support existing tables being added to the schema ([#64](https://github.com/superfly/corrosion/pull/64))
- **Observability:** richer metrics and logging for various parts of Corrosion (including subscription and channel-capacity signals) ([#468](https://github.com/superfly/corrosion/pull/468), [#472](https://github.com/superfly/corrosion/pull/472)), and stronger Antithesis coverage ([#463](https://github.com/superfly/corrosion/pull/463))
- **Garbage Collection**: Add optional garage collection for tables that don't reuse primary keys ([418](https://github.com/superfly/corrosion/pull/418))

**Migrating from Corrosion v0.1.0 to v1.0.0**:

v1.0.0 introduces some breaking changes, notably the database schema has changed. The most significant database changes are:

- The clock tables (`<table>__crsql_clock`) have a new schema.
- The `__corro_bookkeeping` table has been removed.

Because of these schema differences, a running v0 cluster cannot be upgraded
in-place. Instead, the entire cluster must be re-bootstrapped from a single
snapshot. Please see [reseeding corrosion](doc/reseeding.md)

Thanks to everyone who made a contribution!

## v0.1.0

Initial release!
