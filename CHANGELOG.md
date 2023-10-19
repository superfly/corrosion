# Changelog

## Unreleased

- Parallel synchronization w/ many deadlock and bug fixes ([#78](../../pull/78))
- Upgraded to cr-sqlite 0.16.0 (unreleased) ([#75](../../pull/75))
- Rewrite compaction logic to be more correct and efficient ([#74](../../pull/74))
- `corrosion consul sync` will now bundle services and checks in a single transaction (changeset) ([#73](../../pull/73))
- (**BREAKING**) Persist subscriptions across reboots, including many reliability improvements ([#69](../../pull/69))
- Support existing tables being added to the schema ([#64](../../pull/64))

## v0.1.0

Initial release!