# cr-sqlite

What to figure out:
- ~~gossip format~~
- ~~sync format~~
- backup + restore
    - mostly figured out in: https://github.com/vlcn-io/cr-sqlite/issues/145
- soft deletes
- ~~filters + subscriptions~~
- ~~schema versioning (hash of sqlite_master for each table?)~~ (probably not needed anymore with stricter constraints with the schema)
- ~~corroctl~~
- ~~dynamic reload of schema~~