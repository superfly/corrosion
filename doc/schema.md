# Schema

Corrosion's schema definition happens via files each representing one or more tables, written in SQL (SQLite-flavored). This is done through `CREATE TABLE` and `CREATE INDEX` exclusively!

Manual migrations are not supported (yet). When schema files change, Corrosion can be reloaded (or restarted) and it will compute a diff between the old and new schema and make the changes.

Any destructive actions on the table schemas are ignored / prohibited. This includes removing a table definition entirely or removing a column from a table. Indexes can be removed or added.

## Constraints

- Only `CREATE TABLE` and `CREATE INDEX` are allowed
- No unique indexes allowed (except for the default primary key unique index that does not need to be created)
- The primary key must be non nullable
- Non-nullable columns require a default value
  - This is a cr-sqlite constraint, but in practice w/ Corrosion: it does not matter. Entire changes will be applied all at once and no fields will be missing.
  - If table schemas are modified, then a default value is definitely required.

## Example

```sql
-- /etc/corrosion/schema/apps.sql

CREATE TABLE apps (
    id INT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL DEFAULT "",
    user_id INT NOT NULL DEFAULT 0
);

CREATE INDEX apps_user_id ON apps (user_id);
```
