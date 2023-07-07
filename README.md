# Corrosion

## Launching on Fly

```
fly launch --no-deploy --org <org> --region <region> --copy-config --name <app-name>
fly volumes create corro_data -y -s 10 -r <region> -a <app-name>
fly deploy --dockerfile examples/fly/Dockerfile -a <app-name> -c examples/fly/fly.toml
fly machine clone -a <app-name> --region <other region> --select
fly machine clone -a <app-name> --region <yet another region> --select
```

## Reading data

The readable API surface for Corrosion-produced data is to use SQLite and read directly from the database.

Adding indexes either through the API or schema files is the way to go to improve read performance on application-specific queries.

## Writing data

The only way to write data that propagates through the Corrosion cluster is to go through its HTTP API.

Corrosion's HTTP API is rqlite-compatible. This is both meant as a shortcut and to have ready-made clients for it.

### POST /v1/transactions

See: https://rqlite.io/docs/api/api/#writing-data

Corrosion-specific limitations:
- All requests are treated as if the `transaction` query param had been passed (everything runs in a transaction)
- There's a limit to how many statements can be input at once to reduce changeset sizes (this shouldn't be the case for long)

## Updating / migrating / changing the schema

Corrosion migrates schema changes automatically without the need for manual migration files. Each schema SQL file is expected to contain SQLite-compatible statements that create resources (e.g. `CREATE TABLE` and `CREATE INDEX`). `ALTER` or `DROP` statements are not supported. Destructive actions are ignored.

### File-based schemas

Configuring Corrosion with `schema_paths` lets you define filesystem directories where `.sql` files can be found. These files should, as a whole, represent the schema for your data.

You can add or modify schema files in known paths or even modify the `schema_paths` and then send a SIGHUP signal to Corrosion to apply schema changes, without a restart.

### POST /db/schema

This endpoint accepts the same type of body as the `/v1/transactions` endpoint, except it will mutate Corrosion's schema.

## Subscribing to changes

One of Corrosion's most powerful features is the ability to subscribe to changes as they are applied to the local node.

This works via websockets for maximum compatibility.

## Migrating data

(WIP)