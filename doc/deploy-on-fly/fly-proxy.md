# Run Corrosion commands on a remote node

Corrosion's CLI works through the API. You can install Corrosion locally and run Corrosion commands on a remote node.

## Connecting

A convenient way to access a remote node's API port on Fly.io is to open a user-mode WireGuard tunnel using the [`fly proxy`](https://fly.io/docs/flyctl/proxy/) command.

In the [example configuration](./launch.md#corrosion-configuration), Corrosion's API is configured on port 8080, so with Corrosion installed on the local machine, in a separate terminal, run:

```
$ fly proxy 8080 --app <your-app-name>
```

to proxy your local port 8080 to port 8080 on a node belonging to your Corrosion app on Fly.io.

As with the `fly ssh console` command, if you run this command from the directory containing your Corrosion app's `fly.toml` configuration file, you can omit the `--app` flag.


## Running commands

You don't need a local Corrosion configuration file if you're only using Corrosion to interface with a remote node. You do need to pass an API address to CLI commands. Here's an example using the `query` subcommand.

```
$ ./corrosion  query --timer --columns "SELECT * FROM todos" --api-addr "127.0.0.1:8080"
id|title|completed_at
some-id|Write some Corrosion docs!|1234567890
some-id-2|Show how broadcasts work|1234567890
some-id-3|Hello from a template!|1234567890
time: 0.000000249s
```