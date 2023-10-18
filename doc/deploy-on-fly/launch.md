# Launch on Fly.io

This example deploys a 2-node Corrosion cluster on [Fly Machines](https://fly.io/docs/machines/) VMs, using the example files included in the Corrosion repository. 

You'll be provisioning two `shared-cpu-1x` Machines and two 1GB [Fly Volumes](https://fly.io/docs/reference/volumes/) storage volumes. See the [Fly.io resource pricing](https://fly.io/pricing/) page for cost information.

## Speedrun

```bash
$ cp examples/fly/fly.toml .
$ fly launch --dockerfile examples/fly/Dockerfile
$ fly scale count 1 --region <second-fly-region>
```

## Longer version

### Prep

Clone the corrosion repository. 

Check out the stable release branch.

```bash
$ git checkout tags/v0.1.0 -b v010
```

From the repo root directory:

Copy the example `fly.toml` to the working directory.

```bash
$ cp examples/fly/fly.toml .
```

### Launch a new app

Launch a new app on Fly.io, using the example Dockerfile.

```bash
$ fly launch --dockerfile examples/fly/Dockerfile
```

Confirm that you would like to copy the configuration from this file to the new app. You don't need any other databases for this exercise.
Confirm that you want to deploy now. 

Fly Launch will build the Docker image, create a storage volume, and deploy your new Corrosion app on a single Fly Machine.

Once deployment is complete, you can check that all is well with `fly status` and `fly logs`. 

### Add a second node

Scale up to two Machines. Put the second one in [another part of the world](https://fly.io/docs/reference/regions/) if you like:

```bash
$ fly scale count 1 --region <second-fly-region>
```

`fly scale count` provisions a new Machine with an empty volume attached. Once the new node joins the cluster, Corrosion populates its local database on this volume with the latest data from the cluster.

Once the second Machine is running, you should be able to see log messages from Corrosion on both instances.


### Check on the database

To get a shell session on a Fly Machine, from any directory: 

```bash
$ fly ssh console --pty --app <your-app-name> --select
```

On each node, Corrosion's local database is at `/var/lib/corrosion/state.db`. At this point it contains no data, but the `todos` table has been created according to the schema file `/etc/corrosion/schemas/todo.sql`.

```bash
# sqlite3 /var/lib/corrosion/state.db '.mode column' 'PRAGMA table_info(todos);'
cid  name          type     notnull  dflt_value  pk
---  ------------  -------  -------  ----------  --
0    id            BLOB     0                    1 
1    title         TEXT     1        ''          0 
2    completed_at  INTEGER  0                    0 
```

You can use this example database to test out your Corrosion cluster: [Example database](./demo.md).

## Example files for Fly.io deployment

Example files are in `corrosion/examples/fly/`.

### Dockerfile

The example Dockerfile `corrosion/examples/fly/Dockerfile` creates a Debian-based Docker image with a Corrosion binary built from a local copy of the source repository. It uses `entrypoint.sh`, `config.toml`, and `schemas/todo.sql` to configure and run Corrosion with an empty example database.

SQLite3 and [not-perf](https://github.com/koute/not-perf) are installed in this image. 

### Fly Launch configuration file

Fly Launch uses a TOML file for app configuration.

```toml
app = "corrosion"

[env]
RUST_BACKTRACE="1"
# RUST_LOG="info,foca=debug"

[mounts]
source = "corro_data"
destination = "/var/lib/corrosion"

[metrics]
port = 9090
path = "/"
```

The `mounts` section tells Fly Launch that this app needs a storage volume named `"corro_data"` and that it should be mounted at `/var/lib/corrosion` in the Machine's file system. A Fly Volume of this name will be created for the first Machine on the first deployment.

No public services are configured for the Corrosion cluster, because nodes communicate over private networking.


### Corrosion configuration

The example `config.toml` omits the `gossip.addr` and `gossip.bootstrap` entries. On startup. the `entrypoint.sh` script fills them in using `FLY_PRIVATE_IP` and `FLY_APP_NAME` environment variables that are available within the runtime environment.

The complete configuration file looks something like this:

```toml
# /etc/corrosion/config.toml
[db]
path = "/var/lib/corrosion/state.db"
schema_paths = ["/etc/corrosion/schemas"]
    
[gossip]
addr = "[fdaa:0:and:so:on:and:so:forth]:8787"
bootstrap = ["<your-app-name>.internal:8787"]
# addr and bootstrap for Fly.io deployment example are written 
# on startup by entrypoint script
plaintext = true # Cryptography and authz are handled by Fly.io private networking
max_mtu = 1372 # For Fly.io private network
disable_gso = true # For Fly.io private network

[api]
addr = "[::]:8080" # Must be available on IPv6 for Fly.io private network
# authz = { bearer-token = "secure-token" }

[admin]
path = "/app/admin.sock"

[telemetry]
prometheus.addr = "0.0.0.0:9090"

[log]
colors = false
```

The network settings are tailored for communication over your Fly.io private IPv6 WireGuard network.


