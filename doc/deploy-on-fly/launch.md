# Launch a Corrosion cluster

This example deploys a 2-node Corrosion cluster on [Fly Machines](https://fly.io/docs/machines/) VMs, using the example files in `corrosion/examples/fly/` within the Corrosion git repository.

Each node is a separate [Fly Machine](https://fly.io/docs/machines/), and nodes communicate with each other over Fly.io private networking. The cluster is initialized with an empty database.

You'll be provisioning two `shared-cpu-1x` Machines and two 1GB [Fly volumes](https://fly.io/docs/reference/volumes/) for persistent storage. See the [Fly.io resource pricing](https://fly.io/pricing/) page for cost information.

## Speedrun

In a nutshell, deploying Corrosion from source looks like this:

```bash
$ cp examples/fly/fly.toml .                        # copy the Fly Launch config file
$ fly launch --dockerfile examples/fly/Dockerfile   # launch a new app on Fly.io using the example files
$ fly scale count 1 --region <second-fly-region>    # add a second node to the cluster
```

Here it is again, in slightly more detail:

## Preparation

Clone the Corrosion repository, and enter its root directory. 

```bash
$ git clone https://github.com/superfly/corrosion.git && cd corrosion
```

Check out the latest release as a new branch.

```bash
$ git checkout tags/v0.1.0 -b v010
```

[Fly Launch](https://fly.io/docs/apps/launch/) uses a TOML file for [app configuration](https://fly.io/docs/reference/configuration/). Copy the example `fly.toml` to the working directory.

```bash
$ cp examples/fly/fly.toml .
```

## Launch a new app

Launch a new app on Fly.io, using the example Dockerfile.

Follow the prompts. Say `Yes` to copying the configuration from `fly.toml`, and `No` to adding any other databases. You can say `Yes` to `deploy now`, as well.

```bash
$ fly launch --dockerfile examples/fly/Dockerfile
```

```
Creating app in /Users/chris/Corrosion/corrosion
An existing fly.toml file was found for app corrosion
? Would you like to copy its configuration to the new app? Yes
Using dockerfile examples/fly/Dockerfile
? Choose an app name (leaving blank will default to 'corrosion') zaphod-test-app
? Select Organization: Zaphod Beeblebrox (personal)
Some regions require a paid plan (bom, fra, maa).
See https://fly.io/plans to set up a plan.

? Choose a region for deployment: Toronto, Canada (yyz)
App will use 'yyz' region as primary

Created app 'zaphod-test-app' in organization 'personal'
Admin URL: https://fly.io/apps/zaphod-test-app
Hostname: zaphod-test-app.fly.dev
? Would you like to set up a Postgresql database now? No
? Would you like to set up an Upstash Redis database now? No
Wrote config file fly.toml
? Would you like to deploy now? Yes
```

If you happen to have responded `No` to the `Would you like to deploy now?` line, you can execute the deployment step separately with the `fly deploy` command.

Fly Launch will build the Docker image, create a storage volume, and deploy your new Corrosion app on a single Fly Machine.

When deployment is complete, check that a Machine has been created and is in the `started` state with the `fly status` command:

```bash
$ fly status
```
```out
App
  Name     = zaphod-test-app                                        
  Owner    = personal                                   
  Hostname = zaphod-test-app.fly.dev                                
  Image    = zaphod-test-app:deployment-01HD1QXKKJZX9RD1WP52YCRD9Q  
  Platform = machines                                   

Machines
PROCESS ID              VERSION REGION  STATE   ROLE    CHECKS  LAST UPDATED         
app     9185741db15398  1       yyz     started                 2023-10-18T16:04:03Z
```

You can also see the latest internal activity with the `fly logs` command.

## Check on the database

To get a shell session on a Fly Machine use [`fly ssh console`](https://fly.io/docs/flyctl/ssh-console/): 

```bash
$ fly ssh console --pty --select
```

A Corrosion node's local database is located by default at `/var/lib/corrosion/state.db`. At this point it contains no data, but the `todos` table has been created according to the schema file `/etc/corrosion/schemas/todo.sql`.

You can read from this database using `sqlite3` from the command line on the Corrosion node.

```bash
# sqlite3 /var/lib/corrosion/state.db '.mode column' 'PRAGMA table_info(todos);'
```
```plain
cid  name          type     notnull  dflt_value  pk
---  ------------  -------  -------  ----------  --
0    id            BLOB     1                    1 
1    title         TEXT     1        ''          0 
2    completed_at  INTEGER  0                    0 
```

## Add a second node

Scale up to two Machines. Put the second one in [another part of the world](https://fly.io/docs/reference/regions/) if you like:

```bash
$ fly scale count 1 --region <second-fly-region>
```

The [`fly scale count` command](https://fly.io/docs/flyctl/scale-count/) provisions a new Machine with an empty volume attached, because the original Machine has a volume. Once the new node joins the cluster, Corrosion populates its local database on this volume with the latest data from the cluster.

Once the second Machine is running, you should be able to see log messages from Corrosion on both instances.

You can use the example database to test out your Corrosion cluster: [Work with cluster data on Fly.io](./explore.md).

## Example Files

### Fly Launch configuration

The Fly platform uses a [TOML file to configure an app for deployment](https://fly.io/docs/reference/configuration/).

```bash
$ cp examples/fly/fly.toml .
```

```toml
# Example fly.toml
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

The `app` entry is updated with your chosen app name on launch.

The `mounts` section tells Fly Launch that this app needs a storage volume named `"corro_data"` and that it should be mounted at `/var/lib/corrosion` in the Machine's file system. A Fly Volume of this name will be created for the first Machine on the first deployment.

Corrosion exports Prometheus metrics; the `metrics` section tells the [Fly Platform where to look for them](https://fly.io/docs/reference/metrics/#prometheus-on-fly-io). This port setting corresponds to the setting for `prometheus.addr` under `telemetry` in the [Corrosion configuration](#corrosion-configuration).

No public services are configured for the Corrosion cluster, because nodes communicate over private networking.

### Dockerfile

The example Dockerfile `corrosion/examples/fly/Dockerfile` builds Corrosion from a local copy of the source repository in a separate stage and creates a final Debian-based Docker image with the built Corrosion binary included. It copies the example files from the local `corrosion/examples/fly/corrosion-files` directory and uses them to configure and run Corrosion with an empty example database.

SQLite3 and [not-perf](https://github.com/koute/not-perf) are installed for convenience.

```Docker
# build image
FROM rust:latest as builder

RUN apt update && apt install -y build-essential gcc-x86-64-linux-gnu clang llvm

# Install mold
ENV MOLD_VERSION=1.11.0
RUN set -eux; \
    curl --fail --location "https://github.com/rui314/mold/releases/download/v${MOLD_VERSION}/mold-${MOLD_VERSION}-x86_64-linux.tar.gz" --output /tmp/mold.tar.gz; \
    tar --directory "/usr/local" -xzvf "/tmp/mold.tar.gz" --strip-components 1; \
    rm /tmp/mold.tar.gz; \
    mold --version;

RUN set -eux; \
    curl --fail --location "https://github.com/koute/not-perf/releases/download/0.1.1/not-perf-x86_64-unknown-linux-gnu.tgz" --output /tmp/nperf.tar.gz; \
    tar --directory "/usr/local/bin" -xzvf "/tmp/nperf.tar.gz"; \
    rm /tmp/nperf.tar.gz; \
    nperf --version;

WORKDIR /usr/src/app
COPY . .
# Will build and cache the binary and dependent crates in release mode
RUN --mount=type=cache,target=/usr/local/cargo,from=rust:latest,source=/usr/local/cargo \
    --mount=type=cache,target=target \
    cargo build --release && mv target/release/corrosion ./

# Runtime image
FROM debian:bookworm-slim

RUN apt update && apt install -y sqlite3 watch && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/nperf /usr/src/app/corrosion /usr/local/bin/

# Create "corrosion" user
RUN useradd -ms /bin/bash corrosion

COPY examples/fly/entrypoint.sh /entrypoint.sh
COPY examples/fly/corrosion-files/ /etc/corrosion/

ENTRYPOINT ["/entrypoint.sh"]
# Run the app
CMD ["corrosion", "agent"]
```


### Corrosion configuration

The supplied example Corrosion config file, `config.toml`, omits the `gossip.addr` and `gossip.bootstrap` entries. On startup. the `entrypoint.sh` script fills these in using `FLY_PRIVATE_IP` and `FLY_APP_NAME` environment variables that exist within the runtime environment.

The complete configuration file looks something like this on the running Machine:

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
plaintext = true   # Cryptography and authz are handled by Fly.io private networking
max_mtu = 1372     # For Fly.io private network
disable_gso = true # For Fly.io private network

[api]
addr = "[::]:8080" # Must be available on IPv6 for Fly.io private network

[admin]
path = "/app/admin.sock"

[telemetry]
prometheus.addr = "0.0.0.0:9090"

[log]
colors = false
```

The network settings in this example config are tailored for communication over your Fly private IPv6 WireGuard network. Cluster members communicate over port 8787, and the Corrosion API is reachable on port 8080. Corrosion exports Prometheus metrics at port 9090, which is hooked up to the [Prometheus service on Fly.io](https://fly.io/docs/reference/metrics/#prometheus-on-fly-io) via the `metrics` section in [`fly.toml`](#fly-launch-configuration).
