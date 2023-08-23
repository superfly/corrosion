# The `corrosion tls` command

In non-development environment, you'll want to configure `[gossip.tls]` to secure the transport of information within the cluster.

```
$ corrosion tls --help
Tls-related commands

Usage: corrosion tls [OPTIONS] <COMMAND>

Commands:
  ca      TLS certificate authority commands
  server  TLS server certificate commands
  client  TLS client certificate commands (for mutual TLS)
  help    Print this message or the help of the given subcommand(s)
```

## `corrosion tls ca generate`

A CA (Certificate Authority) is necessary to sign server certificates. It's expected for a Corrosion cluster to have a single CA key pair for signing all the nodes' server certificates.

**Store the key somewhere secure!**

```toml
$ corrosion tls ca generate --help
Generate a TLS certificate authority

Usage: corrosion tls ca generate [OPTIONS]
```

By default, certificates will be output as `ca_cert.pem` and `ca_key.pem` in the current directory.

## `corrosion tls server generate`

Generates a server certificate key pair for encrypting peer-to-peer packets. To be used in the `gossip.tls` configuration block.

The command accepts a `<IP>` positional argument, it needs to be the IP address your cluster's nodes will use for connecting to the server you're generating the certificates for.

You'll need to have previously generated a CA key pair as it's required to pass `--ca-key` and `--ca-cert` flags w/ paths to each PEM file respectively.

```
$ corrosion tls server generate --help
Generate a TLS server certificate from a CA

Usage: corrosion tls server generate [OPTIONS] --ca-key <CA_KEY> --ca-cert <CA_CERT> <IP>

Arguments:
  <IP>
```

## `corrosion tls client generate`

Generates a client certificate key pair to authorizing peer-to-peer clients.

You'll need to have previously generated a CA key pair as it's required to pass `--ca-key` and `--ca-cert` flags w/ paths to each PEM file respectively.

```
$ corrosion tls client generate
Generate a TLS certificate from a CA

Usage: corrosion tls client generate [OPTIONS] --ca-key <CA_KEY> --ca-cert <CA_CERT>
```