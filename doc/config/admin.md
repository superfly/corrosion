# The [admin] block

The `[admin]` block configures the Unix domain socket used by the [`corrosion`](../cli/README.md) CLI to send administrative commands to a running agent.

### Optional fields

#### `admin.path`

Path of the admin Unix socket. Defaults to `/var/run/corrosion/admin.sock`.

Path for unix socket used to send commands for admin operations.

```toml
[admin]
path = "/admin.sock"
```
