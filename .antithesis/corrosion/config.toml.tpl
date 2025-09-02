[db]
path = "/var/lib/corrosion/state.db"
schema_paths = ["/etc/corrosion/schemas"]

[gossip]
# addr for Fly.io deployment example are written
# on startup by entrypoint script
addr = "$IP_ADDR:8787"
pg.addr = "$IP_ADDR:5470"

# TODO: (maybe use a broadcast address instead?)
bootstrap = ["corrosion1:8787"]
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

[consul.client]
address = "localhost:8500"

[perf]
wal_threshold_mb = 5
