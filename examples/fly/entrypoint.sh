#!/bin/bash

# Add gossip and bootstrap addresses to Corrosion config file before 
# starting agent with Dockerfile CMD
sed -i 's/\[gossip\]/&\naddr = "['${FLY_PRIVATE_IP}']:8787"\
bootstrap = ["'${FLY_APP_NAME}'.internal:8787"]/' /app/config.toml

exec "$@"