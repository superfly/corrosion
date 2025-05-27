#!/bin/bash
set -e
# Add gossip and bootstrap addresses to Corrosion config file before 
# starting agent with Dockerfile CMD
IP_ADDR=$(ip -4 a s eth0 | grep inet | awk '{print $2}' | cut -d '/' -f 1)
export IP_ADDR=${IP_ADDR}
envsubst < /etc/corrosion/config.toml.tpl > /etc/corrosion/config.toml

cat /etc/corrosion/config.toml
su - corrosion

# access admin socket over network
socat TCP-LISTEN:6644,reuseaddr,fork UNIX-CONNECT:/app/admin.sock &

# start consul agent
# TODO: bind to IP_ADDR
consul agent -dev --retry-join consul -bind=0.0.0.0 -client=0.0.0.0 -log-level=error -log-file=/tmp/consul.log &

# start corro-consul
corrosion consul sync &

exec "$@"
