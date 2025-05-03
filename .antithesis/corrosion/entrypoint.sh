#!/bin/bash
set -e
# Add gossip and bootstrap addresses to Corrosion config file before 
# starting agent with Dockerfile CMD
IP_ADDR=$(ip -4 a s eth0 | grep inet | awk '{print $2}' | cut -d '/' -f 1)
echo "IP_ADDR: ${IP_ADDR}"
envsubst < /etc/corrosion/config.toml.tpl > /etc/corrosion/config.toml
sed -i 's/\[gossip\]/&\naddr = "'${IP_ADDR}':8787"/' /etc/corrosion/config.toml

su - corrosion

# access admin socket over network
socat TCP-LISTEN:6644,reuseaddr,fork UNIX-CONNECT:/app/admin.sock &

exec "$@"
