#!/usr/bin/env bash

set +e

IFS=',' read -ra addrs <<< "${CORROSION_ADDRS:-corrosion1,corrosion2,corrosion3}"
echo "${addrs[@]}"
declare -A queue_state


# Check if the runtime loop exited early on any node.
# Missing gossip member metrics is a sign of this
for addr in "${addrs[@]}"; do
    output=$(curl -s http://$addr:9090/metrics)
    if [[ $? -ne 0 ]]; then
        echo "[-] Failed to curl $addr metrics"
        continue
    fi
    value=$(echo "$output" | grep "^corro_gossip_members" | awk '{print $2}')

    if [[ $? -ne 0 ]]; then
        echo "[-] Rutime loop might not be running on $addr"
        exit 1        
    fi

    echo "[+] $addr has $value gossip members"
done
