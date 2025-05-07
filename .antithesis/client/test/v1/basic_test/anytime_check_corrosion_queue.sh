#!/usr/bin/env bash

set -e

IFS=',' read -ra addrs <<< "${CORROSION_ADDRS:-corrosion1,corrosion2,corrosion3}"
echo "${addrs[@]}"
declare -A queue_state


# Check if the queue has been full for more than 20 minutes
for i in {1..40}; do
    for addr in "${addrs[@]}"; do
        value=$(curl -s http://$addr:9090/metrics | grep "^corro_agent_changes_in_queue" | awk '{print $2}')

        if [[ -z $value ]]; then
            echo "[-] Failed to curl $addr metrics"
            continue
        fi

        if [[ $value -gt 20000 ]];
        then
            echo "[-] Number of changes in $addr queue is too high: $value"
            queue_state[$addr]=$((${queue_state[$addr]} + 1))
        else
            echo "[+] Number of changes in $addr queue is ok: $value"
            queue_state[$addr]=0
        fi
    done

    sleep 60
done

exit_code=0
for addr in "${addrs[@]}"; do
    if [[ ${queue_state[$addr]} -gt 30 ]]; then
        echo "[-] $addr has too many changes in the queue"
        exit_code=1
    fi
done

if [[ $exit_code -eq 0 ]]; then
    echo "[+] All queues are ok"
    exit 0
fi

echo "[-] Some queues have too many changes"
exit 1
