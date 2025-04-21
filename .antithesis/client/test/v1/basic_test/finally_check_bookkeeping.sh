#! /bin/bash

set -e
# make this better + files

arr=()

for i in {1..3}; do
    socat UNIX-LISTEN:/tmp/corrosion${i}_admin.sock,fork,reuseaddr,unlink-early,mode=770 TCP:corrosion${i}:6644 &
    sed "s/\[admin\]/&\npath = \"\/tmp\/corrosion${i}_admin.sock\"/" /opt/antithesis/test/v1/basic_test/config.toml > /tmp/corrosion${i}.toml
    arr+=("/tmp/corrosion${i}.toml")
done

echo ${arr[@]}

max_attempts=100
for i in {1..max_attempts}; do
    python3 /opt/antithesis/test/v1/basic_test/check_bookkeeping.py ${arr[@]}
    if [ $? -eq 0 ]; then
        break
    fi
    sleep 30
done
# todo: check data in db
