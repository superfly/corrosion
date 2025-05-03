#! /bin/bash

set +e

arr=()

for i in {1..3}; do
    socat UNIX-LISTEN:/tmp/corrosion${i}_admin.sock,fork,reuseaddr,unlink-early,mode=770 TCP:corrosion${i}:6644 &
    sed "s/\[admin\]/&\npath = \"\/tmp\/corrosion${i}_admin.sock\"/" /opt/antithesis/test/v1/basic_test/config.toml > /tmp/corrosion${i}.toml
    arr+=("/tmp/corrosion${i}.toml")
done

echo ${arr[@]}

max_attempts=100
for i in {1..100}; do
    python3 /opt/antithesis/test/v1/basic_test/check_bookkeeping.py ${arr[@]}
    if [ $? -eq 0 ]; then
        break
    fi
    sleep 30
done
# todo: check data in db

// URL: https://flyio.antithesis.com/report/vUJU7WZ-WlOH353nOuvLoEC4/R-fjBCEoh-wNgC0SRbTtLq6P0rON04hXqjuf9gAJJvE.html?auth=v2.public.eyJuYmYiOiIyMDI1LTA1LTAzVDEwOjMyOjIzLjY3NjU3NjA5MVoiLCJzY29wZSI6eyJSZXBvcnRTY29wZVYxIjp7ImFzc2V0IjoiUi1makJDRW9oLXdOZ0MwU1JiVHRMcTZQMHJPTjA0aFhxanVmOWdBSkp2RS5odG1sIiwicmVwb3J0X2lkIjoidlVKVTdXWi1XbE9IMzUzbk91dkxvRUM0In19fUJePb9BwOTLNHVhSWLLWHcfCQ0DoWRag--fW9tvb5obpuYWQXue76G1ZNIqURrAoUW6jzT7n6e5qV8FWoBULwo 
// Notebook: /notebook/webhook/flyio.nb2
// Params: antithesis.config_image=antithesis-config%3Aed30689b&antithesis.description=basic_test+on+main&antithesis.duration=0.1&antithesis.images=corrosion%3Aed30689b%2Ccorro-client%3Aed30689b&antithesis.integrations.type=none&antithesis.is_ephemeral=false&antithesis.report.recipients=somtochi%40fly.io
// Property: Always: Missed Always grouped by: ineffective deletion of gaps in-db
// Failed on 2025-05-03T10:47Z
replay_moment = Moment._exact(
    Session.from({id: "bbac403bd37be5a0eb902c4d94761ced-30-8"}),
    '-395759189880979307',
    568.7353141165804
)
