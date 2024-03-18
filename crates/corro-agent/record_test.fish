#!/usr/bin/env fish

sudo echo "Got root, thanks!"

cargo run --test $argv[1] --release 2>/dev/null -- --nocapture --ignored &

echo "PID: $last_pid"
set -g keep_tryin true

while $keep_tryin
    sudo nperf record --pid $last_pid -o $argv[1]-recording.txt 2>/dev/null
    if test $status -eq 0
        set -g keep_tryin false
    else
        set -g keep_tryin true
    end
end
