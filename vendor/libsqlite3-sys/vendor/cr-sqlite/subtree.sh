#!/bin/sh

# If directory exists then pull, otherwise add
if [ -d "core/rs/sqlite-rs-embedded" ]; then
    git subtree pull --squash -P core/rs/sqlite-rs-embedded git@github.com:vlcn-io/sqlite-rs-embedded.git aba562843b26642e2242f8e3388b83a1a2625031
else
    git subtree add --squash -P core/rs/sqlite-rs-embedded git@github.com:vlcn-io/sqlite-rs-embedded.git aba562843b26642e2242f8e3388b83a1a2625031
fi
