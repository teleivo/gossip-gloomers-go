#!/usr/bin/env bash
# https://fly.io/dist-sys/5a/
set -euo pipefail

go build -o maelstrom-kafka .
maelstrom test -w kafka --bin ./maelstrom-kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000 "$@"
