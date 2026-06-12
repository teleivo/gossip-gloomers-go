#!/usr/bin/env bash
# https://fly.io/dist-sys/4/
set -euo pipefail

go build -o maelstrom-counter .
# challenge test command
maelstrom test -w g-counter --bin ./maelstrom-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition "$@"
# :servers {:send-count 1406,
#   :recv-count 1362,
#   :msg-count 1406,
#   :msgs-per-op 0.7177131},
# I think max 6ms for read and 10ms for write but the legends are hard to read
# also works with more nodes and a higher request rate, gossiping the full state to 4 nodes every
# second gives
# :servers {:send-count 24724,
#   :recv-count 22572,
#   :msg-count 24724,
#   :msgs-per-op 1.3009208}
# at max 7ms/14ms for read and write at the beginning/end of the test
# maelstrom test -w g-counter --bin ./maelstrom-counter --node-count 100 --rate 1000 --time-limit 20 --nemesis partition
