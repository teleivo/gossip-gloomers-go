# Gossip Gloomers (Go)

Solutions to the [Gossip Gloomers](https://fly.io/dist-sys/) distributed systems challenges by
fly.io. Built for learning.

## Challenges

Each challenge lives in its own directory and has a `./test.sh` script that builds the binary and
runs the Maelstrom test with the parameters from the challenge.

| Challenge | Directory |
|-----------|-----------|
| [#1: Echo](https://fly.io/dist-sys/1/) | [maelstrom-echo](maelstrom-echo) |
| [#2: Unique ID Generation](https://fly.io/dist-sys/2/) | [maelstrom-unique-ids](maelstrom-unique-ids) |
| [#3a: Single-Node Broadcast](https://fly.io/dist-sys/3a/) | [maelstrom-broadcast-3a](maelstrom-broadcast-3a) |
| [#3b: Multi-Node Broadcast](https://fly.io/dist-sys/3b/) | [maelstrom-broadcast-3b](maelstrom-broadcast-3b) |
| [#3c: Fault Tolerant Broadcast](https://fly.io/dist-sys/3c/) | [maelstrom-broadcast-3c](maelstrom-broadcast-3c) |
| [#3d: Efficient Broadcast, Part I](https://fly.io/dist-sys/3d/) (grid — explores 2√N latency, does not meet requirements) | [maelstrom-broadcast-3d-grid](maelstrom-broadcast-3d-grid) |
| [#3d: Efficient Broadcast, Part I](https://fly.io/dist-sys/3d/) (spanning tree — solution) | [maelstrom-broadcast-3d-tree](maelstrom-broadcast-3d-tree) |
| [#3e: Efficient Broadcast, Part II](https://fly.io/dist-sys/3e/) | [maelstrom-broadcast-3d-tree](maelstrom-broadcast-3d-tree) |
| [#4: Grow-Only Counter](https://fly.io/dist-sys/4/) | [maelstrom-counter](maelstrom-counter) |
| [#5a: Single-Node Kafka-Style Log](https://fly.io/dist-sys/5a/) | [maelstrom-kafka-5a](maelstrom-kafka-5a) |
| [#5b: Multi-Node Kafka-Style Log](https://fly.io/dist-sys/5b/) | [maelstrom-kafka-5b](maelstrom-kafka-5b) |
| [#5c: Efficient Kafka-Style Log](https://fly.io/dist-sys/5c/) (wip) | [maelstrom-kafka-5c](maelstrom-kafka-5c) |

## Resources

[Martin Kleppmann's distributed systems course](https://martin.kleppmann.com/2020/11/18/distributed-systems-and-elliptic-curves.html)
has lectures on broadcast and gossip protocols that are useful companions to these challenges.

Shapiro et al., [A comprehensive study of Convergent and Commutative Replicated Data
Types](https://inria.hal.science/inria-00555588/document). The G-Counter in challenge #4 follows
the state-based CRDT specification from this paper.

## Disclaimer

I wrote this for my personal learning and it is provided as-is without warranty. Feel free to use it!

See [LICENSE](LICENSE) for full license terms.
