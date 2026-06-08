package main

import (
	"context"
	"encoding/json"
	"errors"
	"hash/maphash"
	"log"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Challenge #5c: Efficient Kafka-Style Log
// https://fly.io/dist-sys/5c/

// Idea for solution:
// "send": store a per log offset under the log name in the kv store. Use CAS with retry
// for a node/handler invocation to get exclusive access to that offset. Then write the message at
// log name + "-" + offset.
// "poll": read log messages using above log key pattern given users offset. Relying on the fact
// that offsets are contiguous read up to 4 and stop on first that does not exist.

// Test setup: 2 nodes, --concurrency 2n (4 threads total, 2 per node), --rate 1000, --time-limit 20.
// Rate 1000 means ~1000 ops/sec total across all threads with random jitter (gen/stagger).
// Each client thread is sequential: one request at a time, wait for response, then next.
// So at most 4 in-flight requests simultaneously. At this rate collisions on the same key
// are frequent. CAS failures are unavoidable under cross-node contention but same-node
// races can be reduced by tracking offsets locally instead of reading from lin-kv each time.
// msgs-per-op counts all messages (client<->node + node<->lin-kv); reducing CAS retries
// and lin-kv reads is the main lever for bringing it down.
//
// Starting point: 5b stats
// ./metrics.sh
// === results ===
// availability:   0.9996232
// msgs/op (all):  13.047209
// msgs/op (srv):  10.852882
// worst lag (s):  30.642763997
// send ok:        true
//
// === CAS operations (op=send) ===
// total:  13005
// ok:     8905
// failed: 4100
// ratio:  31.5%

// why is the key lag 30+s if the test only takes 20s to run?
//
// TODO is my realtime key lag going up linearly with the amount of keys? and does this mean keys as
// in the log key? and what is the lag by thread?
//
// think: can I get away with storing the logs in the seq-kv? and only offsets in lin-kv. offsets
// need be be contiguous/monotonic at least for my poll. issue with using lin-kv is that a client
// polling a node that lags behind forever would not see the logs unless that node talks to the
// other node instead of the kv directly.
// other idea: could each node be assigned an offset offset to reduce cas failure due to multiple
// leaders competing for the same offset. So like a shard inside the log? but how to then merge this
// into an overall monotonic append only log?

// idea 1: partition keys accross nodes to remove cas failures due to writes alternating between
// nodes. nodes themselves currently serialize offset

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	n.Handle("init", func(msg maelstrom.Message) error {
		logger = logger.With(slog.String("node", n.ID()))
		return nil
	})

	var muLogs sync.RWMutex
	// logsOffset caches the last known offset per log key to use as a CAS hint, reducing
	// lin-kv reads on contention; absent keys are treated as -1 so the first offset is 0
	logsOffset := make(map[string]int)

	n.Handle("send", func(msg maelstrom.Message) error {
		var body struct {
			Key string `json:"key"`
			Msg int    `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		reqLogger := logger.With(
			slog.String("op", "send"),
			slog.String("src", msg.Src),
			slog.String("key", body.Key),
			slog.Int("msg", body.Msg),
		)
		// TODO this is super slow!!! why?
		s := shard(body.Key, n.NodeIDs())
		if s != n.ID() {
			var wg sync.WaitGroup // TODO better way to to this?
			wg.Add(1)
			reqLogger.Debug("forwarding to other shard", "shard", s)
			err := n.RPC(s, msg.Body, func(msg maelstrom.Message) error {
				defer wg.Done()
				var body struct {
					Offset int `json:"offset"`
				}
				if err := json.Unmarshal(msg.Body, &body); err != nil {
					return err
				}
				// TODO no error handling needed here as rpc errors will be returned by n.RPC correct?
				return n.Reply(msg, map[string]any{
					"type":   "send_ok",
					"offset": body.Offset,
				})
			})
			wg.Wait()
			if err != nil {
				// todo
				return err
			}
		}

		muLogs.RLock()
		offset, ok := logsOffset[body.Key]
		if !ok {
			offset = -1 // first CAS goes from -1 to 0, making offsets 0-based
		}
		muLogs.RUnlock()

		for i := 0; ; i++ { // cas with retry
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			err := kv.CompareAndSwap(ctx, body.Key, offset, offset+1, true)
			if err == nil {
				reqLogger.Debug("cas", "result", "ok", "attempt", i, "offset", offset)
				offset++
				break
			}
			if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
				return err
			}
			errPre, _ := errors.AsType[*maelstrom.RPCError](err)
			reqLogger.Error("cas", "result", "precondition failed", "attempt", i, "offset", offset, "error", errPre.Text)
			ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			offset, err = kv.ReadInt(ctx, body.Key)
			if err != nil {
				return err
			}
		}

		muLogs.Lock()
		logsOffset[body.Key] = max(logsOffset[body.Key], offset)
		muLogs.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		err := kv.Write(ctx, logKey(body.Key, offset), body.Msg)
		if err != nil {
			// offset is claimed but the log entry was never written; leaves a hole that makes
			// all subsequent entries for this key invisible to poll; retrying the write would
			// reduce the chance of a hole but poll would also need to be adapted to skip holes
			return err
		}

		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body struct {
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// TODO can we keep poll as is? since we use lin-kv every node will see writes by the other
		// nodes in real time.

		msgs := make(map[string][][2]int, len(body.Offsets))
		update := make(map[string]int)
		for key, offset := range body.Offsets {
			// server may return any number of contiguous messages; 4 is arbitrary
			for range 4 {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()
				msg, err := kv.ReadInt(ctx, logKey(key, offset))
				if err != nil {
					if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
						break
					}
					return err

				}
				msgs[key] = append(msgs[key], [2]int{offset, msg})
				update[key] = offset
				offset++
			}
		}

		err := n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": msgs,
		})
		go func() {
			muLogs.Lock()
			for k, v := range update {
				logsOffset[k] = max(logsOffset[k], v)
			}
			muLogs.Unlock()
		}()
		return err
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body struct {
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// each client has a unique msg.Src and the Maelstrom kafka workload always sends the full
		// offset map per commit, so overwriting is safe; needs revisiting if callers commit partial maps
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		err := kv.Write(ctx, msg.Src, body.Offsets)
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body struct {
			Keys []string `json:"keys"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		result, err := kv.Read(ctx, msg.Src)
		if err != nil {
			if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				return err
			}
			return n.Reply(msg, map[string]any{
				"type":    "list_committed_offsets_ok",
				"offsets": map[string]int{},
			})
		}

		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": result,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func logKey(key string, offset int) string {
	return key + "-" + strconv.Itoa(offset)
}

func shard(key string, nodes []string) string {
	var h maphash.Hash
	h.WriteString(key)
	return nodes[h.Sum64()%uint64(len(nodes))]
}
