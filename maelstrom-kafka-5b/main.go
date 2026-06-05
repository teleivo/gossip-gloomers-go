package main

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Challenge #5b: Multi-Node Kafka-Style Log
// https://fly.io/dist-sys/5b/

// Idea for solution:
// "send": store a per log offset under the log name in the kv store. Use CAS with retry
// for a node/handler invocation to get exclusive access to that offset. Then write the message at
// log name + "-" + offset.
// "poll": read log messages using above log key pattern given users offset. Relying on the fact
// that offsets are contiguous read up to 4 and stop on first that does not exist.

// TODO towards 5c
// current 5b stats
// ./metrics.sh
// === results ===
// availability:   0.999573
// msgs/op (all):  13.805498
// msgs/op (srv):  11.592741
// worst lag (s):  30.630575327
// send ok:        true
//
// === CAS operations (op=send) ===
// total:  13183
// ok:     8824
// failed: 4359
// ratio:  33.0%
//
// updating local offsets on poll reduced failed cas a bit
//
// ./metrics.sh
// === results ===
// availability:   0.9995635
// msgs/op (all):  13.761144
// msgs/op (srv):  11.569644
// worst lag (s):  30.760134432
// send ok:        true
//
// === CAS operations (op=send) ===
// total:  12593
// ok:     8681
// failed: 3912
// ratio:  31.0%
//
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

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	level := slog.LevelDebug
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	n.Handle("init", func(msg maelstrom.Message) error {
		logger = logger.With(slog.String("node", n.ID()))
		return nil
	})

	// TODO rethink offsets as poll API is different than I thought. So if I get x I need to find x
	// or the smallest offset after x

	var muLogs sync.RWMutex
	logsOffset := make(map[string]int)

	n.Handle("send", func(msg maelstrom.Message) error {
		var body struct {
			Key string `json:"key"`
			Msg int    `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		reqLogger := logger.With(slog.String("op", "send"), slog.String("src", msg.Src), slog.String("key", body.Key), slog.Int("msg", body.Msg))

		var offset int
		muLogs.Lock()
		// TODO rethink this: purpose is to not have to read offset at the start and reduce cas
		// failures. Can I shrink the critical section?
		offset = logsOffset[body.Key]
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
				// TODO any error we should also retry on?
				return err
			}
			reqLogger.Error("cas", "result", "precondition failed", "attempt", i, "offset", offset)
			ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			offset, err = kv.ReadInt(ctx, body.Key)
			if err != nil {
				// TODO any error we should retry on?
				return err
			}
		}
		logsOffset[body.Key] = offset
		muLogs.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		err := kv.Write(ctx, logKey(body.Key, offset), body.Msg)
		if err != nil {
			// TODO rollback offset? I mean offsets are ok to be sparse but my current poll relies
			// on contiguous offsets
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

		// TODO idea 1: keep what I have but update local offsets if I was able to read an offset
		// for a key > than what I have locally in my offsets. downside locking of local offsets but
		// same as idea 2. upside no extra network call.
		// TODO idea 2: read up to date offset for each key. downside one extra read per requested
		// key. upside freshness
		// TODO should I watch for "new" offsets and update my internal logsOffset map? if I happen
		// to read a log with an offset higher than what I have in my map that is knowledge I can
		// update almost for free; at the cost of a lock on the map. I could collect a
		// map[string]int of the max offsets I've seen and then after the reply/or in a goroutine
		// update the logsOffset

		msgs := make(map[string][][2]int, len(body.Offsets))
		update := make(map[string]int)
		for key, offset := range body.Offsets {
			offset = max(offset, 1)
			// TODO read key+offset until key+offset+3 ? and len(log) replaced by
			// logsOffset[body.Key] as the last known offset?
			// but it could be that this node has no sends so it has not initialized its in memory
			// offsets. So naive would be I read + 3 times until the first errors and tells me no
			// key

			// server may return any number of contiguous messages; 4 is arbitrary
			for range 4 {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()
				msg, err := kv.ReadInt(ctx, logKey(key, offset))
				if err != nil {
					if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
						break
					}
					// TODO should we retry some errors?
					return err

				}
				msgs[key] = append(msgs[key], [2]int{offset, msg})
				update[key] = offset
				offset++
			}
		}

		// TODO add idea 1: can also do this after sending reply to not add to latency
		muLogs.Lock()
		for k, v := range update {
			logsOffset[k] = max(logsOffset[k], v)
		}
		muLogs.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": msgs,
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body struct {
			Offsets map[string]int `json:"offsets"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// TODO ok to just store the entire map? or do I need to override only keys present in body.Offset
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		err := kv.Write(ctx, msg.Src, body.Offsets)
		if err != nil {
			// TODO retry anything
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
		if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			return err
		}

		if err != nil && maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
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
