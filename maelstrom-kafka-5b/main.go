package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Challenge #5b: Multi-Node Kafka-Style Log
// https://fly.io/dist-sys/5b/

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

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

		// TODO zero offset is not used, should it?
		var offset int
		muLogs.Lock()
		offset = logsOffset[body.Key]
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			err := kv.CompareAndSwap(ctx, body.Key, offset, offset+1, true)
			if err == nil {
				offset++
				break
			}
			if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
				// TODO any error we should also retry on?
				return err
			}
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

		// TODO any sanitization or better parseablity I should use in key scheme?
		err := kv.Write(context.Background(), body.Key+strconv.Itoa(offset), body.Msg)
		if err != nil {
			// TODO rollback offset?
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

		// TODO should I watch for "new" offsets and update my internal logsOffset map? if I happen
		// to read a log with an offset higher than what I have in my map that is knowledge I can
		// update almost for free; at the cost of a lock on the map. I could collect a
		// map[string]int of the max offsets I've seen and then after the reply/or in a goroutine
		// update the logsOffset
		msgs := make(map[string][][2]int, len(body.Offsets))
		for key, offset := range body.Offsets {
			// TODO read key+offset until key+offset+3 ? and len(log) replaced by
			// logsOffset[body.Key] as the last known offset?
			// but it could be that this node has no sends so it has not initialized its in memory
			// offsets. So naive would be I read + 3 times until the first errors and tells me no
			// key

			// server may return any number of contiguous messages; 3 is arbitrary
			for i := range 3 {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()
				msg, err := kv.ReadInt(ctx, key+strconv.Itoa(offset))
				if err != nil {
					if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
						break
					}
					// TODO should we retry some errors?
					return err

				}
				msgs[key] = append(msgs[key], [2]int{i + offset, msg})
			}
		}

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
