package main

import (
	"encoding/json"
	"log"
	"maps"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Challenge #5a: Single-Node Kafka-Style Log
// https://fly.io/dist-sys/5a/

func main() {
	n := maelstrom.NewNode()

	var muLogs sync.RWMutex
	logs := make(map[string][]int)

	var muOffsets sync.RWMutex
	offsets := make(map[string]map[string]int)

	n.Handle("send", func(msg maelstrom.Message) error {
		var body struct {
			Key string `json:"key"`
			Msg int    `json:"msg"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		muLogs.Lock()
		offset := len(logs[body.Key])
		logs[body.Key] = append(logs[body.Key], body.Msg)
		muLogs.Unlock()

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

		muLogs.RLock()
		msgs := make(map[string][][2]int, len(body.Offsets))
		for key, offset := range body.Offsets {
			log, ok := logs[key]
			if !ok {
				continue
			}
			// server may return any number of contiguous messages; 3 is arbitrary
			for i := 0; i+offset < len(log) && i < 3; i++ {
				msgs[key] = append(msgs[key], [2]int{i + offset, log[i+offset]})
			}
		}
		muLogs.RUnlock()

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

		muOffsets.Lock()
		if _, ok := offsets[msg.Src]; !ok {
			offsets[msg.Src] = make(map[string]int)
		}
		maps.Copy(offsets[msg.Src], body.Offsets)
		muOffsets.Unlock()

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

		result := make(map[string]int, len(body.Keys))
		muOffsets.RLock()
		clientOffsets := offsets[msg.Src]
		for _, key := range body.Keys {
			if v, ok := clientOffsets[key]; ok {
				result[key] = v
			}
		}
		muOffsets.RUnlock()

		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": result,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
