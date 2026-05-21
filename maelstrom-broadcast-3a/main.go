package main

import (
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	messages []float64
	topology map[string]any
)

// https://fly.io/dist-sys/3a/
func main() {
	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		data := body["message"]
		if data == nil {
			return fmt.Errorf("empty message field")
		}
		msgData, ok := data.(float64)
		if !ok {
			return fmt.Errorf("message should be an integer: %v", data)
		}

		messages = append(messages, msgData)
		body = map[string]any{
			"type": "broadcast_ok",
		}

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := map[string]any{
			"type":     "read_ok",
			"messages": messages,
		}
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		var ok bool
		topology, ok = body["topology"].(map[string]any)
		if !ok {
			return fmt.Errorf("malformed topology message body: %v", body["topology"])
		}

		reply := map[string]any{
			"type": "topology_ok",
		}
		return n.Reply(msg, reply)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
