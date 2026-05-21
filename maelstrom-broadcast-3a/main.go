package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	messages []int
	topology map[string][]string
)

// https://fly.io/dist-sys/3a/
func main() {
	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		messages = append(messages, body.Message)
		reply := map[string]any{
			"type": "broadcast_ok",
		}
		return n.Reply(msg, reply)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := map[string]any{
			"type":     "read_ok",
			"messages": messages,
		}
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body struct {
			Topology map[string][]string `json:"topology"`
		}
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		topology = body.Topology

		reply := map[string]any{
			"type": "topology_ok",
		}
		return n.Reply(msg, reply)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
