package main

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// https://fly.io/dist-sys/2/ Challenge #2: Unique ID Generation
// we receive
// {
//   "type": "generate"
// }
// we respond
// {
//   "type": "generate_ok",
//   "id": 123
// }
//
// Solved using: https://en.wikipedia.org/wiki/Snowflake_ID
// 64-bit: 0 | 41-bit timestamp ms since some epoch | 10-bit worker ID | 12-bit sequence number
//
// We chose our RC batch start date as our epoch: 18th May 2026
// Timstamp of 2^41 ms: ~69 years
//
// 12-bit sequence number is to avoid duplicate ids generated on the same node in the same millisecond. So a node can generate 4069 ids per millisecond
//
// Challenge allows id to be of any type. We marshal IDs into string to avoid JS double precision issue "1234"

// RC batch start date
var epochBase = time.Date(2026, 0o5, 18, 0, 0, 0, 0, time.UTC)

func main() {
	n := maelstrom.NewNode()

	var nodeID int64
	n.Handle("init", func(msg maelstrom.Message) error {
		// strip leading character 'n' from node ID
		// https://fly.io/dist-sys/1/ node id is "n1", "n2", ...
		var err error
		nodeID, err = strconv.ParseInt(n.ID()[1:], 10, 64)
		if err != nil {
			return err
		}

		return nil
	})

	refTimestamp := now()
	var counter uint16
	var mu sync.Mutex
	n.Handle("generate", func(msg maelstrom.Message) error {
		timestamp := now()
		var curCounter uint16
		mu.Lock()
		if timestamp == refTimestamp {
			counter++
			// TODO prevent duplicate ids when all 12-bits are used up. Either return an error/wait a ms
			if counter > 4095 {
				panic("counter overflow")
			}
		} else {
			refTimestamp = timestamp
			counter = 0
		}
		curCounter = counter
		mu.Unlock()

		id := format(timestamp, nodeID, curCounter)
		body := map[string]any{
			"type": "generate_ok",
			"id":   fmt.Sprintf("%d", id),
		}

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func now() int64 {
	return time.Since(epochBase).Milliseconds()
}

// format returns a snowflake formatted ID.
// 0|41-bit|10-bit|12-bit
func format(timestamp, nodeID int64, counter uint16) int64 {
	nodeID &= 1023
	nodeID = nodeID << 12

	byteCounter := int64(counter)
	byteCounter &= int64(4095)

	timestamp &= (1 << 41) - 1
	timestamp <<= 22
	return timestamp | nodeID | byteCounter
}
