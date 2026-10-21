// Command subscriber consumes sequenced messages and fails on any gap, duplicate or corruption.
package main

import (
	"flag"
	"log"
	"time"

	"github.com/lirm/aeron-go/e2e"
)

func main() {
	aeronDir := flag.String("aeron-dir", e2e.AeronDir(), "media driver directory (default $AERON_DIR)")
	channel := flag.String("channel", "aeron:udp?endpoint=localhost:20121", "subscription channel")
	stream := flag.Int("stream", 1001, "stream id")
	count := flag.Int64("count", 10000, "number of messages to receive before exiting")
	from := flag.Int64("from", 0, "first expected sequence number")
	timeout := flag.Duration("timeout", 2*time.Minute, "give up after this long")
	readyFile := flag.String("ready-file", "", "file touched once the subscription is added")
	flag.Parse()

	a, err := e2e.Connect(*aeronDir)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer a.Close()
	sub, err := a.AddSubscription(*channel, int32(*stream))
	if err != nil {
		log.Fatalf("add subscription: %v", err)
	}
	defer sub.Close()
	if err := e2e.TouchFile(*readyFile); err != nil {
		log.Fatal(err)
	}

	checker := e2e.NewSequenceChecker(*from)
	if err := e2e.Consume(e2e.ReassemblingPoller(sub), checker, *from+*count, *timeout); err != nil {
		log.Fatal(err)
	}
	log.Printf("OK %s", checker)
}
