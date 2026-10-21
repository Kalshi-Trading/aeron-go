// Command publisher offers sequenced messages (see e2e.EncodeMessage) on a channel, optionally asking
// the archive to record it first.
package main

import (
	"flag"
	"log"
	"time"

	"github.com/lirm/aeron-go/e2e"
)

func main() {
	aeronDir := flag.String("aeron-dir", e2e.AeronDir(), "media driver directory (default $AERON_DIR)")
	channel := flag.String("channel", "aeron:udp?endpoint=localhost:20121", "publication channel")
	stream := flag.Int("stream", 1001, "stream id")
	count := flag.Int64("count", 10000, "number of messages")
	from := flag.Int64("from", 0, "first sequence number")
	size := flag.Int("size", 256, "message size in bytes")
	interval := flag.Duration("interval", 0, "pause between messages")
	exclusive := flag.Bool("exclusive", false, "use an exclusive publication")
	waitConnected := flag.Duration("wait-connected", 30*time.Second, "wait for a subscriber before publishing")
	record := flag.String("record", "", "if set, StartRecording on this channel (remote) before publishing")
	controlChannel := flag.String("control-channel", e2e.DefaultControlChannel, "archive control request channel")
	controlStream := flag.Int("control-stream", e2e.DefaultControlStream, "archive control request stream")
	readyFile := flag.String("ready-file", "", "file touched after -ready-after messages have been sent")
	readyAfter := flag.Int64("ready-after", 0, "number of messages to send before touching -ready-file")
	linger := flag.Duration("linger", time.Second, "keep the publication open after the last message")
	flag.Parse()

	a, err := e2e.Connect(*aeronDir)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer a.Close()

	if *record != "" {
		arch, err := e2e.ConnectArchive(*aeronDir, *controlChannel, int32(*controlStream))
		if err != nil {
			log.Fatalf("connect archive: %v", err)
		}
		defer arch.Close()
		if _, err := arch.StartRecording(*record, int32(*stream), false, true); err != nil {
			log.Fatalf("StartRecording(%s): %v", *record, err)
		}
		log.Printf("recording %s stream %d", *record, *stream)
	}

	addPublication := a.AddPublication
	if *exclusive {
		addPublication = a.AddExclusivePublication
	}
	pub, err := addPublication(*channel, int32(*stream))
	if err != nil {
		log.Fatalf("add publication: %v", err)
	}
	defer pub.Close()
	if err := e2e.WaitConnected(pub, *waitConnected); err != nil {
		log.Fatal(err)
	}
	log.Printf("publishing %d messages on %s stream %d session %d", *count, *channel, *stream, pub.SessionID())

	ready := func(seq int64) {
		if seq+1-*from == *readyAfter {
			if err := e2e.TouchFile(*readyFile); err != nil {
				log.Fatal(err)
			}
		}
	}
	if *readyAfter == 0 {
		ready(*from - 1)
	}
	start := time.Now()
	if err := e2e.Publish(pub, e2e.PublishOptions{From: *from, Count: *count, Size: *size, Interval: *interval,
		Timeout: time.Duration(*count)**interval + time.Minute, OnSent: ready}); err != nil {
		log.Fatal(err)
	}
	log.Printf("published %d messages in %v, position %d", *count, time.Since(start), pub.Position())
	time.Sleep(*linger)
}
