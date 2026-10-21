// Command nfpeer is the Go client process the network fault suite (systests/adversarial/network-faults) kills with
// SIGKILL to crash a publisher or subscriber for real. It prints one event per line on stdout:
//
//	pub: "connected <sessionId>", then "sent <seq>" every 500 messages
//	sub: "image <sessionId> <correlationId> <joinPosition>", "gone <correlationId>", "seq <seq>" every 500 messages
//	     and "error <msg>" on a sequence/corruption error (sequences are checked per image, from the first seen)
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/e2e"
)

func main() {
	dir := flag.String("dir", "", "aeron dir")
	mode := flag.String("mode", "pub", "pub or sub")
	channel := flag.String("channel", "", "channel")
	stream := flag.Int("stream", 0, "stream id")
	exclusive := flag.Bool("exclusive", false, "exclusive publication")
	interval := flag.Duration("interval", 200*time.Microsecond, "delay between messages")
	flag.Parse()

	ctx := aeron.NewContext().AeronDir(*dir).MediaDriverTimeout(10 * time.Second).
		ErrorHandler(func(err error) { fmt.Printf("error %v\n", err) })
	a, err := aeron.Connect(ctx)
	if err != nil {
		fmt.Printf("error %v\n", err)
		os.Exit(1)
	}
	defer a.Close()
	if *mode == "pub" {
		publish(a, *channel, int32(*stream), *exclusive, *interval)
	} else {
		subscribe(a, *channel, int32(*stream))
	}
}

func publish(a *aeron.Aeron, channel string, stream int32, exclusive bool, interval time.Duration) {
	var pub aeron.Publisher
	var err error
	if exclusive {
		pub, err = a.AddExclusivePublication(channel, stream)
	} else {
		pub, err = a.AddPublication(channel, stream)
	}
	if err != nil {
		fmt.Printf("error %v\n", err)
		os.Exit(1)
	}
	for !pub.IsConnected() {
		time.Sleep(time.Millisecond)
	}
	fmt.Printf("connected %d\n", pub.SessionID())
	msg := make([]byte, 4096)
	buf := atomic.MakeBuffer(msg)
	for seq := int64(0); ; seq++ {
		n := e2e.HeaderLength + int(seq%300)
		if seq%10 == 0 {
			n = 3000
		}
		e2e.EncodeMessage(msg[:n], seq)
		for pub.Offer(buf, 0, int32(n), nil) < 0 {
			time.Sleep(50 * time.Microsecond)
		}
		if seq%500 == 0 {
			fmt.Printf("sent %d\n", seq)
		}
		time.Sleep(interval)
	}
}

func subscribe(a *aeron.Aeron, channel string, stream int32) {
	type reader struct {
		next      int64
		assembler *aeron.ImageFragmentAssembler
	}
	readers := map[int64]*reader{}
	sub, err := a.AddSubscriptionWithHandlers(channel, stream,
		func(img aeron.Image) {
			fmt.Printf("image %d %d %d\n", img.SessionID(), img.CorrelationID(), img.JoinPosition())
		},
		func(img aeron.Image) { fmt.Printf("gone %d\n", img.CorrelationID()) })
	if err != nil {
		fmt.Printf("error %v\n", err)
		os.Exit(1)
	}
	fmt.Println("ready")
	for {
		n := 0
		for _, img := range sub.Images() {
			r := readers[img.CorrelationID()]
			if r == nil {
				r = &reader{next: -1}
				r.assembler = aeron.NewImageFragmentAssembler(func(b *atomic.Buffer, off, length int32, _ *logbuffer.Header) {
					seq, err := e2e.DecodeMessage(b.GetBytesArray(off, length))
					if err != nil || (r.next >= 0 && seq != r.next) {
						fmt.Printf("error seq %d want %d: %v\n", seq, r.next, err)
					}
					r.next = seq + 1
					if seq%500 == 0 {
						fmt.Printf("seq %d\n", seq)
					}
				}, 0)
				readers[img.CorrelationID()] = r
			}
			n += img.Poll(r.assembler.OnFragment, 64)
		}
		if n == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
}
