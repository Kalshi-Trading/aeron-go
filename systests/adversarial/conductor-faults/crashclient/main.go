// Command crashclient is a minimal Aeron client run as a child process by the conductor-faults adversarial suite so
// the tests can kill -9 / SIGSTOP a real client process. It prints one line per event on stdout:
//
//	ready <clientId> <sessionId>   resources are registered (sessionId is 0 for sub/idle)
//	sent <seq>                     every -report messages published
//	recv <seq>                     every -report messages received in order
//	image <sessionId>              an image became available (sub)
//	gap <expected> <got>           a sequence error was detected (sub)
//	terminal <type> <message>      the client terminated (the error type as %T); exit code 3
//
// Payloads match docker/interop InteropTool: little-endian int64 sequence followed by byte(seq*31+j).
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
)

func main() {
	mode := flag.String("mode", "pub", "pub, sub or idle")
	dir := flag.String("dir", "/dev/shm/aeron-adv-conductor-faults", "aeron dir")
	channel := flag.String("channel", "aeron:ipc", "channel")
	stream := flag.Int("stream", 9800, "stream id")
	exclusive := flag.Bool("exclusive", false, "use an ExclusivePublication")
	claim := flag.Bool("claim", false, "publish with TryClaim")
	size := flag.Int("size", 64, "message size")
	interval := flag.Duration("interval", 100*time.Microsecond, "delay between messages")
	report := flag.Int64("report", 100, "report progress every n messages")
	driverTimeout := flag.Duration("driver-timeout", 3*time.Second, "media driver timeout")
	flag.Parse()

	terminal := make(chan error, 1)
	ctx := aeron.NewContext().AeronDir(*dir).MediaDriverTimeout(*driverTimeout).
		ErrorHandler(func(err error) {
			select {
			case terminal <- err:
			default:
			}
		}).
		AvailableImageHandler(func(img aeron.Image) { fmt.Printf("image %d\n", img.SessionID()) })
	a, err := aeron.Connect(ctx)
	if err != nil {
		fmt.Printf("terminal %T %v\n", err, err)
		os.Exit(3)
	}
	die := func() {
		for !a.IsClosed() {
			select {
			case <-terminal:
			case <-time.After(10 * time.Millisecond):
			}
		}
		err := a.TerminalError()
		fmt.Printf("terminal %T %v\n", err, err)
		os.Exit(3)
	}

	switch *mode {
	case "idle":
		fmt.Printf("ready %d 0\n", a.ClientID())
		die()
	case "sub":
		sub, err := a.AddSubscription(*channel, int32(*stream))
		if err != nil {
			fmt.Printf("terminal %T %v\n", err, err)
			os.Exit(3)
		}
		fmt.Printf("ready %d 0\n", a.ClientID())
		next := int64(0)
		handler := func(buf *atomic.Buffer, offset, length int32, _ *logbuffer.Header) {
			seq := buf.GetInt64(offset)
			if seq != next {
				fmt.Printf("gap %d %d\n", next, seq)
			}
			next = seq + 1
			if next%*report == 0 {
				fmt.Printf("recv %d\n", seq)
			}
		}
		for !a.IsClosed() {
			if sub.Poll(handler, 64) == 0 {
				time.Sleep(100 * time.Microsecond)
			}
		}
		die()
	default:
		var pub aeron.Publisher
		if *exclusive {
			pub, err = a.AddExclusivePublication(*channel, int32(*stream))
		} else {
			pub, err = a.AddPublication(*channel, int32(*stream))
		}
		if err != nil {
			fmt.Printf("terminal %T %v\n", err, err)
			os.Exit(3)
		}
		fmt.Printf("ready %d %d\n", a.ClientID(), pub.SessionID())
		msg := make([]byte, *size)
		buf := atomic.MakeBuffer(msg)
		var bc logbuffer.Claim
		for seq := int64(0); !a.IsClosed(); {
			Encode(msg, seq)
			var ret int64
			if *claim {
				if ret = tryClaim(pub, int32(len(msg)), &bc); ret > 0 {
					bc.Buffer().PutBytesArray(bc.Offset(), &msg, 0, int32(len(msg)))
					bc.Commit()
				}
			} else {
				ret = pub.Offer(buf, 0, int32(len(msg)), nil)
			}
			if ret < 0 {
				time.Sleep(100 * time.Microsecond)
				continue
			}
			if seq%*report == 0 {
				fmt.Printf("sent %d\n", seq)
			}
			seq++
			if *interval > 0 {
				time.Sleep(*interval)
			}
		}
		die()
	}
}

func tryClaim(pub aeron.Publisher, length int32, bc *logbuffer.Claim) int64 {
	switch p := pub.(type) {
	case *aeron.ExclusivePublication:
		return p.TryClaim(length, bc)
	case *aeron.Publication:
		return p.TryClaim(length, bc)
	}
	return aeron.PublicationClosed
}

// Encode writes seq as a little-endian int64 followed by the InteropTool fill pattern.
func Encode(msg []byte, seq int64) {
	binary.LittleEndian.PutUint64(msg, uint64(seq))
	for j := 8; j < len(msg); j++ {
		msg[j] = byte(seq*31 + int64(j))
	}
}
