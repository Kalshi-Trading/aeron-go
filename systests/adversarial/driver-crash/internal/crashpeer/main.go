// Command crashpeer is a Go Aeron client run as a child process by the adversarial driver-crash tests, so they can
// SIGKILL or SIGSTOP a real client process. It mirrors docker/adversarial-driver-crash/JavaPeer.java: it publishes
// and/or subscribes and reports every error-handler call, close-handler call and image event as JSON lines on stdout.
//
// Roles: pub, sub, pubsub, idle, claimhang (TryClaim a frame and never commit it), adds (add/remove publications and
// subscriptions in a loop).
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/e2e"
)

var outMu sync.Mutex

func emit(event string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["event"] = event
	fields["lang"] = "go"
	fields["tMs"] = time.Now().UnixMilli()
	b, _ := json.Marshal(fields)
	outMu.Lock()
	fmt.Println(string(b))
	outMu.Unlock()
}

// classify maps a Go client error to the simple name of the Java 1.53.3 exception it corresponds to.
func classify(err error) string {
	var dt *aeron.DriverTimeoutError
	var ct *aeron.ClientTimeoutError
	var st *aeron.ConductorServiceTimeoutError
	switch {
	case errors.As(err, &dt):
		return "DriverTimeout"
	case errors.As(err, &ct):
		return "ClientTimeout"
	case errors.As(err, &st):
		return "ConductorServiceTimeout"
	}
	return fmt.Sprintf("%T", err)
}

func main() {
	dir := flag.String("dir", "/dev/shm/aeron-adv-driver-crash", "aeron dir")
	channel := flag.String("channel", "aeron:ipc", "channel")
	stream := flag.Int("stream", 9000, "stream id")
	role := flag.String("role", "pubsub", "pub|sub|pubsub|idle|claimhang|adds")
	exclusive := flag.Bool("exclusive", false, "use an exclusive publication")
	claim := flag.Bool("claim", false, "publish with TryClaim")
	size := flag.Int("size", 64, "message length")
	interval := flag.Duration("interval", time.Millisecond, "pause between messages")
	driverTimeout := flag.Duration("driverTimeout", 2*time.Second, "media driver timeout")
	linger := flag.Duration("linger", 500*time.Millisecond, "stay alive this long after the client terminated")
	flag.Parse()

	ctx := aeron.NewContext().AeronDir(*dir).MediaDriverTimeout(*driverTimeout).
		ErrorHandler(func(err error) { emit("error", map[string]any{"type": classify(err), "message": err.Error()}) }).
		AvailableImageHandler(func(img aeron.Image) { emit("available", map[string]any{"sessionId": img.SessionID()}) }).
		UnavailableImageHandler(func(img aeron.Image) {
			emit("unavailable", map[string]any{"sessionId": img.SessionID()})
		})
	a, err := aeron.Connect(ctx)
	if err != nil {
		emit("exception", map[string]any{"type": classify(err), "message": err.Error()})
		os.Exit(2)
	}
	a.AddCloseHandler(func() { emit("closed", nil) })

	var pub aeron.Publisher
	var sub *aeron.Subscription
	if strings.Contains(*role, "sub") {
		if sub, err = a.AddSubscription(*channel, int32(*stream)); err != nil {
			emit("exception", map[string]any{"type": classify(err), "message": err.Error()})
			os.Exit(2)
		}
	}
	if strings.Contains(*role, "pub") || *role == "claimhang" {
		if *exclusive {
			pub, err = a.AddExclusivePublication(*channel, int32(*stream))
		} else {
			pub, err = a.AddPublication(*channel, int32(*stream))
		}
		if err != nil {
			emit("exception", map[string]any{"type": classify(err), "message": err.Error()})
			os.Exit(2)
		}
	}
	sessionID := int32(0)
	if pub != nil {
		sessionID = pub.SessionID()
	}
	emit("ready", map[string]any{"clientId": a.ClientID(), "sessionId": sessionID})

	msg := make([]byte, *size)
	buf := atomic.MakeBuffer(msg)
	var bufClaim logbuffer.Claim
	var seq, received int64
	handler := func(*atomic.Buffer, int32, int32, *logbuffer.Header) { received++ }
	nextProgress := time.Now()
	lastOffer := ""
	for !a.IsClosed() {
		switch *role {
		case "claimhang":
			if pub.IsConnected() && pub.TryClaim(int32(*size), &bufClaim) > 0 {
				emit("claimed", map[string]any{"position": pub.Position()})
				select {} // never commit; the test SIGKILLs this process
			}
		case "adds":
			p, err := a.AddPublication(*channel, int32(*stream))
			if err == nil {
				s, err2 := a.AddSubscription(*channel, int32(*stream))
				if err2 == nil {
					_ = s.Close()
				}
				_ = p.Close()
				seq++
			}
		default:
			if pub != nil {
				var r int64
				if *claim {
					if r = pub.TryClaim(int32(*size), &bufClaim); r > 0 {
						e2e.EncodeMessage(msg, seq)
						bufClaim.Buffer().PutBytesArray(bufClaim.Offset(), &msg, 0, int32(*size))
						bufClaim.Commit()
					}
				} else {
					e2e.EncodeMessage(msg, seq)
					r = pub.Offer(buf, 0, int32(*size), nil)
				}
				if r > 0 {
					seq++
				} else if r == aeron.PublicationClosed {
					lastOffer = "CLOSED"
				}
			}
			if sub != nil {
				sub.Poll(handler, 64)
			}
		}
		if time.Now().After(nextProgress) {
			emit("progress", map[string]any{"sent": seq, "received": received, "lastOffer": lastOffer})
			nextProgress = time.Now().Add(200 * time.Millisecond)
		}
		time.Sleep(*interval)
	}
	offerAfter := ""
	if pub != nil {
		offerAfter = fmt.Sprint(pub.Offer(buf, 0, int32(*size), nil))
	}
	terminal := ""
	if err := a.TerminalError(); err != nil {
		terminal = classify(err)
	}
	emit("terminated", map[string]any{"offerAfter": offerAfter, "sent": seq, "terminal": terminal})
	time.Sleep(*linger)
	_ = a.Close()
	emit("exit", nil)
}
