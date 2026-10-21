// Command rlhelper is a killable Aeron client used by the resource-limits adversarial tests: they run it as a child
// process and SIGKILL/SIGSTOP it while it publishes into back pressure, holds an uncommitted claim, consumes, or owns
// many publications. It reports progress as single "event key=value ..." lines on stdout.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lirm/aeron-go/aeron"
	aeronatomic "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
)

func main() {
	mode := flag.String("mode", "pub", "pub | claim | sub | pubs | fill")
	dir := flag.String("dir", "", "aeron dir")
	channel := flag.String("channel", "aeron:ipc", "channel")
	stream := flag.Int("stream", 9900, "stream id")
	exclusive := flag.Bool("exclusive", false, "exclusive publication")
	size := flag.Int("size", 256, "message length (>= 8)")
	count := flag.Int("count", 1000, "number of publications for -mode=pubs")
	stallAfter := flag.Int64("stall-after", -1, "sub: stop polling after this many messages (-1 never)")
	flag.Parse()

	ctx := aeron.NewContext().AeronDir(*dir).MediaDriverTimeout(5 * time.Second).
		ErrorHandler(func(err error) { emit("error", "msg", fmt.Sprintf("%q", err.Error())) })
	a, err := aeron.Connect(ctx)
	if err != nil {
		fail(err)
	}
	emit("connected", "clientId", a.ClientID())
	switch *mode {
	case "pub":
		publish(a, *channel, int32(*stream), *exclusive, *size)
	case "claim":
		claim(a, *channel, int32(*stream), *size)
	case "sub":
		subscribe(a, *channel, int32(*stream), *stallAfter)
	case "pubs":
		pubs(a, *channel, int32(*stream), *count)
	case "fill":
		fill(a, *dir, *channel, int32(*stream), *count)
		_ = a.Close()
		emit("closed", "maps", mapped(*dir))
		return
	default:
		fail(fmt.Errorf("unknown mode %q", *mode))
	}
	select {}
}

func emit(event string, kv ...any) {
	s := event
	for i := 0; i+1 < len(kv); i += 2 {
		s += fmt.Sprintf(" %v=%v", kv[i], kv[i+1])
	}
	fmt.Println(s)
}

func fail(err error) {
	emit("fatal", "msg", fmt.Sprintf("%q", err.Error()))
	os.Exit(1)
}

// publish offers sequence-numbered messages forever, reporting the position each time a new result code is seen.
func publish(a *aeron.Aeron, channel string, stream int32, exclusive bool, size int) {
	var pub aeron.Publisher
	var err error
	if exclusive {
		pub, err = a.AddExclusivePublication(channel, stream)
	} else {
		pub, err = a.AddPublication(channel, stream)
	}
	if err != nil {
		fail(err)
	}
	emit("ready", "sessionId", pub.SessionID(), "registrationId", pub.RegistrationID())
	buf := aeronatomic.MakeBuffer(make([]byte, size))
	var seq int64
	last := int64(1)
	for {
		buf.PutInt64(0, seq)
		r := pub.Offer(buf, 0, int32(size), nil)
		if r > 0 {
			seq++
			r = 1
		}
		if r != last {
			emit("result", "code", aeron.PublicationErrorString(r), "seq", seq, "position", pub.Position())
			last = r
		}
		if r == aeron.PublicationClosed || r == aeron.MaxPositionExceeded {
			return
		}
		if r < 0 {
			time.Sleep(time.Millisecond)
		}
	}
}

// claim publishes one message, then claims a second one and never commits it, so the driver has to unblock the log.
func claim(a *aeron.Aeron, channel string, stream int32, size int) {
	pub, err := a.AddPublication(channel, stream)
	if err != nil {
		fail(err)
	}
	emit("ready", "sessionId", pub.SessionID(), "registrationId", pub.RegistrationID())
	deadline := time.Now().Add(10 * time.Second)
	for !pub.IsConnected() {
		if time.Now().After(deadline) {
			fail(fmt.Errorf("not connected"))
		}
		time.Sleep(time.Millisecond)
	}
	var bc logbuffer.Claim
	for i := int64(0); i < 2; {
		r := pub.TryClaim(int32(size), &bc)
		if r < 0 {
			time.Sleep(time.Millisecond)
			continue
		}
		bc.Buffer().PutInt64(bc.Offset(), i)
		if i == 0 {
			bc.Commit()
		}
		i++
	}
	emit("claimed", "position", pub.Position())
}

func subscribe(a *aeron.Aeron, channel string, stream int32, stallAfter int64) {
	sub, err := a.AddSubscriptionWithHandlers(channel, stream,
		func(img aeron.Image) { emit("available", "sessionId", img.SessionID()) },
		func(img aeron.Image) { emit("unavailable", "sessionId", img.SessionID()) })
	if err != nil {
		fail(err)
	}
	emit("ready", "registrationId", sub.RegistrationID())
	var n atomic.Int64
	handler := func(buffer *aeronatomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
		n.Add(1)
	}
	for {
		if stallAfter >= 0 && n.Load() >= stallAfter {
			emit("stalled", "count", n.Load())
			select {}
		}
		if sub.Poll(handler, 16) == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
}

// pubs adds count exclusive publications (one log buffer each) over 50 streams and keeps them open.
func pubs(a *aeron.Aeron, channel string, stream int32, count int) {
	for i := 0; i < count; i++ {
		if _, err := a.AddExclusivePublication(channel, stream+int32(i%50)); err != nil {
			fail(err)
		}
	}
	emit("ready", "count", count)
}

// fill adds exclusive publications (each with its own log buffer) until the driver rejects one, checks the rejected
// add leaves nothing mapped, then frees one log and requires a new publication to succeed and carry data.
func fill(a *aeron.Aeron, dir, channel string, stream int32, max int) {
	var added []*aeron.ExclusivePublication
	for i := 0; ; i++ {
		if i == max {
			fail(fmt.Errorf("driver accepted %d publications without running out of storage", max))
		}
		before := mapped(dir)
		pub, err := a.AddExclusivePublication(channel, stream+int32(i))
		if err == nil {
			added = append(added, pub)
			continue
		}
		var regErr *aeron.RegistrationError
		code := int32(-1)
		if errors.As(err, &regErr) {
			code = int32(regErr.Code)
		}
		emit("rejected", "added", len(added), "code", code, "storage", aeron.IsStorageSpaceError(err),
			"leakedMaps", mapped(dir)-before, "msg", fmt.Sprintf("%q", err.Error()))
		break
	}
	if len(added) == 0 {
		fail(fmt.Errorf("no publication fit"))
	}
	if err := added[0].Close(); err != nil {
		fail(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		pub, err := a.AddExclusivePublication(channel, stream+90)
		if err == nil {
			sub, err := a.AddSubscription(channel, stream+90)
			if err != nil {
				fail(err)
			}
			buf := aeronatomic.MakeBuffer(make([]byte, 64))
			for pub.Offer(buf, 0, 64, nil) < 0 {
				if time.Now().After(deadline) {
					fail(fmt.Errorf("recovered publication never accepted an offer"))
				}
				time.Sleep(time.Millisecond)
			}
			got := 0
			for got == 0 && time.Now().Before(deadline) {
				got = sub.Poll(func(*aeronatomic.Buffer, int32, int32, *logbuffer.Header) {}, 10)
			}
			emit("recovered", "received", got)
			return
		}
		if time.Now().After(deadline) {
			fail(fmt.Errorf("no recovery after freeing a log: %w", err))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// mapped counts this process's mappings of files under dir.
func mapped(dir string) int {
	maps, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		fail(err)
	}
	return strings.Count(string(maps), dir+"/")
}

var _ term.ReservedValueSupplier
