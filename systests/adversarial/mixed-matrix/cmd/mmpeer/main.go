// Command mmpeer is the killable Go client process of systests/adversarial/mixed-matrix. It speaks the same
// arguments, wire format and JSON line events as the Java MatrixPeer (docker/adversarial-mixed-matrix):
//
//	mmpeer pub channel=... stream=N kind=concurrent|exclusive|tryclaim|offerblock writer=N sizes=a,b rateUs=N
//	           [dest=...] [hangAfter=N: after N messages claim a frame, never commit it and hang]
//	mmpeer sub channel=... stream=N kind=poll|controlled [dest=...]
//
// stdin "close" closes gracefully; the tests also SIGKILL and SIGSTOP it.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/systests/adversarial/mixed-matrix/internal/mm"
)

var out = mm.NewEmitter(os.Stdout)

func main() {
	args := map[string]string{}
	var dests []string
	for _, a := range os.Args[2:] {
		k, v, _ := strings.Cut(a, "=")
		if k == "dest" {
			dests = append(dests, v)
		} else {
			args[k] = v
		}
	}
	var closing atomic.Bool
	go func() {
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
			if strings.TrimSpace(sc.Text()) == "close" {
				closing.Store(true)
			}
		}
	}()
	get := func(k, def string) string {
		if v, ok := args[k]; ok {
			return v
		}
		return def
	}
	timeoutMs, _ := strconv.Atoi(get("driverTimeoutMs", "3000"))
	ctx := aeron.NewContext().AeronDir(get("dir", "/dev/shm/aeron-adv-mixed-matrix")).
		MediaDriverTimeout(time.Duration(timeoutMs) * time.Millisecond).
		ErrorHandler(func(err error) { out.Emit("clientError", "message", fmt.Sprintf("%T: %v", err, err)) })
	a, err := aeron.Connect(ctx)
	if err != nil {
		fail(err)
	}
	stream, _ := strconv.Atoi(get("stream", "9700"))
	if os.Args[1] == "pub" {
		err = pub(a, get, int32(stream), dests, &closing)
	} else {
		err = sub(a, get, int32(stream), dests, &closing)
	}
	if err != nil {
		fail(err)
	}
	if err := a.Close(); err != nil {
		fail(err)
	}
}

func fail(err error) {
	out.Emit("terminated", "error", fmt.Sprintf("%T: %v", errors.Unwrap(err), err))
	os.Exit(3)
}

func pub(a *aeron.Aeron, get func(string, string) string, stream int32, dests []string, closing *atomic.Bool) error {
	writer, _ := strconv.Atoi(get("writer", "1"))
	rateUs, _ := strconv.Atoi(get("rateUs", "200"))
	p, err := mm.AddPub(a, get("kind", mm.PubConcurrent), get("channel", ""), stream, uint8(writer),
		mm.ParseSizes(get("sizes", "64")))
	if err != nil {
		return err
	}
	for _, d := range dests {
		if err := p.Pub.AddDestination(d); err != nil {
			return err
		}
	}
	out.Emit("ready", "sessionId", p.Pub.SessionID(), "maxPayloadLength", p.Pub.MaxPayloadLength(),
		"maxMessageLength", p.Pub.MaxMessageLength())
	hangAfter, _ := strconv.Atoi(get("hangAfter", "-1"))
	firstPosition := int64(-1)
	for !closing.Load() {
		if hangAfter >= 0 && int(p.Next()) >= hangAfter {
			// claim a frame and never commit it, then wait to be SIGKILLed: the driver must unblock the log
			var claim logbuffer.Claim
			for {
				if result := p.Pub.TryClaim(64, &claim); result > 0 {
					break
				} else if !mm.Retryable(result) {
					return fmt.Errorf("tryClaim: %d", result)
				}
			}
			out.Emit("claimed", "next", p.Next())
			select {}
		}
		before := p.Pub.Position()
		n, err := p.Step()
		if n > 0 && firstPosition < 0 {
			firstPosition = before
		}
		if err != nil {
			return err
		}
		if n > 0 && rateUs > 0 {
			time.Sleep(time.Duration(rateUs) * time.Microsecond)
		} else if n == 0 {
			time.Sleep(20 * time.Microsecond)
		}
	}
	out.Emit("closed", "writer", writer, "next", p.Next(), "sessionId", p.Pub.SessionID(),
		"firstPosition", firstPosition)
	return p.Close()
}

func sub(a *aeron.Aeron, get func(string, string) string, stream int32, dests []string, closing *atomic.Bool) error {
	s, err := mm.AddSub(a, get("kind", mm.SubPoll), get("channel", ""), stream, rand.New(rand.NewSource(1)), false,
		dests...)
	if err != nil {
		return err
	}
	out.Emit("ready", "registrationId", s.Sub.RegistrationID())
	for !closing.Load() {
		if s.Poll() == 0 {
			if s.Sub.IsClosed() {
				return errors.New("subscription closed")
			}
			time.Sleep(50 * time.Microsecond)
		}
	}
	for _, st := range s.Tracker.Images() {
		first := ""
		if len(st.Errors) > 0 {
			first = st.Errors[0]
		}
		out.Emit("report", "sessionId", st.SessionID, "correlationId", st.CorrelationID, "messages", st.Messages,
			"errors", len(st.Errors), "firstError", first, "writers", st.WriterSummary(), "eos", st.EOS)
	}
	out.Emit("done")
	return s.Close()
}
