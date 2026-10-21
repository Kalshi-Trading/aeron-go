// Command crashclient is the Go peer of the adversarial publisher-crash suite: a publisher or subscriber the tests run
// as a child process and SIGKILL/SIGSTOP. It mirrors docker/adversarial-publisher-crash CrashTool.java (same flags as
// key=value pairs, same JSON events on stdout, same message format as e2e.EncodeMessage).
//
//	crashclient pub dir=<aeron dir> channel=<uri> stream=<id> [exclusive=true]
//	            [mode=offer|claim|offerblock|holdclaim|holdfrag] [size=<bytes>] [count=<n, -1 forever>] [from=<seq>]
//	            [intervalUs=<us>] [holdAfter=<n>] [holdFragment=<k>] [clientName=<name>] [dest=<uri>]...
//
// holdclaim takes a TryClaim for message holdAfter and never commits it. holdfrag blocks inside the
// ReservedValueSupplier of fragment holdFragment of the (fragmented) message holdAfter, so fragments before it are
// committed and the rest of the message is never written. Both then wait to be killed.
//
//	crashclient sub dir=<aeron dir> channel=<uri> stream=<id> [dest=<uri>]...
//
// stdin commands: "report" (sub: per-session stats), "exit".
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/e2e"
)

var (
	args  = map[string]string{}
	dests []string
	stdin = make(chan string, 16)
	outMu sync.Mutex
)

func main() {
	if len(os.Args) < 2 {
		fatal(errors.New("usage: crashclient pub|sub key=value..."))
	}
	for _, kv := range os.Args[2:] {
		k, v, _ := strings.Cut(kv, "=")
		if k == "dest" {
			dests = append(dests, v)
		} else {
			args[k] = v
		}
	}
	go func() {
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			stdin <- strings.TrimSpace(s.Text())
		}
		stdin <- "EOF"
	}()
	ctx := aeron.NewContext().AeronDir(str("dir", "/dev/shm/aeron-adv-publisher-crash")).
		ClientName(str("clientName", "crashclient")).
		MediaDriverTimeout(5 * time.Second).
		ErrorHandler(func(err error) { emit("error", "type", errType(err), "message", err.Error()) }).
		AvailableImageHandler(func(image aeron.Image) {
			emit("image", "sessionId", image.SessionID(), "joinPosition", image.JoinPosition(),
				"correlationId", image.CorrelationID())
		}).
		UnavailableImageHandler(func(image aeron.Image) {
			emit("imageGone", "sessionId", image.SessionID(), "position", image.Position(),
				"correlationId", image.CorrelationID())
		})
	a, err := aeron.Connect(ctx)
	if err != nil {
		fatal(err)
	}
	switch os.Args[1] {
	case "pub":
		err = pub(a)
	case "sub":
		err = sub(a)
	default:
		err = fmt.Errorf("unknown mode %s", os.Args[1])
	}
	if err != nil {
		if a.IsClosed() && a.TerminalError() != nil {
			emit("terminated", "type", errType(a.TerminalError()), "message", a.TerminalError().Error())
		}
		fatal(err)
	}
	_ = a.Close()
}

func pub(a *aeron.Aeron) error {
	channel, stream := str("channel", ""), integer("stream", 0)
	mode, size := str("mode", "offer"), int32(integer("size", 64))
	count, from := integer("count", -1), integer("from", 0)
	interval := time.Duration(integer("intervalUs", 0)) * time.Microsecond
	holdAfter := integer("holdAfter", -1)
	holdFragment := integer("holdFragment", 1)

	var pub aeron.Publisher
	var excl *aeron.ExclusivePublication
	var err error
	if str("exclusive", "false") == "true" || mode == "offerblock" {
		excl, err = a.AddExclusivePublication(channel, int32(stream))
		pub = excl
	} else {
		pub, err = a.AddPublication(channel, int32(stream))
	}
	if err != nil {
		return err
	}
	for _, d := range dests {
		if err := pub.AddDestination(d); err != nil {
			return err
		}
	}
	emit("ready", "sessionId", pub.SessionID(), "registrationId", pub.RegistrationID(), "clientId", a.ClientID(),
		"initialTermId", pub.InitialTermID(), "termLength", pub.TermBufferLength(), "position", pub.Position(),
		"maxPayloadLength", pub.MaxPayloadLength())
	if err := e2e.WaitConnected(pub, 30*time.Second); err != nil {
		return err
	}
	emit("connected", "sessionId", pub.SessionID(), "position", pub.Position())

	msg := make([]byte, size)
	buf := ab.MakeBuffer(msg)
	block := make([]byte, util.AlignInt32(size+logbuffer.DataFrameHeader.Length, logbuffer.FrameAlignment))
	blockBuf := ab.MakeBuffer(block)
	var claim logbuffer.Claim
	var lastProgress time.Time
	sent := int64(0)
	for seq := from; count < 0 || seq < from+count; seq++ {
		if mode == "holdclaim" && sent == holdAfter {
			var result int64
			for result = pub.TryClaim(size, &claim); result <= 0; result = pub.TryClaim(size, &claim) {
				if err := checkResult(a, result); err != nil {
					return err
				}
			}
			claim.PutBytes(ab.MakeBuffer(claimBytes(seq, size)), 0, size)
			emit("holding", "seq", seq, "position", result)
			select {}
		}
		for {
			var result int64
			switch mode {
			case "offer", "holdclaim":
				e2e.EncodeMessage(msg, seq)
				result = pub.Offer(buf, 0, size, nil)
			case "holdfrag":
				e2e.EncodeMessage(msg, seq)
				var supplier term.ReservedValueSupplier
				if sent == holdAfter {
					fragment := int64(0)
					supplier = func(_ *ab.Buffer, termOffset int32, _ int32) int64 {
						if fragment == holdFragment {
							emit("holding", "seq", seq, "termOffset", termOffset, "position", pub.Position())
							select {}
						}
						fragment++
						return 0
					}
				}
				result = pub.Offer(buf, 0, size, supplier)
			case "claim":
				if result = pub.TryClaim(size, &claim); result > 0 {
					claim.PutBytes(ab.MakeBuffer(claimBytes(seq, size)), 0, size)
					claim.Commit()
				}
			case "offerblock":
				result = offerBlock(excl, block, blockBuf, msg, seq, size)
			default:
				return fmt.Errorf("unknown pub mode %s", mode)
			}
			if result > 0 {
				break
			}
			if err := checkResult(a, result); err != nil {
				return err
			}
		}
		sent++
		if time.Since(lastProgress) >= 20*time.Millisecond {
			lastProgress = time.Now()
			emit("progress", "next", seq+1, "position", pub.Position())
		}
		if interval > 0 {
			time.Sleep(interval)
		}
		select {
		case cmd := <-stdin:
			if cmd == "exit" {
				emit("done", "next", from+sent, "position", pub.Position())
				return pub.Close()
			}
		default:
		}
	}
	emit("done", "next", from+sent, "position", pub.Position())
	for cmd := range stdin {
		if cmd == "exit" || cmd == "EOF" {
			break
		}
	}
	return pub.Close()
}

func claimBytes(seq int64, size int32) []byte {
	b := make([]byte, size)
	e2e.EncodeMessage(b, seq)
	return b
}

// offerBlock frames one message by hand at the publication's current term offset and offers it with OfferBlock,
// like a log replicator would. Term ends are padded with AppendPadding.
func offerBlock(pub *aeron.ExclusivePublication, block []byte, blockBuf *ab.Buffer, msg []byte, seq int64,
	size int32) int64 {
	frameLength := size + logbuffer.DataFrameHeader.Length
	if pub.TermOffset()+int32(len(block)) > pub.TermBufferLength() {
		if pub.TermOffset() >= pub.TermBufferLength() {
			return pub.OfferBlock(blockBuf, 0, 0)
		}
		if r := pub.AppendPadding(pub.TermBufferLength() - pub.TermOffset() - logbuffer.DataFrameHeader.Length); r < 0 {
			return r
		}
	}
	for i := range block {
		block[i] = 0
	}
	h := &logbuffer.DataFrameHeader
	blockBuf.PutInt32(h.FrameLengthFieldOffset, frameLength)
	blockBuf.PutUInt8(h.VersionFieldOffset, 0)
	blockBuf.PutUInt8(h.FlagsFieldOffset, 0xC0)
	blockBuf.PutUInt16(h.TypeFieldOffset, h.TypeData)
	blockBuf.PutInt32(h.TermOffsetFieldOffset, pub.TermOffset())
	blockBuf.PutInt32(h.SessionIDFieldOffset, pub.SessionID())
	blockBuf.PutInt32(h.StreamIDFieldOffset, pub.StreamID())
	blockBuf.PutInt32(h.TermIDFieldOffset, pub.TermID())
	e2e.EncodeMessage(msg, seq)
	copy(block[h.DataOffset:], msg)
	return pub.OfferBlock(blockBuf, 0, int32(len(block)))
}

func checkResult(a *aeron.Aeron, result int64) error {
	switch result {
	case aeron.NotConnected, aeron.BackPressured, aeron.AdminAction:
		return nil
	}
	// Give the conductor time to report why the publication closed through the error handler before exiting.
	time.Sleep(time.Second)
	return fmt.Errorf("offer failed: %d", result)
}

type stats struct {
	First, Next, Received, Gaps, Duplicates, Corrupt, LastPosition int64
}

func sub(a *aeron.Aeron) error {
	sub, err := a.AddSubscription(str("channel", ""), int32(integer("stream", 0)))
	if err != nil {
		return err
	}
	for _, d := range dests {
		if err := sub.AddDestination(d); err != nil {
			return err
		}
	}
	emit("ready", "registrationId", sub.RegistrationID(), "clientId", a.ClientID())
	sessions := map[int32]*stats{}
	assembler := aeron.NewFragmentAssembler(func(buffer *ab.Buffer, offset, length int32, header *logbuffer.Header) {
		s := sessions[header.SessionID()]
		if s == nil {
			s = &stats{First: -1, Next: -1}
			sessions[header.SessionID()] = s
		}
		s.Received++
		s.LastPosition = header.Position()
		seq, err := e2e.DecodeMessage(buffer.GetBytesArray(offset, length))
		switch {
		case err != nil:
			s.Corrupt++
		case s.First < 0:
			s.First, s.Next = seq, seq+1
		case seq == s.Next:
			s.Next++
		case seq > s.Next:
			s.Gaps++
			s.Next = seq + 1
		default:
			s.Duplicates++
		}
	}, aeron.DefaultFragmentAssemblyBufferLength)
	var total int64
	var lastProgress time.Time
	for {
		if sub.Poll(assembler.OnFragment, 64) == 0 {
			time.Sleep(100 * time.Microsecond)
		}
		if sub.IsClosed() {
			return errors.New("subscription closed")
		}
		select {
		case cmd := <-stdin:
			switch cmd {
			case "report":
				ids := make([]int, 0, len(sessions))
				for id := range sessions {
					ids = append(ids, int(id))
				}
				sort.Ints(ids)
				for _, id := range ids {
					s := sessions[int32(id)]
					emit("session", "sessionId", id, "first", s.First, "next", s.Next, "received", s.Received,
						"gaps", s.Gaps, "duplicates", s.Duplicates, "corrupt", s.Corrupt, "lastPosition", s.LastPosition)
				}
				emit("reportEnd", "images", sub.ImageCount())
			case "exit", "EOF":
				return sub.Close()
			}
		default:
		}
		if time.Since(lastProgress) >= 100*time.Millisecond {
			lastProgress = time.Now()
			var received int64
			for _, s := range sessions {
				received += s.Received
			}
			if received != total {
				total = received
				emit("progress", "received", total)
			}
		}
	}
}

func errType(err error) string {
	t := reflect.TypeOf(err)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Name()
}

func str(key, dflt string) string {
	if v, ok := args[key]; ok {
		return v
	}
	if dflt == "" {
		fatal(fmt.Errorf("missing %s=", key))
	}
	return dflt
}

func integer(key string, dflt int64) int64 {
	v, err := strconv.ParseInt(str(key, strconv.FormatInt(dflt, 10)), 10, 64)
	if err != nil {
		fatal(err)
	}
	return v
}

func emit(event string, kv ...interface{}) {
	m := map[string]interface{}{"event": event}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i].(string)] = kv[i+1]
	}
	b, _ := json.Marshal(m)
	outMu.Lock()
	_, _ = os.Stdout.Write(append(b, '\n'))
	outMu.Unlock()
}

func fatal(err error) {
	emit("fatal", "type", errType(err), "message", err.Error())
	os.Exit(1)
}
