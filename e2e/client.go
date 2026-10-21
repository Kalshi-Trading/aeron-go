package e2e

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
)

// Env enables the docker-backed end-to-end tests across the repository.
const Env = "AERON_E2E"

// Default endpoints of the docker/docker-compose.yml archiving driver.
const (
	DefaultControlChannel = "aeron:udp?endpoint=localhost:8010"
	DefaultControlStream  = 10
)

// Enabled reports whether AERON_E2E=1.
func Enabled() bool {
	return os.Getenv(Env) == "1"
}

// AeronDir returns $AERON_DIR or the aeron-go default directory.
func AeronDir() string {
	if dir := os.Getenv("AERON_DIR"); dir != "" {
		return dir
	}
	return aeron.DefaultAeronDir + "/aeron-" + aeron.UserName
}

// NewContext returns a client context for aeronDir with a generous driver timeout.
func NewContext(aeronDir string) *aeron.Context {
	return aeron.NewContext().AeronDir(aeronDir).MediaDriverTimeout(10 * time.Second)
}

// Connect connects an Aeron client to the driver in aeronDir.
func Connect(aeronDir string) (*aeron.Aeron, error) {
	return aeron.Connect(NewContext(aeronDir))
}

// PublishOptions controls Publish.
type PublishOptions struct {
	From     int64         // first sequence number
	Count    int64         // number of messages
	Size     int           // message length, at least HeaderLength
	Interval time.Duration // pause between messages, 0 for none
	Timeout  time.Duration // overall deadline including back pressure retries
	// OnSent is called after each successful offer with the sequence just sent.
	OnSent func(seq int64)
}

func (o PublishOptions) withDefaults() PublishOptions {
	if o.Size < HeaderLength {
		o.Size = 64
	}
	if o.Timeout == 0 {
		o.Timeout = time.Minute
	}
	return o
}

// WaitConnected waits for pub to have at least one connected subscriber.
func WaitConnected(pub *aeron.Publication, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for !pub.IsConnected() {
		if time.Now().After(deadline) {
			return fmt.Errorf("publication %s stream %d not connected after %v", pub.Channel(), pub.StreamID(), timeout)
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

// Publish offers opts.Count sequenced messages on pub, retrying on back pressure, admin action and
// not connected until opts.Timeout.
func Publish(pub *aeron.Publication, opts PublishOptions) error {
	opts = opts.withDefaults()
	deadline := time.Now().Add(opts.Timeout)
	msg := make([]byte, opts.Size)
	buf := atomic.MakeBuffer(msg)
	for seq := opts.From; seq < opts.From+opts.Count; seq++ {
		EncodeMessage(msg, seq)
		for {
			ret := pub.Offer(buf, 0, int32(len(msg)), nil)
			if ret >= 0 {
				break
			}
			switch ret {
			case aeron.NotConnected, aeron.BackPressured, aeron.AdminAction:
			default:
				return fmt.Errorf("offer seq %d failed: %d", seq, ret)
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("offer seq %d timed out (last result %d)", seq, ret)
			}
			time.Sleep(100 * time.Microsecond)
		}
		if opts.OnSent != nil {
			opts.OnSent(seq)
		}
		if opts.Interval > 0 {
			time.Sleep(opts.Interval)
		}
	}
	return nil
}

// PollFunc polls up to fragmentLimit fragments into handler.
type PollFunc func(handler term.FragmentHandler, fragmentLimit int) (int, error)

// SubscriptionPoller adapts a subscription to PollFunc.
func SubscriptionPoller(sub *aeron.Subscription) PollFunc {
	return func(handler term.FragmentHandler, fragmentLimit int) (int, error) {
		return sub.Poll(handler, fragmentLimit), nil
	}
}

// ErrTimeout is returned by Consume when the expected messages did not arrive in time.
var ErrTimeout = errors.New("timed out")

// Consume polls until checker has seen sequence until-1 (i.e. checker.Next >= until), a sequence error
// is detected, or timeout elapses.
func Consume(poll PollFunc, checker *SequenceChecker, until int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	handler := checker.Handler()
	for checker.Next < until {
		n, err := poll(handler, 64)
		if err != nil {
			return err
		}
		if err := checker.Err(); err != nil {
			return err
		}
		if n == 0 {
			if time.Now().After(deadline) {
				return fmt.Errorf("%w waiting for seq %d: %s", ErrTimeout, until-1, checker)
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
	return checker.Err()
}

// TouchFile creates path (used as a docker compose healthcheck readiness marker). Empty path is a no-op.
func TouchFile(path string) error {
	if path == "" {
		return nil
	}
	return os.WriteFile(path, []byte("ready\n"), 0o644)
}

// ReassemblingPoller adapts a subscription to PollFunc, reassembling fragmented messages.
func ReassemblingPoller(sub *aeron.Subscription) PollFunc {
	var assembler *aeron.FragmentAssembler
	var current term.FragmentHandler
	return func(handler term.FragmentHandler, fragmentLimit int) (int, error) {
		if assembler == nil {
			assembler = aeron.NewFragmentAssembler(func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
				current(buffer, offset, length, header)
			}, aeron.DefaultFragmentAssemblyBufferLength)
		}
		current = handler
		return sub.Poll(assembler.OnFragment, fragmentLimit), nil
	}
}
