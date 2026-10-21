package examples

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lirm/aeron-go/aeron"
	aatomic "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/aeron/logging"
)

var aeronLoggers = []string{"aeron", "memmap", "driver", "counters", "logbuffers", "buffer", "rb", "examples"}

// SetupLogging sets library loggers to INFO, or DEBUG when -l is given.
func SetupLogging() {
	level := logging.INFO
	if *ExamplesConfig.LoggingOn {
		level = logging.DEBUG
	}
	for _, name := range aeronLoggers {
		logging.SetLevel(level, name)
	}
}

// NewContext returns an aeron.Context configured from the shared flags.
func NewContext() *aeron.Context {
	to := time.Duration(*ExamplesConfig.DriverTo) * time.Millisecond
	return aeron.NewContext().AeronDir(*ExamplesConfig.AeronPrefix).MediaDriverTimeout(to)
}

// SignalContext returns a context cancelled on SIGINT/SIGTERM (Java ShutdownSignalBarrier).
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// NewIdleStrategy builds the idle strategy named by -idle.
func NewIdleStrategy() idlestrategy.Idler {
	return IdleStrategyByName(*ExamplesConfig.IdleStrategy)
}

// IdleStrategyByName maps busy|yield|sleep|backoff (or Agrona class names) to an Idler.
func IdleStrategyByName(name string) idlestrategy.Idler {
	switch n := strings.ToLower(name); {
	case strings.Contains(n, "yield"):
		return idlestrategy.Yielding{}
	case strings.Contains(n, "sleep"):
		return idlestrategy.Sleeping{SleepFor: time.Millisecond}
	case strings.Contains(n, "backoff"):
		return idlestrategy.NewDefaultBackoffIdleStrategy()
	default:
		return idlestrategy.Busy{}
	}
}

// OfferResultString describes a negative Publication.Offer result the way the Java samples do.
func OfferResultString(result int64) string {
	switch result {
	case aeron.BackPressured:
		return "Offer failed due to back pressure"
	case aeron.NotConnected:
		return "Offer failed because publisher is not connected to a subscriber"
	case aeron.AdminAction:
		return "Offer failed because of an administration action in the system"
	case aeron.PublicationClosed:
		return "Offer failed because publication is closed"
	case aeron.MaxPositionExceeded:
		return "Offer failed due to publication reaching its max position"
	default:
		return fmt.Sprintf("Offer failed due to unknown reason: %d", result)
	}
}

// IsTerminalOfferResult reports whether retrying an offer with this result is pointless.
func IsTerminalOfferResult(result int64) bool {
	return result == aeron.PublicationClosed || result == aeron.MaxPositionExceeded
}

// PrintAvailableImage mirrors SamplesUtil.printAvailableImage.
func PrintAvailableImage(image aeron.Image) {
	channel, streamID := imageSubscription(image)
	fmt.Printf("Available image on %s streamId=%d sessionId=%d mtu=%d term-length=%d from %s\n",
		channel, streamID, image.SessionID(), image.MtuLength(), image.TermBufferLength(), image.SourceIdentity())
}

// PrintUnavailableImage mirrors SamplesUtil.printUnavailableImage.
func PrintUnavailableImage(image aeron.Image) {
	channel, streamID := imageSubscription(image)
	fmt.Printf("Unavailable image on %s streamId=%d sessionId=%d\n", channel, streamID, image.SessionID())
}

func imageSubscription(image aeron.Image) (string, int32) {
	if sub := image.Subscription(); sub != nil {
		return sub.Channel(), sub.StreamID()
	}
	return "", aeron.NullValue
}

// PrintStringMessage mirrors SamplesUtil.printAsciiMessage.
func PrintStringMessage(streamID int32) term.FragmentHandler {
	return func(buffer *aatomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
		fmt.Printf("Message to stream %d from session %d (%d@%d) <<%s>>\n",
			streamID, header.SessionId(), length, offset, string(buffer.GetBytesArray(offset, length)))
	}
}

// LengthGenerator returns a fixed or random (8..max) message length generator.
func LengthGenerator(random bool, max int) func() int32 {
	if random && max > 8 {
		return func() int32 { return int32(8 + rand.Intn(max-8)) }
	}
	return func() int32 { return int32(max) }
}

// RateReporter mirrors io.aeron.samples.RateReporter: OnMessage is safe to call from the hot path
// and Run reports deltas every interval until ctx is done.
type RateReporter struct {
	interval      time.Duration
	report        func(messagesPerSec, bytesPerSec float64, totalMessages, totalBytes int64)
	totalBytes    atomic.Int64
	totalMessages atomic.Int64
}

// NewRateReporter creates a RateReporter; report defaults to PrintRate when nil.
func NewRateReporter(interval time.Duration,
	report func(messagesPerSec, bytesPerSec float64, totalMessages, totalBytes int64)) *RateReporter {
	if report == nil {
		report = PrintRate
	}
	return &RateReporter{interval: interval, report: report}
}

// OnMessage records one message of the given length.
func (r *RateReporter) OnMessage(length int64) {
	r.totalBytes.Add(length)
	r.totalMessages.Add(1)
}

// TotalMessages returns the number of messages recorded so far.
func (r *RateReporter) TotalMessages() int64 { return r.totalMessages.Load() }

// TotalBytes returns the number of bytes recorded so far.
func (r *RateReporter) TotalBytes() int64 { return r.totalBytes.Load() }

// Handler returns a fragment handler that records every fragment.
func (r *RateReporter) Handler() term.FragmentHandler {
	return func(_ *aatomic.Buffer, _ int32, length int32, _ *logbuffer.Header) { r.OnMessage(int64(length)) }
}

// Run reports until ctx is cancelled.
func (r *RateReporter) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	last := time.Now()
	var lastBytes, lastMessages int64
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			bytes, msgs := r.totalBytes.Load(), r.totalMessages.Load()
			span := now.Sub(last).Seconds()
			if span <= 0 {
				continue
			}
			r.report(float64(msgs-lastMessages)/span, float64(bytes-lastBytes)/span, msgs, bytes)
			last, lastBytes, lastMessages = now, bytes, msgs
		}
	}
}

// PrintRate mirrors SamplesUtil.printRate.
func PrintRate(messagesPerSec, bytesPerSec float64, totalMessages, totalBytes int64) {
	fmt.Printf("%.04g msgs/sec, %.04g payload bytes/sec, totals %d messages %d MB\n",
		messagesPerSec, bytesPerSec, totalMessages, totalBytes/(1024*1024))
}

// SubscriberLoop polls sub with a fragment assembler until ctx is done or done() returns true.
func SubscriberLoop(ctx context.Context, sub *aeron.Subscription, handler term.FragmentHandler,
	limit int, idler idlestrategy.Idler, done func() bool) {
	assembler := aeron.NewFragmentAssembler(handler, aeron.DefaultFragmentAssemblyBufferLength)
	for ctx.Err() == nil && (done == nil || !done()) {
		idler.Idle(sub.Poll(assembler.OnFragment, limit))
	}
}

// Linger sleeps for -linger ms if set.
func Linger() {
	if ms := *ExamplesConfig.LingerMs; ms > 0 {
		fmt.Printf("Lingering for %d milliseconds...\n", ms)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
