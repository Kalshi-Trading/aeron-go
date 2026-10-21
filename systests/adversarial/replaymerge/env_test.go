package replaymerge

import (
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/replaymerge"
	"github.com/lirm/aeron-go/systests/adversarial/replaymerge/internal/rmrun"
)

// env is a fresh stack with a recorded live publication on ports p.
type env struct {
	t       *testing.T
	c       *client
	arch    *archive.Archive
	p       ports
	stream  int32
	pub     *rmrun.Publisher
	session int32
	recID   int64
	sent    atomic.Int64
	stop    chan struct{}
	pubErr  chan error
}

// newEnv recreates the stack and records an in-process publication of kind on the ports starting at base.
func newEnv(t *testing.T, base int, stream int32, pc rmrun.PubConfig) *env {
	freshStack(t)
	e := &env{t: t, p: portsFor(base), stream: stream}
	e.c = connect(t)
	e.arch = connectArchive(t, e.c.Aeron)
	pc.StreamID = stream
	if pc.Channel == "" {
		pc.Channel = e.p.pubChannel("")
	}
	pub, err := rmrun.NewPublisher(e.c.Aeron, pc)
	require.NoError(t, err)
	e.pub = pub
	t.Cleanup(func() { _ = pub.Pub().Close() })
	e.session = pub.Pub().SessionID()
	e.recID = startRecording(t, e.arch, e.p, stream, e.session)
	return e
}

// publish runs the publisher in the background until stopped (or Count is reached).
func (e *env) publish() {
	e.stop = make(chan struct{})
	e.pubErr = make(chan error, 1)
	go func() { e.pubErr <- e.pub.Run(e.stop, rmrun.Nop, func(next int64) { e.sent.Store(next) }) }()
	e.t.Cleanup(e.stopPublishing)
}

func (e *env) stopPublishing() {
	if e.stop != nil {
		select {
		case <-e.stop:
		default:
			close(e.stop)
		}
		defer func() { e.stop = nil }()
		select {
		case <-e.pubErr:
		case <-time.After(10 * time.Second):
			buf := make([]byte, 1<<20)
			e.t.Errorf("publisher did not stop:\n%s", buf[:runtime.Stack(buf, true)])
		}
	}
}

func (e *env) awaitSent(n int64, timeout time.Duration) {
	e.t.Helper()
	awaitTrue(e.t, fmt.Sprintf("%d messages sent", n), timeout, func() bool { return e.sent.Load() >= n })
}

// mergeConfig is a ReplayMerge of the env recording from position 0 on the Go consumer ports.
func (e *env) mergeConfig(count int64) rmrun.MergeConfig {
	return rmrun.MergeConfig{RecordingID: e.recID, SessionID: e.session, StreamID: e.stream,
		ReplayDestination: e.p.replayDestination(), LiveDestination: e.p.liveDestination(), Count: count,
		Timeout: 90 * time.Second}
}

// oracleMerge starts the Java ReplayMerge oracle on its own live/replay ports (base+4, base+5).
func (e *env) oracleMerge(count int64, extra ...string) *child {
	args := []string{
		fmt.Sprintf("recordingId=%d", e.recID), fmt.Sprintf("sessionId=%d", e.session),
		fmt.Sprintf("stream=%d", e.stream), fmt.Sprintf("count=%d", count),
		fmt.Sprintf("replayDestination=aeron:udp?endpoint=localhost:%d", e.p.control+5),
		fmt.Sprintf("liveDestination=aeron:udp?endpoint=localhost:%d|control=localhost:%d", e.p.control+4,
			e.p.control),
		"timeoutMs=90000",
	}
	o := startOracle(e.t, "replaymerge", append(args, extra...)...)
	o.awaitEvent("ready", 60*time.Second)
	return o
}

// requireExact asserts that a Go merge delivered from..to-1 with no gaps, duplicates or corruption.
func requireExact(t *testing.T, r rmrun.MergeResult, to int64) {
	t.Helper()
	require.NoError(t, r.Err(), "sequence: %s", r.Checker)
	require.Zero(t, r.Gaps, "gaps: %s", r.Checker)
	require.Zero(t, r.Duplicates, "duplicates: %s", r.Checker)
	require.Zero(t, r.Corrupt, "corrupt: %s", r.Checker)
	require.Equal(t, to, r.Next, "next: %s", r.Checker)
}

// requireOracleExact asserts the same for a Java oracle result event.
func requireOracleExact(t *testing.T, res rmrun.Event, to int64) {
	t.Helper()
	require.Zero(t, num(res, "gaps"), "java %v", res)
	require.Zero(t, num(res, "duplicates"), "java %v", res)
	require.Zero(t, num(res, "corrupt"), "java %v", res)
	require.Equal(t, to, num(res, "next"), "java %v", res)
}

func failMessages(r rmrun.MergeResult) string {
	var s []string
	for _, err := range r.Failures {
		s = append(s, err.Error())
	}
	return strings.Join(s, "; ")
}

// atState returns an OnState hook that runs fault once when the merge first enters state.
func atState(state replaymerge.State, fault func()) func(replaymerge.State, *rmrun.Checker) {
	var done bool
	return func(s replaymerge.State, _ *rmrun.Checker) {
		if s == state && !done {
			done = true
			fault()
		}
	}
}
