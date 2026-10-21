package replaymerge

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lirm/aeron-go/archive/replaymerge"
	"github.com/lirm/aeron-go/systests/adversarial/replaymerge/internal/rmrun"
)

// TestControlledPollLargeFragmented merges a stream of messages up to 100 KB (fragmented over ~70 MTU frames)
// interleaved with small ones through ControlledPoll, side by side with Java.
func TestControlledPollLargeFragmented(t *testing.T) {
	requireAdversarial(t)
	const total = 10000
	// fc=min: under the default max flow control a merged consumer that stalls for a moment (GC, CPU) while the
	// archive keeps up falls more than a term behind and can never repair the loss (seen for both Java and Go).
	e := newEnv(t, 27400, 9420, rmrun.PubConfig{Count: total, Rate: 300, Sizes: []int{64, 100000, 64, 30000},
		Channel: portsFor(27400).pubChannel("|fc=min")})
	e.publish()
	e.awaitSent(1500, 30*time.Second)
	o := e.oracleMerge(total)
	cfg := e.mergeConfig(total)
	cfg.Controlled = true
	cfg.Timeout = 3 * time.Minute
	r := rmrun.RunReplayMerge(e.c.Aeron, cfg, rmrun.Nop)
	require.Empty(t, r.Failures, "%s (publisher sent %d)", failMessages(r), e.sent.Load())
	require.True(t, r.Merged)
	requireExact(t, r, total)
	requireOracleExact(t, o.result(2*time.Minute), total)
}

// TestHighRateSmallMessages merges while the publisher sends 64-byte messages as fast as it can (the live stream
// is faster than the replay can catch up until the publisher's flow control lets the replay through), for Go and
// Java side by side.
func TestHighRateSmallMessages(t *testing.T) {
	requireAdversarial(t)
	const total = 1500000
	e := newEnv(t, 27400, 9421, rmrun.PubConfig{Count: total, Rate: 100000, Sizes: []int{64, 32, 128}})
	e.publish()
	e.awaitSent(200000, 30*time.Second)
	o := e.oracleMerge(total)
	cfg := e.mergeConfig(total)
	cfg.Timeout = 2 * time.Minute
	r := rmrun.RunReplayMerge(e.c.Aeron, cfg, rmrun.Nop)
	require.Empty(t, r.Failures, "%s", failMessages(r))
	requireExact(t, r, total)
	res := o.result(2 * time.Minute)
	requireOracleExact(t, res, total)
	require.True(t, r.Merged)
	require.Equal(t, true, res["merged"], "java %v", res)
}

// TestMDCManualLive merges from a manual-control MDC publication that sends to an archive endpoint and to the
// ReplayMerge live endpoint (no control address on the live destination).
func TestMDCManualLive(t *testing.T) {
	requireAdversarial(t)
	const total = 30000
	freshStack(t)
	e := &env{t: t, p: portsFor(27400), stream: 9422}
	e.c = connect(t)
	e.arch = connectArchive(t, e.c.Aeron)
	pub, err := rmrun.NewPublisher(e.c.Aeron, rmrun.PubConfig{Channel: "aeron:udp?control-mode=manual|term-length=1m",
		StreamID: e.stream, Kind: "excl", Count: total, Rate: 3000, Sizes: []int{64, 1500, 64, 5000}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Pub().Close() })
	dests, ok := pub.Pub().(interface{ AddDestination(string) error })
	require.True(t, ok)
	require.NoError(t, dests.AddDestination(fmt.Sprintf("aeron:udp?endpoint=localhost:%d", e.p.record)))
	require.NoError(t, dests.AddDestination(fmt.Sprintf("aeron:udp?endpoint=localhost:%d", e.p.live)))
	e.pub, e.session = pub, pub.Pub().SessionID()
	_, err = e.arch.StartRecording(fmt.Sprintf("aeron:udp?endpoint=localhost:%d|session-id=%d", e.p.record,
		e.session), e.stream, false, true)
	require.NoError(t, err)
	e.recID = -1
	awaitTrue(t, "recording", 10*time.Second, func() bool {
		ds, err := e.arch.ListRecordingsForUri(0, 100, fmt.Sprintf("localhost:%d", e.p.record), e.stream)
		for _, d := range ds {
			if err == nil && d.SessionId == e.session {
				e.recID = d.RecordingId
			}
		}
		return e.recID >= 0
	})
	e.publish()
	e.awaitSent(5000, 30*time.Second)
	cfg := e.mergeConfig(total)
	cfg.LiveDestination = fmt.Sprintf("aeron:udp?endpoint=localhost:%d", e.p.live)
	r := rmrun.RunReplayMerge(e.c.Aeron, cfg, rmrun.Nop)
	require.Empty(t, r.Failures, "%s", failMessages(r))
	require.True(t, r.Merged)
	requireExact(t, r, total)
}

// TestInvalidSubscriptions checks the Java ReplayMerge constructor checks: IPC merging and a subscription without
// control-mode=manual are rejected (IllegalArgumentException in Java) before any archive request is sent.
func TestInvalidSubscriptions(t *testing.T) {
	requireAdversarial(t)
	freshStack(t)
	c := connect(t)
	arch := connectArchive(t, c.Aeron)
	for channel, want := range map[string]string{
		"aeron:ipc":                          "IPC merging is not supported",
		"aeron:udp?endpoint=localhost:27406": "control-mode=manual",
	} {
		sub, err := c.Aeron.AddSubscription(channel, 9423)
		require.NoError(t, err)
		_, err = replaymerge.NewReplayMerge(sub, arch, "aeron:udp?endpoint=localhost:0",
			"aeron:udp?endpoint=localhost:27407", "aeron:udp?endpoint=localhost:27408|control=localhost:27409", 0, 0, 5000)
		require.ErrorContains(t, err, want)
		require.NoError(t, sub.Close())
	}
}
