package chaos

import (
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

func lossSize(seq int64) int {
	if seq%10 == 0 {
		return 4000 + int(seq%997) // fragmented at mtu=1408
	}
	return 100 + int(seq%300)
}

func naksSent(a *aeron.Aeron) int64 {
	naks := int64(-1)
	a.CounterReader().Scan(func(c counters.Counter) {
		if c.TypeId == 0 && strings.HasPrefix(c.Label, "NAKs sent") {
			naks = c.Value
		}
	})
	return naks
}

// TestChaosPacketLossLive: the lossy driver drops ~5% of received data frames. NAK driven retransmission must deliver
// every message (fragmented and not) exactly once and in order.
func TestChaosPacketLossLive(t *testing.T) {
	requireChaos(t)
	a := connect(t, lossyDir)
	const channel = "aeron:udp?endpoint=localhost:24540|mtu=1408|term-length=1m"
	const count = 30000
	naksBefore := naksSent(a)
	sub, err := a.AddSubscription(channel, 5600)
	require.NoError(t, err)
	pub, err := a.AddExclusivePublication(channel, 5600)
	require.NoError(t, err)
	require.NoError(t, e2e.WaitConnected(pub, 5*time.Second))
	checker := e2e.NewSequenceChecker(0)
	done := make(chan error, 1)
	go func() { done <- e2e.Consume(e2e.ReassemblingPoller(sub), checker, count, 90*time.Second) }()
	publishAll(t, pub, count, lossSize, 90*time.Second)
	require.NoError(t, <-done)
	require.Equal(t, int64(count), checker.Received)
	naksAfter := naksSent(a)
	t.Logf("%s; NAKs sent %d -> %d", checker, naksBefore, naksAfter)
	require.Greater(t, naksAfter, naksBefore, "loss generator not active")
}

// TestChaosPacketLossRecordReplay records a lossy UDP stream and replays it over a lossy UDP channel: the replay must
// match what was published, byte for byte and in order.
func TestChaosPacketLossRecordReplay(t *testing.T) {
	requireChaos(t)
	a := connect(t, lossyDir)
	arch := connectArchive(t, lossyDir, lossyControlChannel)
	const channel = "aeron:udp?endpoint=localhost:24541|mtu=1408|term-length=1m"
	const count = 20000
	streamID := stream(5610)
	_, err := arch.StartRecording(channel, streamID, false, true)
	require.NoError(t, err)
	pub, err := a.AddExclusivePublication(channel, streamID)
	require.NoError(t, err)
	require.NoError(t, e2e.WaitConnected(pub, 10*time.Second))
	publishAll(t, pub, count, lossSize, 90*time.Second)
	rec, err := e2e.WaitForRecording(arch, "endpoint=localhost:24541", streamID, 10*time.Second, bySession(pub.SessionID()))
	require.NoError(t, err)
	require.NoError(t, e2e.WaitRecordingPosition(arch, rec.RecordingId, pub.Position(), 60*time.Second))

	replay, _, err := e2e.Replay(arch, rec.RecordingId, 0, pub.Position(), "aeron:udp?endpoint=localhost:24542|mtu=1408", 5611)
	require.NoError(t, err)
	defer replay.Close()
	checker := e2e.NewSequenceChecker(0)
	require.NoError(t, e2e.Consume(e2e.ReassemblingPoller(replay), checker, count, 90*time.Second))
	require.Equal(t, int64(count), checker.Received)
	t.Logf("replay %s", checker)
}
