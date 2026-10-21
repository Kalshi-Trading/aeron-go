package chaos

import (
	"testing"
	"time"

	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

// TestChaosLargeMessages sends messages from 12 bytes up to the publication's max message length, including sizes at
// and around MTU and term boundaries, over IPC and UDP, and checks every one is reassembled intact and in order.
func TestChaosLargeMessages(t *testing.T) {
	requireChaos(t)
	restartStack(t)
	a := connect(t, chaosDir)
	channels := []string{
		"aeron:ipc?term-length=16m",
		"aeron:udp?endpoint=localhost:24570|term-length=16m|mtu=8192",
		"aeron:udp?endpoint=localhost:24571|term-length=64k|mtu=1408",
	}
	for i, channel := range channels {
		t.Run(channel, func(t *testing.T) {
			streamID := int32(5800 + i)
			sub, err := a.AddSubscription(channel, streamID)
			require.NoError(t, err)
			defer sub.Close()
			pub, err := a.AddExclusivePublication(channel, streamID)
			require.NoError(t, err)
			defer pub.Close()
			require.NoError(t, e2e.WaitConnected(pub, 5*time.Second))
			maxLen := int(pub.MaxMessageLength())
			payload := int(pub.MaxPayloadLength())
			sizes := []int{maxLen, e2e.HeaderLength, maxLen - 1, payload, payload + 1, 2*payload - 1, 2 * payload,
				2*payload + 1, maxLen / 2, maxLen/3 + 7, 64*1024 + 3}
			const count = 300
			size := func(seq int64) int {
				n := sizes[int(seq)%len(sizes)]
				if n < e2e.HeaderLength {
					n = e2e.HeaderLength
				}
				if n > maxLen {
					n = maxLen
				}
				return n
			}
			checker := e2e.NewSequenceChecker(0)
			done := make(chan error, 1)
			go func() { done <- e2e.Consume(e2e.ReassemblingPoller(sub), checker, count, 60*time.Second) }()
			publishAll(t, pub, count, size, 60*time.Second)
			require.NoError(t, <-done)
			require.Equal(t, int64(count), checker.Received)
			t.Logf("max=%d payload=%d: %s", maxLen, payload, checker)
		})
	}

	// Offers over the max message length are rejected (Java throws IllegalArgumentException), not truncated.
	sub, err := a.AddSubscription("aeron:ipc", 5810)
	require.NoError(t, err)
	defer sub.Close()
	pub, err := a.AddExclusivePublication("aeron:ipc?term-length=64k", 5810)
	require.NoError(t, err)
	defer pub.Close()
	require.NoError(t, e2e.WaitConnected(pub, 5*time.Second))
	tooLong := make([]byte, pub.MaxMessageLength()+1)
	require.Panics(t, func() { pub.Offer(ab.MakeBuffer(tooLong), 0, int32(len(tooLong)), nil) })
}
