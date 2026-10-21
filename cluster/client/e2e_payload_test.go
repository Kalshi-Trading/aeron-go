package client

import (
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/interop"
	"github.com/stretchr/testify/require"
)

// echoPayloadListener verifies echoed egress messages in order with the interop payload generator.
type echoPayloadListener struct {
	recordingListener
	sizes  interop.Sizes
	next   int64
	errors []string
}

func (l *echoPayloadListener) OnMessage(_ int64, _ int64, buffer *atomic.Buffer, offset int32, length int32,
	_ *logbuffer.Header) {
	if err := interop.Verify(buffer.GetBytesArray(offset, length), l.next, l.sizes); err != "" {
		l.errors = append(l.errors, err)
	}
	l.next++
}

// The Java cluster's echo service returns every ingress message unchanged and in order: sizes straddle the
// single-frame boundary of the default 1408 MTU (1408 - DataHeader - SessionHeader = 1344) and go up to the
// 64k ingress term's max message length, sent through both Offer and TryClaim.
func TestE2EEchoPayloadSizesAndOrder(t *testing.T) {
	nodes := e2eNodes(t)
	const maxPayload = 8192 - SessionHeaderLength
	listener := &echoPayloadListener{sizes: interop.Sizes{8, 64, 1343, 1344, 1345, 4000, maxPayload}}
	cl, err := Connect(e2eContext(nodes, listener))
	require.NoError(t, err)
	defer cl.Close()

	const count = 2_000
	buf := atomic.MakeBuffer(make([]byte, maxPayload))
	var claim logbuffer.Claim
	deadline := time.Now().Add(30 * time.Second)
	for seq := int64(0); seq < count; seq++ {
		length := listener.sizes.Length(seq)
		claimable := length <= 1344 && seq%2 == 1
		for {
			require.True(t, time.Now().Before(deadline), "ingress timed out at seq %d", seq)
			var result int64
			if claimable {
				if result = cl.TryClaim(length, &claim); result > 0 {
					interop.FillBuffer(claim.Buffer(), claim.Offset()+SessionHeaderLength, seq, length)
					claim.Commit()
				}
			} else {
				interop.FillBuffer(buf, 0, seq, length)
				result = cl.Offer(buf, 0, length)
			}
			if result > 0 {
				break
			}
			cl.PollEgress()
		}
		cl.PollEgress()
	}
	pollUntil(t, cl, 30*time.Second, func() bool { return listener.next == count })
	require.Empty(t, listener.errors)
}
