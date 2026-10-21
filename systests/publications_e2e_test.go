// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package systests

import (
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/stretchr/testify/require"
)

// Publication E2E against a Java 1.53.3 driver and the Java PubVerifier subscriber (docker/publications.compose.yml):
//
//	docker compose -f docker/docker-compose.yml -f docker/publications.compose.yml up -d --build archiving-driver pub-verifier
//	AERON_E2E=1 go test ./systests/ -run TestPublicationsE2E -v
const (
	e2eDataChannel  = "aeron:udp?endpoint=localhost:20140"
	e2eReplyChannel = "aeron:udp?endpoint=localhost:20141"
	e2eReplyStream  = 3100
)

type verifierReply map[string]int64

type e2eHarness struct {
	t       *testing.T
	client  *aeron.Aeron
	replies *aeron.Subscription
	pending map[int64][]verifierReply
	errors  chan error
}

func newE2EHarness(t *testing.T) *e2eHarness {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 with the docker Java driver and pub-verifier running")
	}
	dir := os.Getenv("AERON_DIR")
	if dir == "" {
		dir = "/dev/shm/aeron-compose"
	}
	h := &e2eHarness{t: t, pending: map[int64][]verifierReply{}, errors: make(chan error, 100)}
	client, err := aeron.Connect(aeron.NewContext().AeronDir(dir).
		ErrorHandler(func(err error) { h.errors <- err }))
	require.NoError(t, err)
	h.client = client
	t.Cleanup(func() { _ = client.Close() })
	h.replies, err = client.AddSubscription(e2eReplyChannel, e2eReplyStream)
	require.NoError(t, err)
	h.waitFor("reply image", func() bool { return h.replies.IsConnected() })
	return h
}

func (h *e2eHarness) waitFor(what string, cond func() bool) {
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func parseReply(s string) verifierReply {
	r := verifierReply{}
	for _, kv := range strings.Fields(s) {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) == 2 {
			v, err := strconv.ParseInt(parts[1], 10, 64)
			if err == nil {
				r[parts[0]] = v
			}
		}
	}
	return r
}

// await returns the next n replies for streamID from the Java verifier.
func (h *e2eHarness) await(streamID int32, n int) []verifierReply {
	handler := func(buffer *atomic.Buffer, offset int32, length int32, _ *logbuffer.Header) {
		r := parseReply(string(buffer.GetBytesArray(offset, length)))
		h.pending[r["stream"]] = append(h.pending[r["stream"]], r)
	}
	key := int64(streamID)
	h.waitFor(fmt.Sprintf("%d replies on stream %d", n, streamID), func() bool {
		h.replies.Poll(handler, 16)
		return len(h.pending[key]) >= n
	})
	got := h.pending[key][:n]
	h.pending[key] = h.pending[key][n:]
	return got
}

func (h *e2eHarness) offer(what string, fn func() int64) int64 {
	var result int64
	h.waitFor(what, func() bool {
		result = fn()
		if result == aeron.PublicationClosed || result == aeron.MaxPositionExceeded {
			h.t.Fatalf("%s: %s", what, aeron.PublicationErrorString(result))
		}
		return result > 0
	})
	return result
}

func (h *e2eHarness) noErrors() {
	for {
		select {
		case err := <-h.errors:
			h.t.Errorf("client error: %v", err)
		default:
			return
		}
	}
}

func payload(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i*31) + byte(i>>8)
	}
	return b
}

func requireMessage(t *testing.T, r verifierReply, msg []byte, position int64) {
	require.EqualValues(t, len(msg), r["len"], "length")
	require.EqualValues(t, crc32.ChecksumIEEE(msg), r["crc"], "crc")
	require.Equal(t, position, r["pos"], "position")
}

func TestPublicationsE2E(t *testing.T) {
	h := newE2EHarness(t)

	t.Run("ConcurrentOfferAndStatus", func(t *testing.T) {
		pub, err := h.client.AddPublication(e2eDataChannel, 3001)
		require.NoError(t, err)
		defer pub.Close()
		h.waitFor("connected", pub.IsConnected)
		require.Equal(t, aeron.ChannelStatusActive, pub.ChannelStatus())
		addrs := pub.LocalSocketAddresses()
		t.Logf("local socket addresses: %v", addrs)
		require.NotEmpty(t, addrs)
		require.Positive(t, pub.AvailableWindow())
		require.Greater(t, pub.PositionLimit(), pub.Position())

		var rv int64
		supplier := func(*atomic.Buffer, int32, int32) int64 { return rv }
		for i := 0; i < 10; i++ {
			msg := payload(100+i*50, byte(i))
			rv = int64(1000 + i)
			pos := h.offer("offer", func() int64 { return pub.Offer(atomic.MakeBuffer(msg), 0, int32(len(msg)), supplier) })
			r := h.await(3001, 1)[0]
			requireMessage(t, r, msg, pos)
			require.Equal(t, rv, r["rv"])
			require.EqualValues(t, pub.SessionID(), r["session"])
		}
		a, b := payload(700, 1), payload(3000, 2)
		pos := h.offer("offer2", func() int64 {
			return pub.Offer2(atomic.MakeBuffer(a), 0, 700, atomic.MakeBuffer(b), 0, 3000, nil)
		})
		requireMessage(t, h.await(3001, 1)[0], append(append([]byte{}, a...), b...), pos)
		vectors := []aeron.DirectBufferVector{
			{Buffer: atomic.MakeBuffer(b), Offset: 10, Length: 2000},
			{Buffer: atomic.MakeBuffer(a), Offset: 0, Length: 700},
		}
		pos = h.offer("vectors", func() int64 { return pub.OfferVectors(vectors, nil) })
		requireMessage(t, h.await(3001, 1)[0], append(append([]byte{}, b[10:2010]...), a...), pos)
	})

	t.Run("TryClaim", func(t *testing.T) {
		pub, err := h.client.AddPublication(e2eDataChannel, 3002)
		require.NoError(t, err)
		defer pub.Close()
		h.waitFor("connected", pub.IsConnected)
		var claim aeron.BufferClaim
		for i := 0; i < 5; i++ {
			msg := payload(200, byte(i))
			pos := h.offer("tryClaim", func() int64 { return pub.TryClaim(int32(len(msg)), &claim) })
			claim.PutBytes(atomic.MakeBuffer(msg), 0, int32(len(msg))).PutReservedValue(int64(i) << 40)
			if i == 2 {
				claim.Abort()
				continue
			}
			claim.Commit()
			r := h.await(3002, 1)[0]
			requireMessage(t, r, msg, pos)
			require.Equal(t, int64(i)<<40, r["rv"])
		}
	})

	t.Run("Fragmented8MB", func(t *testing.T) {
		pub, err := h.client.AddPublication(e2eDataChannel+"|term-length=64m", 3003)
		require.NoError(t, err)
		defer pub.Close()
		h.waitFor("connected", pub.IsConnected)
		require.EqualValues(t, 8*1024*1024, pub.MaxMessageLength())
		msg := payload(8*1024*1024, 7)
		pos := h.offer("offer 8MB", func() int64 { return pub.Offer(atomic.MakeBuffer(msg), 0, int32(len(msg)), nil) })
		requireMessage(t, h.await(3003, 1)[0], msg, pos)

		half := int32(len(msg) / 2)
		pos = h.offer("offer2 8MB", func() int64 {
			return pub.Offer2(atomic.MakeBuffer(msg), half, half, atomic.MakeBuffer(msg), 0, half, nil)
		})
		requireMessage(t, h.await(3003, 1)[0], append(append([]byte{}, msg[half:]...), msg[:half]...), pos)
	})

	t.Run("ExclusiveInitialPosition", func(t *testing.T) {
		termLength := int64(64 * 1024)
		channel := e2eDataChannel + "|term-length=64k|init-term-id=100|term-id=105|term-offset=1024"
		pub, err := h.client.AddExclusivePublication(channel, 3004)
		require.NoError(t, err)
		defer pub.Close()
		start := 5*termLength + 1024
		require.Equal(t, start, pub.Position())
		require.EqualValues(t, 100, pub.InitialTermID())
		require.EqualValues(t, 105, pub.TermID())
		h.waitFor("connected", pub.IsConnected)

		msg := payload(100, 3)
		pos := h.offer("exclusive offer", func() int64 { return pub.Offer(atomic.MakeBuffer(msg), 0, 100, nil) })
		require.Equal(t, start+160, pos)
		r := h.await(3004, 1)[0]
		requireMessage(t, r, msg, pos)
		require.EqualValues(t, 105, r["termId"])
		require.EqualValues(t, 1024, r["termOffset"])
		require.EqualValues(t, 100, r["initialTermId"])

		// Cross into the next term to exercise exclusive rotation with a consumer.
		big := payload(5000, 9)
		var last int64
		for i := 0; i < 20; i++ {
			last = h.offer("exclusive rotate", func() int64 { return pub.Offer(atomic.MakeBuffer(big), 0, 5000, nil) })
		}
		replies := h.await(3004, 20)
		requireMessage(t, replies[19], big, last)
		require.Greater(t, last, 6*termLength)
		require.EqualValues(t, 106, replies[19]["termId"])

		var claim aeron.BufferClaim
		pos = h.offer("exclusive tryClaim", func() int64 { return pub.TryClaim(64, &claim) })
		claim.PutBytes(atomic.MakeBuffer(msg), 0, 64).PutReservedValue(55)
		claim.Commit()
		r = h.await(3004, 1)[0]
		requireMessage(t, r, msg[:64], pos)
		require.EqualValues(t, 55, r["rv"])
	})

	t.Run("OfferBlockAppendPaddingRevoke", func(t *testing.T) {
		pub, err := h.client.AddExclusivePublication(e2eDataChannel, 3005)
		require.NoError(t, err)
		h.waitFor("connected", pub.IsConnected)
		first := payload(40, 1)
		h.offer("offer", func() int64 { return pub.Offer(atomic.MakeBuffer(first), 0, 40, nil) })
		h.await(3005, 1)

		a, b := payload(300, 2), payload(17, 3)
		block := buildBlock(pub, a, b)
		pos := h.offer("offerBlock", func() int64 { return pub.OfferBlock(atomic.MakeBuffer(block), 0, int32(len(block))) })
		replies := h.await(3005, 2)
		requireMessage(t, replies[0], a, pos-int64(len(block))+alignedFrame(300))
		requireMessage(t, replies[1], b, pos)

		padded := h.offer("appendPadding", func() int64 { return pub.AppendPadding(1000) })
		require.Equal(t, pos+alignedFrame(1000), padded)
		after := payload(10, 4)
		pos = h.offer("offer after padding", func() int64 { return pub.Offer(atomic.MakeBuffer(after), 0, 10, nil) })
		requireMessage(t, h.await(3005, 1)[0], after, pos)

		require.False(t, pub.IsRevoked())
		require.NoError(t, pub.Revoke())
		require.True(t, pub.IsClosed())
		require.Equal(t, aeron.PublicationClosed, pub.Offer(atomic.MakeBuffer(after), 0, 10, nil))
	})

	t.Run("ManualMDC", func(t *testing.T) {
		pub, err := h.client.AddPublication("aeron:udp?control-mode=manual", 3006)
		require.NoError(t, err)
		defer pub.Close()
		require.NoError(t, pub.AddDestination(e2eDataChannel))
		h.waitFor("connected", pub.IsConnected)
		msg := payload(256, 5)
		pos := h.offer("mdc offer", func() int64 { return pub.Offer(atomic.MakeBuffer(msg), 0, 256, nil) })
		requireMessage(t, h.await(3006, 1)[0], msg, pos)
		require.NoError(t, pub.RemoveDestination(e2eDataChannel))
		h.waitFor("disconnected", func() bool { return !pub.IsConnected() })
	})

	t.Run("JavaBasicSubscriber", func(t *testing.T) {
		if os.Getenv("AERON_E2E_BASIC_SUBSCRIBER") != "1" {
			t.Skip("set AERON_E2E_BASIC_SUBSCRIBER=1 with `--profile clients up -d java-subscriber` running")
		}
		pub, err := h.client.AddPublication("aeron:udp?endpoint=localhost:20122", 1002)
		require.NoError(t, err)
		defer pub.Close()
		h.waitFor("connected", pub.IsConnected)
		text := fmt.Sprintf("Hello from aeron-go %d", time.Now().UnixNano())
		h.offer("basic offer", func() int64 { return pub.Offer(atomic.MakeBuffer([]byte(text)), 0, int32(len(text)), nil) })
		h.waitFor("BasicSubscriber output", func() bool {
			out, _ := exec.Command("docker", "compose", "-f", "../docker/docker-compose.yml", "--profile", "clients",
				"logs", "--no-color", "java-subscriber").CombinedOutput()
			return strings.Contains(string(out), text)
		})
	})

	h.noErrors()
}

func alignedFrame(payloadLength int32) int64 {
	return int64(util.AlignInt32(payloadLength+logbuffer.DataFrameHeader.Length, logbuffer.FrameAlignment))
}

// buildBlock formats committed data frames starting at the exclusive publication's current term offset, as an
// archive replicator would, for OfferBlock.
func buildBlock(pub *aeron.ExclusivePublication, payloads ...[]byte) []byte {
	var block []byte
	offset := pub.TermOffset()
	hdr := &logbuffer.DataFrameHeader
	for _, p := range payloads {
		frame := make([]byte, alignedFrame(int32(len(p))))
		b := atomic.MakeBuffer(frame)
		b.PutInt32(hdr.FrameLengthFieldOffset, int32(len(p))+hdr.Length)
		b.PutUInt8(hdr.FlagsFieldOffset, 0xC0)
		b.PutUInt16(hdr.TypeFieldOffset, hdr.TypeData)
		b.PutInt32(hdr.TermOffsetFieldOffset, offset)
		b.PutInt32(hdr.SessionIDFieldOffset, pub.SessionID())
		b.PutInt32(hdr.StreamIDFieldOffset, pub.StreamID())
		b.PutInt32(hdr.TermIDFieldOffset, pub.TermID())
		copy(frame[hdr.Length:], p)
		block = append(block, frame...)
		offset += int32(len(frame))
	}
	return block
}
