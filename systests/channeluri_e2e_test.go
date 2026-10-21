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
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	aatomic "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// End-to-end tests for ChannelUriStringBuilder-produced channels against an external (docker) Java media
// driver. Run with:
//
//	docker compose -f docker/docker-compose.yml up -d --build archiving-driver
//	AERON_E2E=1 AERON_DIR=/dev/shm/aeron-compose go test ./systests/ -run TestChannelUriE2E -v

const e2eTimeout = 10 * time.Second

func connectChannelUriE2E(t *testing.T) *aeron.Aeron {
	t.Helper()
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against an external Java media driver")
	}
	dir := os.Getenv("AERON_DIR")
	if dir == "" {
		dir = "/dev/shm/aeron-compose"
	}
	a, err := aeron.Connect(aeron.NewContext().AeronDir(dir).MediaDriverTimeout(10 * time.Second))
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func build(t *testing.T, b *aeron.ChannelUriStringBuilder) string {
	t.Helper()
	s, err := b.Build()
	require.NoError(t, err)
	t.Logf("channel: %s", s)
	return s
}

func udp() *aeron.ChannelUriStringBuilder {
	return aeron.NewChannelUriStringBuilder().Media(aeron.UdpMedia)
}

// e2ePub is the surface shared by *aeron.Publication and *aeron.ExclusivePublication used by these tests.
type e2ePub interface {
	Channel() string
	SessionID() int32
	InitialTermID() int32
	IsConnected() bool
	Position() int64
	Offer(buffer *aatomic.Buffer, offset int32, length int32, reservedValueSupplier term.ReservedValueSupplier) int64
	Close() error
}

func addPub(t *testing.T, a *aeron.Aeron, channel string, streamID int32, exclusive bool) e2ePub {
	t.Helper()
	var pub e2ePub
	var err error
	if exclusive {
		pub, err = a.AddExclusivePublication(channel, streamID)
	} else {
		pub, err = a.AddPublication(channel, streamID)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })
	return pub
}

func addSub(t *testing.T, a *aeron.Aeron, channel string, streamID int32, onImage aeron.AvailableImageHandler) *aeron.Subscription {
	t.Helper()
	if onImage == nil {
		onImage = func(aeron.Image) {}
	}
	sub, err := a.AddSubscriptionWithHandlers(channel, streamID, onImage, func(aeron.Image) {})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

func awaitTrue(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(e2eTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			require.FailNow(t, "timed out waiting for "+what)
		}
		time.Sleep(time.Millisecond)
	}
}

func offerN(t *testing.T, pub e2ePub, prefix string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		msg := []byte(fmt.Sprintf("%s-%d", prefix, i))
		buf := aatomic.MakeBuffer(msg)
		awaitTrue(t, "offer on "+pub.Channel(), func() bool {
			return pub.Offer(buf, 0, int32(len(msg)), nil) > 0
		})
	}
}

func pollN(t *testing.T, sub *aeron.Subscription, prefix string, n int) {
	t.Helper()
	received := 0
	handler := func(buffer *aatomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
		assert.Equal(t, fmt.Sprintf("%s-%d", prefix, received), string(buffer.GetBytesArray(offset, length)))
		received++
	}
	awaitTrue(t, fmt.Sprintf("%d messages on %s", n, sub.Channel()), func() bool {
		sub.Poll(handler, 10)
		return received >= n
	})
	assert.Equal(t, n, received)
}

func TestChannelUriE2EMdcDynamic(t *testing.T) {
	a := connectChannelUriE2E(t)
	const streamID = 24001
	control := "localhost:24325"
	pubChannel := build(t, udp().ControlEndpoint(control).ControlMode(aeron.MdcControlModeDynamic).
		TermLength(128*1024).Mtu(1408).Linger(0).ChannelAndEntityTags(freshTag(), freshTag()))
	pub := addPub(t, a, pubChannel, streamID, false)

	var subs []*aeron.Subscription
	for _, ep := range []string{"localhost:24326", "localhost:24327"} {
		ch := build(t, udp().Endpoint(ep).ControlEndpoint(control).Group(false).Reliable(true))
		subs = append(subs, addSub(t, a, ch, streamID, nil))
	}
	for _, s := range subs {
		sub := s
		awaitTrue(t, "MDC subscriber connected", sub.IsConnected)
	}
	awaitTrue(t, "MDC publication connected", pub.IsConnected)
	offerN(t, pub, "mdc", 20)
	for _, sub := range subs {
		pollN(t, sub, "mdc", 20)
	}
}

// freshTag returns a tag unlikely to collide with tags still registered in the shared driver by earlier runs.
func freshTag() int64 {
	return 1_000_000 + (time.Now().UnixNano()/1000%1_000_000_000)*4 + tagSeq.Add(4)
}

var tagSeq atomic.Int64

func TestChannelUriE2ETaggedChannels(t *testing.T) {
	a := connectChannelUriE2E(t)
	const streamA, streamB = 24011, 24012
	endpoint := "localhost:24330"
	pubTag := freshTag()
	entityTag, subTag := pubTag+1, pubTag+2

	pubA := addPub(t, a, build(t, udp().Endpoint(endpoint).ChannelAndEntityTags(pubTag, entityTag)), streamA, false)
	subA := addSub(t, a, build(t, udp().Endpoint(endpoint).ChannelTag(subTag)), streamA, nil)

	// Refer to the existing send/receive channel endpoints purely by channel tag.
	pubB := addPub(t, a, build(t, udp().ChannelTag(pubTag)), streamB, false)
	subB := addSub(t, a, build(t, udp().ChannelTag(subTag)), streamB, nil)

	uri, err := aeron.ParseChannelUri(pubB.Channel())
	require.NoError(t, err)
	assert.Equal(t, strconv.FormatInt(pubTag, 10), uri.ChannelTag())

	awaitTrue(t, "tagged pubA connected", pubA.IsConnected)
	awaitTrue(t, "tagged pubB connected", pubB.IsConnected)
	offerN(t, pubA, "a", 10)
	offerN(t, pubB, "b", 10)
	pollN(t, subA, "a", 10)
	pollN(t, subB, "b", 10)

	// Explicit session-id selection on both sides.
	const sessionID = 777001
	ch := build(t, udp().Endpoint("localhost:24333").SessionId(sessionID))
	pubS := addPub(t, a, ch, streamA, true)
	assert.Equal(t, int32(sessionID), pubS.SessionID())
	subS := addSub(t, a, ch, streamA, nil)
	awaitTrue(t, "session pub connected", pubS.IsConnected)
	offerN(t, pubS, "s", 5)
	pollN(t, subS, "s", 5)
	assert.NotNil(t, subS.ImageBySessionID(sessionID))
}

func TestChannelUriE2ESpiesSimulateConnection(t *testing.T) {
	a := connectChannelUriE2E(t)
	const streamID = 24021

	sscEndpoint := "localhost:24340"
	pub := addPub(t, a, build(t, udp().Endpoint(sscEndpoint).SpiesSimulateConnection(true)), streamID, false)
	spy := addSub(t, a, build(t, udp().Prefix(aeron.SpyQualifier).Endpoint(sscEndpoint)), streamID, nil)
	awaitTrue(t, "ssc=true publication connected via spy only", pub.IsConnected)
	offerN(t, pub, "spy", 10)
	pollN(t, spy, "spy", 10)

	noSscEndpoint := "localhost:24341"
	pubNoSsc := addPub(t, a, build(t, udp().Endpoint(noSscEndpoint).SpiesSimulateConnection(false)), streamID, false)
	spyNoSsc := addSub(t, a, build(t, udp().Prefix(aeron.SpyQualifier).Endpoint(noSscEndpoint)), streamID, nil)
	awaitTrue(t, "spy image", spyNoSsc.HasImages)
	time.Sleep(500 * time.Millisecond)
	assert.False(t, pubNoSsc.IsConnected(), "ssc=false publication must not be connected by a spy alone")
}

func TestChannelUriE2EResponseChannel(t *testing.T) {
	a := connectChannelUriE2E(t)
	const requestStream, responseStream = 24031, 24032
	requestEndpoint := "localhost:24350"
	responseControl := "localhost:24351"

	var requestImage atomic.Value
	subReq := addSub(t, a, build(t, udp().Endpoint(requestEndpoint)), requestStream,
		func(image aeron.Image) { requestImage.Store(image) })
	subRsp := addSub(t, a, build(t, udp().ControlEndpoint(responseControl).ControlMode(aeron.ControlModeResponse)),
		responseStream, nil)
	pubReq := addPub(t, a, build(t, udp().Endpoint(requestEndpoint).ResponseCorrelationId(subRsp.RegistrationID())),
		requestStream, false)

	awaitTrue(t, "request publication connected", pubReq.IsConnected)
	awaitTrue(t, "request image", func() bool { return requestImage.Load() != nil })
	image := requestImage.Load().(aeron.Image)

	pubRsp := addPub(t, a, build(t, udp().ControlEndpoint(responseControl).ControlMode(aeron.ControlModeResponse).
		ResponseCorrelationId(image.CorrelationID())), responseStream, false)
	isResponse, err := aeron.IsControlModeResponse(pubRsp.Channel())
	require.NoError(t, err)
	assert.True(t, isResponse)

	awaitTrue(t, "response subscription connected", subRsp.IsConnected)
	awaitTrue(t, "response publication connected", pubRsp.IsConnected)
	offerN(t, pubReq, "req", 10)
	pollN(t, subReq, "req", 10)
	offerN(t, pubRsp, "rsp", 10)
	pollN(t, subRsp, "rsp", 10)
}

func TestChannelUriE2EInitialPosition(t *testing.T) {
	a := connectChannelUriE2E(t)
	const streamID = 24041
	const termLength = 64 * 1024
	const position = int64(3*termLength + 1024)
	ch := build(t, udp().Endpoint("localhost:24360").InitialPosition(position, 100, termLength))
	pub := addPub(t, a, ch, streamID, true)
	sub := addSub(t, a, ch, streamID, nil)
	assert.Equal(t, position, pub.Position())
	assert.Equal(t, int32(100), pub.InitialTermID())
	awaitTrue(t, "initial position publication connected", pub.IsConnected)
	offerN(t, pub, "pos", 5)
	pollN(t, sub, "pos", 5)
	assert.Greater(t, pub.Position(), position)
}
