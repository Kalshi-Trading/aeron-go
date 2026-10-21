package aeron

import (
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func (p *fakeDriverProxy) RejectImage(int64, int64, string) (int64, error) {
	return p.record("RejectImage")
}

// pollOrder returns the index of the first image polled by each of n subscription polls over three images.
func pollOrder(t *testing.T, n int, poll func(sub *Subscription)) []int {
	cc := NewMockReceivingConductor(t)
	sub := NewSubscription(cc, Channel, RegistrationId, StreamId, ChannelStatusId, nil, nil)
	var order []int
	first := true
	for i := 0; i < 3; i++ {
		idx := i
		img := NewMockImage(t)
		record := func(mock.Arguments) {
			if first {
				order = append(order, idx)
				first = false
			}
		}
		img.On("Poll", mock.Anything, mock.Anything).Run(record).Return(0).Maybe()
		img.On("ControlledPoll", mock.Anything, mock.Anything).Run(record).Return(0).Maybe()
		sub.addImage(img)
	}
	for i := 0; i < n; i++ {
		first = true
		poll(sub)
	}
	return order
}

func TestSubscriptionPollRoundRobinMatchesJava(t *testing.T) {
	expected := []int{0, 1, 2, 0, 1, 2, 0}
	require.Equal(t, expected, pollOrder(t, len(expected), func(sub *Subscription) {
		sub.Poll(func(*atomic.Buffer, int32, int32, *logbuffer.Header) {}, 10)
	}))
	require.Equal(t, expected, pollOrder(t, len(expected), func(sub *Subscription) {
		sub.ControlledPoll(func(*atomic.Buffer, int32, int32, *logbuffer.Header) term.ControlledPollAction {
			return term.ControlledPollActionContinue
		}, 10)
	}))
}

func requirePanics(t *testing.T, f func()) {
	t.Helper()
	require.Panics(t, f)
}

// Java Image polls advance the position in a finally block, so a handler exception consumes the fragment that threw.
func TestImagePollHandlerPanicAdvancesPosition(t *testing.T) {
	frag := func(*atomic.Buffer, int32, int32, *logbuffer.Header) { panic("boom") }
	ctrl := func(*atomic.Buffer, int32, int32, *logbuffer.Header) term.ControlledPollAction { panic("boom") }
	cases := map[string]func(img *image){
		"Poll":                  func(img *image) { img.Poll(frag, 10) },
		"BoundedPoll":           func(img *image) { img.BoundedPoll(frag, math.MaxInt64, 10) },
		"ControlledPoll":        func(img *image) { img.ControlledPoll(ctrl, 10) },
		"BoundedControlledPoll": func(img *image) { img.BoundedControlledPoll(ctrl, math.MaxInt64, 10) },
	}
	for name, poll := range cases {
		t.Run(name, func(t *testing.T) {
			ti := newTestImage(t)
			next := ti.appendFrame(0, 32, flagUnfragmented, 0)
			ti.appendFrame(next, 32, flagUnfragmented, 0)
			requirePanics(t, func() { poll(ti.image) })
			require.Equal(t, int64(next), ti.Position())
		})
	}

	t.Run("BlockPoll", func(t *testing.T) {
		ti := newTestImage(t)
		next := ti.appendFrame(0, 32, flagUnfragmented, 0)
		requirePanics(t, func() {
			ti.BlockPoll(func(*atomic.Buffer, int32, int32, int32, int32) { panic("boom") }, 4096)
		})
		require.Equal(t, int64(next), ti.Position())
	})
	t.Run("RawPoll", func(t *testing.T) {
		ti := newTestImage(t)
		next := ti.appendFrame(0, 32, flagUnfragmented, 0)
		requirePanics(t, func() {
			ti.RawPoll(func(string, int64, *atomic.Buffer, int32, int32, int32, int32) { panic("boom") }, 4096)
		})
		require.Equal(t, int64(next), ti.Position())
	})
}

func TestImageBoundedPollAtOrPastLimitReadsNothing(t *testing.T) {
	ti := newTestImage(t)
	next := ti.appendFrame(0, 32, flagUnfragmented, 0)
	ti.appendFrame(next, 32, flagUnfragmented, 0)
	require.NoError(t, ti.SetPosition(int64(next)))

	called := 0
	frag := func(*atomic.Buffer, int32, int32, *logbuffer.Header) { called++ }
	ctrl := func(*atomic.Buffer, int32, int32, *logbuffer.Header) term.ControlledPollAction {
		called++
		return term.ControlledPollActionContinue
	}
	for _, limit := range []int64{math.MinInt64, 0, int64(next)} {
		require.Equal(t, 0, ti.BoundedPoll(frag, limit, 10))
		require.Equal(t, 0, ti.BoundedControlledPoll(ctrl, limit, 10))
	}
	require.Zero(t, called)
	require.Equal(t, int64(next), ti.Position())
}

func TestImageString(t *testing.T) {
	ti := newTestImage(t)
	s := ti.String()
	require.True(t, strings.HasPrefix(s, "Image{correlationId=99, sessionId="), s)
	require.Contains(t, s, "sourceIdentity='127.0.0.1:1234'")
}

func TestSubscriptionCloseClosesImagesUnderConductor(t *testing.T) {
	tc := newTestConductor(t)
	var unavailable []Image
	sub := addReadySubscription(t, tc, nil, func(img Image) { unavailable = append(unavailable, img) })
	ti := newTestImage(t)
	tc.imageFactory = func(int32, int64, string, int64, string, *atomic.Buffer, int32) Image { return ti.image }
	tc.OnAvailableImage(StreamId1, SessionId1, "log", "src", 1, sub.RegistrationID(), ti.CorrelationID())
	tc.flush()
	require.Equal(t, 1, sub.ImageCount())

	require.NoError(t, sub.Close())
	require.True(t, sub.IsClosed())
	require.True(t, ti.IsClosed())
	require.Equal(t, 0, sub.ImageCount())
	require.Equal(t, []Image{ti.image}, unavailable)
	require.Equal(t, 1, tc.proxy.called("RemoveSubscription"))

	require.NoError(t, sub.Close())
	require.Equal(t, 1, tc.proxy.called("RemoveSubscription"))
	require.Len(t, unavailable, 1)
}

// An image delivered by the conductor while Subscription.Close runs must either be closed with the subscription or
// never added; previously Close emptied the list outside the conductor lock and could leak a late image.
func TestSubscriptionCloseRacingImageAvailable(t *testing.T) {
	for i := 0; i < 200; i++ {
		tc := newTestConductor(t)
		sub := addReadySubscription(t, tc, nil, nil)
		img := newMockImage(CorrelationId1)
		img.On("SessionID").Return(SessionId1).Maybe()
		tc.imageFactory = func(int32, int64, string, int64, string, *atomic.Buffer, int32) Image { return img }
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			tc.adminLock.Lock()
			tc.OnAvailableImage(StreamId1, SessionId1, "log", "src", 1, sub.RegistrationID(), CorrelationId1)
			tc.unlock()
		}()
		go func() {
			defer wg.Done()
			require.NoError(t, sub.Close())
		}()
		wg.Wait()
		require.Equal(t, 0, sub.ImageCount(), "iteration %d", i)
	}
}

func TestForceCloseResourcesClosesImages(t *testing.T) {
	tc := newTestConductor(t)
	sub := addReadySubscription(t, tc, nil, nil)
	ti := newTestImage(t)
	tc.imageFactory = func(int32, int64, string, int64, string, *atomic.Buffer, int32) Image { return ti.image }
	tc.OnAvailableImage(StreamId1, SessionId1, "log", "src", 1, sub.RegistrationID(), ti.CorrelationID())
	tc.flush()

	tc.adminLock.Lock()
	tc.forceCloseResources()
	tc.unlock()
	require.True(t, sub.IsClosed())
	require.True(t, ti.IsClosed())
	require.Equal(t, 0, ti.Poll(func(*atomic.Buffer, int32, int32, *logbuffer.Header) {}, 10))
}

// Java ClientConductor.rejectImage awaits the driver response, so a driver error is reported to the caller.
func TestRejectImageAwaitsDriverResponse(t *testing.T) {
	tc := newTestConductor(t)
	tc.events.respond = func() { tc.OnErrorResponse(tc.proxy.nextCorr, 1, "unknown image") }
	err := tc.rejectImage(CorrelationId1, 0, "bad")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown image")

	tc.events.respond = func() { tc.OnOperationSuccess(tc.proxy.nextCorr) }
	require.NoError(t, tc.rejectImage(CorrelationId1, 0, "bad"))
	require.Equal(t, 2, tc.proxy.called("RejectImage"))
}

func TestTryResolveChannelEndpointPortJavaSemantics(t *testing.T) {
	cc := NewMockReceivingConductor(t)
	for _, channel := range []string{
		"aeron:ipc",
		"aeron:udp?endpoint=localhost:20121",
		"aeron:udp?endpoint=localhost:0|control-mode=manual",
		"aeron:udp?control-mode=manual",
	} {
		sub := NewSubscription(cc, channel, 5, 1001, 7, nil, nil)
		require.Equal(t, channel, sub.TryResolveChannelEndpointPort(), channel)
	}
}
