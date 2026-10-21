/*
Copyright 2026 Kalshi Trading

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aeron

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	StreamId1      = int32(1002)
	CorrelationId1 = int64(2000)
	SessionId1     = int32(13)
	CounterTypeId  = int32(102)
	CounterLabel   = "counter label"
	CounterId      = int32(3)
	testClientID   = int64(77)
)

type fakeDriverProxy struct {
	mu          sync.Mutex
	nextCorr    int64
	keepaliveMs func() int64
	calls       []string
	clientClose int
	sendErr     error
}

func (p *fakeDriverProxy) record(name string) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, name)
	if p.sendErr != nil {
		return 0, p.sendErr
	}
	p.nextCorr++
	return p.nextCorr, nil
}

func (p *fakeDriverProxy) ClientID() int64 { return testClientID }
func (p *fakeDriverProxy) TimeOfLastDriverKeepalive() int64 {
	if p.keepaliveMs != nil {
		return p.keepaliveMs()
	}
	return time.Now().UnixMilli()
}
func (p *fakeDriverProxy) NextCorrelationID() int64 { c, _ := p.record("NextCorrelationID"); return c }
func (p *fakeDriverProxy) AddSubscription(string, int32) (int64, error) {
	return p.record("AddSubscription")
}
func (p *fakeDriverProxy) RemoveSubscription(int64) (int64, error) {
	return p.record("RemoveSubscription")
}
func (p *fakeDriverProxy) AddPublication(string, int32) (int64, error) {
	return p.record("AddPublication")
}
func (p *fakeDriverProxy) AddExclusivePublication(string, int32) (int64, error) {
	return p.record("AddExclusivePublication")
}
func (p *fakeDriverProxy) RemovePublication(int64, bool) (int64, error) {
	return p.record("RemovePublication")
}
func (p *fakeDriverProxy) ClientClose() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clientClose++
	return nil
}
func (p *fakeDriverProxy) AddDestination(int64, string) (int64, error) {
	return p.record("AddDestination")
}
func (p *fakeDriverProxy) RemoveDestination(int64, string) (int64, error) {
	return p.record("RemoveDestination")
}
func (p *fakeDriverProxy) RemoveDestinationByID(int64, int64) (int64, error) {
	return p.record("RemoveDestinationByID")
}
func (p *fakeDriverProxy) AddRcvDestination(int64, string) (int64, error) {
	return p.record("AddRcvDestination")
}
func (p *fakeDriverProxy) RemoveRcvDestination(int64, string) (int64, error) {
	return p.record("RemoveRcvDestination")
}
func (p *fakeDriverProxy) AddCounter(int32, *atomic.Buffer, int32, int32, *atomic.Buffer, int32, int32) (int64, error) {
	return p.record("AddCounter")
}
func (p *fakeDriverProxy) AddCounterByLabel(int32, string) (int64, error) {
	return p.record("AddCounterByLabel")
}
func (p *fakeDriverProxy) RemoveCounter(int64) (int64, error) { return p.record("RemoveCounter") }
func (p *fakeDriverProxy) AddStaticCounterByLabel(int32, string, int64) (int64, error) {
	return p.record("AddStaticCounter")
}
func (p *fakeDriverProxy) AddStaticCounter(int32, *atomic.Buffer, int32, int32, *atomic.Buffer, int32, int32,
	int64) (int64, error) {
	return p.record("AddStaticCounter")
}
func (p *fakeDriverProxy) NextAvailableSessionID(int32) (int64, error) {
	return p.record("GetNextAvailableSessionID")
}

func (p *fakeDriverProxy) called(name string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.calls {
		if c == name {
			n++
		}
	}
	return n
}

// fakeEvents simulates the driver's broadcast: respond is invoked (with the lock held, like the real adapter) on
// each ReceiveMessages call.
type fakeEvents struct {
	respond func()
}

func (f *fakeEvents) ReceiveMessages() int {
	if f.respond != nil {
		r := f.respond
		f.respond = nil
		r()
		return 1
	}
	return 0
}

type testConductor struct {
	*ClientConductor
	proxy  *fakeDriverProxy
	events *fakeEvents
	errs   []error
	errMu  sync.Mutex
}

func newTestConductor(t *testing.T) *testConductor {
	metaBuf := atomic.MakeBuffer(make([]byte, 256*1024))
	meta := counters.InitAndWrapMetaData(metaBuf, 0, 0, 0, 24*1024, 24*1024, 0)
	tc := &testConductor{ClientConductor: new(ClientConductor), proxy: &fakeDriverProxy{nextCorr: 1000},
		events: &fakeEvents{}}
	tc.Init(tc.proxy, nil, 10*time.Second, 200*time.Millisecond, time.Second, 50*time.Millisecond, meta)
	tc.driverListenerAdapter = tc.events
	tc.errorHandler = func(err error) {
		tc.errMu.Lock()
		defer tc.errMu.Unlock()
		tc.errs = append(tc.errs, err)
	}
	return tc
}

// flush dispatches queued callbacks for events delivered directly in tests.
func (tc *testConductor) flush() {
	tc.adminLock.Lock()
	tc.unlock()
}

func (tc *testConductor) errors() []error {
	tc.errMu.Lock()
	defer tc.errMu.Unlock()
	return append([]error(nil), tc.errs...)
}

func preloadLogBuffers(t *testing.T, tc *testConductor, regID int64) {
	lb, err := logbuffer.NewTestingLogbuffer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = logbuffer.RemoveTestingLogbufferFile() })
	tc.logBuffersByID[regID] = lb
}

func TestAsyncAddPublicationPollsUntilReady(t *testing.T) {
	tc := newTestConductor(t)
	regID, err := tc.AsyncAddPublication(Channel, StreamId1)
	require.NoError(t, err)
	require.True(t, tc.IsCommandActive(regID))

	pub, err := tc.GetPublication(regID)
	require.NoError(t, err)
	require.Nil(t, pub)

	preloadLogBuffers(t, tc, regID)
	tc.OnNewPublication(StreamId1, SessionId1, 1, 2, "unused", regID, regID)

	pub, err = tc.GetPublication(regID)
	require.NoError(t, err)
	require.NotNil(t, pub)
	require.Equal(t, regID, pub.RegistrationID())
	require.Equal(t, SessionId1, pub.SessionID())
	require.False(t, tc.IsCommandActive(regID))

	_, err = tc.GetExclusivePublication(regID)
	require.Error(t, err, "concurrent publication must not be returned as exclusive")

	require.NoError(t, pub.Close())
	require.True(t, pub.IsClosed())
	require.Equal(t, 1, tc.proxy.called("RemovePublication"))
	require.Len(t, tc.lingering, 1, "log buffers linger after explicit close")
}

func TestAsyncAddExclusivePublicationError(t *testing.T) {
	tc := newTestConductor(t)
	regID, err := tc.AsyncAddExclusivePublication(Channel, StreamId1)
	require.NoError(t, err)

	tc.OnErrorResponse(regID, 3, "invalid channel")
	pub, err := tc.GetExclusivePublication(regID)
	require.Nil(t, pub)
	var regErr *RegistrationError
	require.True(t, errors.As(err, &regErr))
	require.Equal(t, int32(3), regErr.ErrorCodeValue)
	require.Equal(t, regID, regErr.CorrelationID)

	_, err = tc.GetExclusivePublication(regID)
	require.ErrorIs(t, err, ErrUnknownRegistration)
	require.Empty(t, tc.errors(), "async add errors are returned by Get, not the error handler")
}

func TestAsyncAddTimesOutWithoutResponse(t *testing.T) {
	tc := newTestConductor(t)
	now := time.Now().UnixNano()
	tc.nanoClock = func() int64 { return now }
	regID, err := tc.AsyncAddSubscription(Channel, StreamId1)
	require.NoError(t, err)

	now += tc.driverTimeoutNs + 1
	_, err = tc.GetSubscription(regID)
	var timeoutErr *DriverTimeoutError
	require.True(t, errors.As(err, &timeoutErr))
	require.False(t, tc.IsCommandActive(regID))
}

func TestBlockingAddSubscription(t *testing.T) {
	tc := newTestConductor(t)
	tc.events.respond = func() { tc.OnSubscriptionReady(1001, 7) }
	sub, err := tc.AddSubscription(Channel, StreamId1)
	require.NoError(t, err)
	require.Equal(t, int64(1001), sub.RegistrationID())
	require.Equal(t, int32(7), sub.ChannelStatusId())
	require.False(t, tc.HasActiveCommands())
}

func TestBlockingAddSubscriptionError(t *testing.T) {
	tc := newTestConductor(t)
	tc.events.respond = func() { tc.OnErrorResponse(1001, 11, "boom") }
	sub, err := tc.AddSubscription(Channel, StreamId1)
	require.Nil(t, sub)
	var regErr *RegistrationError
	require.True(t, errors.As(err, &regErr))
	require.Equal(t, int32(11), regErr.ErrorCodeValue)
	require.Empty(t, tc.resources)
}

func TestBlockingAddTimesOut(t *testing.T) {
	tc := newTestConductor(t)
	start := time.Now()
	pub, err := tc.AddPublication(Channel, StreamId1)
	require.Nil(t, pub)
	var timeoutErr *DriverTimeoutError
	require.True(t, errors.As(err, &timeoutErr), "got %v", err)
	require.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond)
	require.Empty(t, tc.resources)
	require.False(t, tc.IsClosed(), "a command timeout does not terminate the client")
}

func TestSendFailureIsReturned(t *testing.T) {
	tc := newTestConductor(t)
	tc.proxy.sendErr = errors.New("ring buffer full")
	_, err := tc.AsyncAddPublication(Channel, StreamId1)
	require.EqualError(t, err, "ring buffer full")
	require.Empty(t, tc.resources)
}

func addReadySubscription(t *testing.T, tc *testConductor, onAvailable, onUnavailable func(Image)) *Subscription {
	regID, err := tc.AsyncAddSubscriptionWithHandlers(Channel, StreamId1, onAvailable, onUnavailable)
	require.NoError(t, err)
	tc.OnSubscriptionReady(regID, 5)
	sub, err := tc.GetSubscription(regID)
	require.NoError(t, err)
	require.NotNil(t, sub)
	return sub
}

func newMockImage(corrID int64) *MockImage {
	img := new(MockImage)
	img.On("CorrelationID").Return(corrID)
	img.On("Close").Return(nil)
	return img
}

func TestImageLifecycleHandlersRunOutsideLock(t *testing.T) {
	tc := newTestConductor(t)
	var available, unavailable []Image
	sub := addReadySubscription(t, tc,
		func(img Image) { available = append(available, img); tc.HasActiveCommands() }, // re-entrant call
		func(img Image) { unavailable = append(unavailable, img); tc.HasActiveCommands() })
	img := newMockImage(CorrelationId1)
	tc.imageFactory = func(int32, int64, string, int64, string, *atomic.Buffer, int32) Image { return img }

	tc.OnAvailableImage(StreamId1, SessionId1, "log", "src", 1, sub.RegistrationID(), CorrelationId1)
	tc.OnAvailableImage(StreamId1, SessionId1, "log", "src", 1, sub.RegistrationID(), CorrelationId1) // duplicate
	tc.OnAvailableImage(StreamId1, SessionId1, "log", "src", 1, 999, CorrelationId1+1)                // unknown sub
	tc.flush()
	require.Len(t, available, 1)
	require.Equal(t, 1, sub.ImageCount())

	tc.OnUnavailableImage(CorrelationId1+5, sub.RegistrationID()) // unknown image
	tc.OnUnavailableImage(CorrelationId1, sub.RegistrationID())
	tc.flush()
	require.Len(t, unavailable, 1)
	require.Equal(t, 0, sub.ImageCount())
	require.Len(t, tc.lingering, 1)

	tc.nanoClock = func() int64 { return time.Now().UnixNano() + int64(time.Hour) }
	tc.checkLingeringResources(tc.nanoClock())
	img.AssertCalled(t, "Close")
	require.Empty(t, tc.lingering)
}

func TestImageFactoryPanicIsReported(t *testing.T) {
	tc := newTestConductor(t)
	sub := addReadySubscription(t, tc, nil, nil)
	tc.imageFactory = func(int32, int64, string, int64, string, *atomic.Buffer, int32) Image { panic("no file") }
	tc.OnAvailableImage(StreamId1, SessionId1, "log", "src", 1, sub.RegistrationID(), CorrelationId1)
	tc.flush()
	require.Len(t, tc.errors(), 1)
	require.Equal(t, 0, sub.ImageCount())
}

func TestDestinations(t *testing.T) {
	tc := newTestConductor(t)
	corrID, err := tc.AsyncAddDestination(1, "aeron:udp?endpoint=localhost:4000")
	require.NoError(t, err)
	require.True(t, tc.IsCommandActive(corrID))
	tc.OnOperationSuccess(corrID)
	require.False(t, tc.IsCommandActive(corrID))

	tc.events.respond = func() { tc.OnOperationSuccess(tc.proxy.nextCorr) }
	require.NoError(t, tc.RemoveDestinationByID(1, corrID))
	tc.events.respond = func() { tc.OnOperationSuccess(tc.proxy.nextCorr) }
	destID, err := tc.AddDestinationWithID(1, "aeron:udp?endpoint=localhost:4001")
	require.NoError(t, err)
	require.Equal(t, tc.proxy.nextCorr, destID)
	tc.events.respond = func() { tc.OnErrorResponse(tc.proxy.nextCorr, 11, "unknown destination") }
	err = tc.RemoveRcvDestination(2, "aeron:udp?endpoint=localhost:4002")
	var regErr *RegistrationError
	require.True(t, errors.As(err, &regErr))

	// Async destination failures go to the error handler.
	corrID, err = tc.AsyncRemoveRcvDestination(2, "aeron:udp?endpoint=localhost:4003")
	require.NoError(t, err)
	tc.OnErrorResponse(corrID, 11, "unknown destination")
	tc.flush()
	require.Len(t, tc.errors(), 1)
	require.False(t, tc.HasActiveCommands())
}

type recordingCounterHandler struct {
	ids []int64
}

func (h *recordingCounterHandler) Handle(_ *counters.Reader, registrationId int64, _ int32) {
	h.ids = append(h.ids, registrationId)
}

func TestCounterLifecycle(t *testing.T) {
	tc := newTestConductor(t)
	availableH, unavailableH := &recordingCounterHandler{}, &recordingCounterHandler{}
	tc.AddAvailableCounterHandler(availableH)
	tc.AddUnavailableCounterHandler(unavailableH)

	firstID, err := tc.AddCounterByLabel(CounterTypeId, CounterLabel)
	require.NoError(t, err)
	tc.OnAvailableCounter(firstID, CounterId)
	tc.flush()
	counter, err := tc.GetCounter(firstID)
	require.NoError(t, err)
	require.Equal(t, CounterId, counter.Id())
	require.Equal(t, []int64{counter.RegistrationId()}, availableH.ids)

	got, err := tc.GetCounter(counter.RegistrationId())
	require.NoError(t, err)
	require.Same(t, counter, got)

	tc.OnUnavailableCounter(counter.RegistrationId(), CounterId)
	tc.flush()
	require.True(t, counter.IsClosed())
	require.Equal(t, []int64{counter.RegistrationId()}, unavailableH.ids)

	require.True(t, tc.RemoveUnavailableCounterHandler(unavailableH), "remove by value must match")
	require.False(t, tc.RemoveUnavailableCounterHandler(unavailableH))

	regID, err := tc.AddCounterByLabel(CounterTypeId, CounterLabel)
	require.NoError(t, err)
	tc.OnAvailableCounter(regID, CounterId)
	c2, err := tc.GetCounter(regID)
	require.NoError(t, err)
	require.NoError(t, c2.Close())
	require.Equal(t, 1, tc.proxy.called("RemoveCounter"))
}

func TestStaticCounterIsNotRemoved(t *testing.T) {
	tc := newTestConductor(t)
	corrID, err := tc.AddStaticCounterByLabel(CounterTypeId, CounterLabel, 555)
	require.NoError(t, err)
	tc.OnStaticCounter(corrID, CounterId)
	counter, err := tc.GetCounter(corrID)
	require.NoError(t, err)
	require.Equal(t, CounterId, counter.Id())
	require.NoError(t, counter.Close())
	require.Equal(t, 0, tc.proxy.called("RemoveCounter"))
}

func TestNextSessionID(t *testing.T) {
	tc := newTestConductor(t)
	tc.events.respond = func() { tc.OnNextAvailableSessionID(tc.proxy.nextCorr, 4242) }
	id, err := tc.NextSessionID(StreamId1)
	require.NoError(t, err)
	require.Equal(t, int32(4242), id)
}

func TestPublicationErrorFrameRouted(t *testing.T) {
	tc := newTestConductor(t)
	var frames []*PublicationErrorFrame
	tc.publicationErrorFrameHandler = func(f *PublicationErrorFrame) { frames = append(frames, f) }
	regID, err := tc.AsyncAddPublication(Channel, StreamId1)
	require.NoError(t, err)
	preloadLogBuffers(t, tc, regID)
	tc.OnNewPublication(StreamId1, SessionId1, 1, 2, "unused", regID, regID)

	buf, n := encodePublicationErrorFrame(regID, 9, "bad")
	tc.OnPublicationError(buf, 0, n)
	buf, n = encodePublicationErrorFrame(regID+100, 9, "other")
	tc.OnPublicationError(buf, 0, n)
	tc.flush()
	require.Len(t, frames, 1)
	require.Equal(t, "bad", frames[0].ErrorMessage)
}

func TestChannelEndpointErrorReported(t *testing.T) {
	tc := newTestConductor(t)
	sub := addReadySubscription(t, tc, nil, nil)
	tc.OnChannelEndpointError(int64(sub.ChannelStatusId()), "address in use")
	tc.flush()
	errs := tc.errors()
	require.Len(t, errs, 1)
	var epErr *ChannelEndpointError
	require.True(t, errors.As(errs[0], &epErr))
	require.Equal(t, sub.ChannelStatusId(), epErr.StatusIndicatorID)
}

func TestDriverTimeoutTerminatesAndForceCloses(t *testing.T) {
	tc := newTestConductor(t)
	sub := addReadySubscription(t, tc, nil, nil)
	closed := 0
	tc.AddCloseHandler(func() { closed++ })

	tc.proxy.keepaliveMs = func() int64 { return time.Now().UnixMilli() - 10_000 }
	tc.timeOfLastServiceNs -= int64(time.Second)
	tc.timeOfLastKeepaliveNs -= int64(time.Second)
	workCount, terminated := tc.doWork()
	require.Equal(t, 0, workCount)
	require.True(t, terminated)
	require.True(t, tc.IsClosed())
	require.True(t, sub.IsClosed(), "resources are force closed on driver timeout")
	var timeoutErr *DriverTimeoutError
	require.True(t, errors.As(tc.TerminalError(), &timeoutErr))
	require.Len(t, tc.errors(), 1)

	_, err := tc.AsyncAddPublication(Channel, StreamId1)
	require.ErrorIs(t, err, ErrClientClosed)

	require.NoError(t, tc.Close())
	require.NoError(t, tc.Close())
	require.Equal(t, 1, closed)
	require.Equal(t, 1, tc.proxy.clientClose)
}

func TestDriverShutdownMessage(t *testing.T) {
	tc := newTestConductor(t)
	tc.proxy.keepaliveMs = func() int64 { return 0 }
	tc.timeOfLastServiceNs -= int64(time.Second)
	tc.timeOfLastKeepaliveNs -= int64(time.Second)
	_, err := tc.service(nullValue)
	require.ErrorContains(t, err, "has been shutdown")
}

func TestConductorServiceTimeout(t *testing.T) {
	tc := newTestConductor(t)
	tc.timeOfLastServiceNs -= tc.interServiceTimeoutNs + int64(time.Second)
	_, err := tc.service(nullValue)
	var svcErr *ConductorServiceTimeoutError
	require.True(t, errors.As(err, &svcErr))
	require.True(t, tc.IsClosed())
}

func TestClientTimeoutFromDriver(t *testing.T) {
	tc := newTestConductor(t)
	tc.OnClientTimeout(testClientID + 1)
	require.False(t, tc.IsClosed())
	tc.OnClientTimeout(testClientID)
	tc.flush()
	require.True(t, tc.IsClosed())
	errs := tc.errors()
	require.Len(t, errs, 1)
	var ctErr *ClientTimeoutError
	require.True(t, errors.As(errs[0], &ctErr))
}

func TestCloseSemantics(t *testing.T) {
	tc := newTestConductor(t)
	var unavailable int
	sub := addReadySubscription(t, tc, nil, func(Image) { unavailable++ })
	img := newMockImage(CorrelationId1)
	tc.imageFactory = func(int32, int64, string, int64, string, *atomic.Buffer, int32) Image { return img }
	tc.OnAvailableImage(StreamId1, SessionId1, "log", "src", 1, sub.RegistrationID(), CorrelationId1)

	order := []string{}
	id := tc.AddCloseHandler(func() { order = append(order, "removed") })
	tc.AddCloseHandler(func() { order = append(order, "h") })
	require.True(t, tc.RemoveCloseHandler(id))

	require.NoError(t, tc.Close())
	require.Equal(t, []string{"h"}, order)
	require.True(t, sub.IsClosed())
	require.Equal(t, 1, unavailable)
	img.AssertCalled(t, "Close")
	require.Equal(t, 1, tc.proxy.clientClose)
	require.NoError(t, sub.Close(), "closing a resource after client close is a no-op")
	require.Equal(t, 0, tc.proxy.called("RemoveSubscription"))
}

func TestStartedConductorConcurrentUseAndClose(t *testing.T) {
	tc := newTestConductor(t)
	tc.Start(idleStrategyForTest{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				regID, err := tc.AsyncAddSubscription(Channel, StreamId1)
				if err != nil {
					return
				}
				tc.adminLock.Lock()
				tc.OnSubscriptionReady(regID, 1)
				tc.unlock()
				if sub, err := tc.GetSubscription(regID); err == nil && sub != nil {
					_ = sub.Close()
				}
			}
		}()
	}
	wg.Wait()
	require.NoError(t, tc.Close())
	require.True(t, tc.IsClosed())
	require.Equal(t, 1, tc.proxy.clientClose)
}

type idleStrategyForTest struct{}

func (idleStrategyForTest) Idle(int) { time.Sleep(time.Millisecond) }

var _ = mock.Anything

func encodePublicationErrorFrame(regID int64, code int32, msg string) (*atomic.Buffer, int32) {
	length := int32(pubErrErrorMessageOffset + 4 + len(msg))
	buf := atomic.MakeBuffer(make([]byte, length))
	buf.PutInt64(pubErrRegistrationIDOffset, regID)
	buf.PutInt32(pubErrErrorCodeOffset, code)
	buf.PutInt32(pubErrErrorMessageOffset, int32(len(msg)))
	b := []byte(msg)
	buf.PutBytesArray(pubErrErrorMessageOffset+4, &b, 0, int32(len(msg)))
	return buf, length
}

func TestAsyncRemovePendingRegistrationsSendsRemove(t *testing.T) {
	tc := newTestConductor(t)
	pubID, err := tc.AsyncAddPublication(Channel, StreamId1)
	require.NoError(t, err)
	exID, err := tc.AsyncAddExclusivePublication(Channel, StreamId1)
	require.NoError(t, err)
	subID, err := tc.AsyncAddSubscription(Channel, StreamId1)
	require.NoError(t, err)

	require.NoError(t, tc.AsyncRemovePublication(pubID))
	require.NoError(t, tc.AsyncRemovePublication(exID))
	require.NoError(t, tc.AsyncRemoveSubscription(subID))
	require.Equal(t, 2, tc.proxy.called("RemovePublication"))
	require.Equal(t, 1, tc.proxy.called("RemoveSubscription"))
	require.False(t, tc.IsCommandActive(pubID))
	require.False(t, tc.IsCommandActive(subID))

	// Late driver responses for the abandoned registrations are ignored.
	tc.OnNewPublication(StreamId1, SessionId1, 1, 2, "unused", pubID, pubID)
	tc.OnSubscriptionReady(subID, 3)
	_, err = tc.GetPublication(pubID)
	require.ErrorIs(t, err, ErrUnknownRegistration)
	_, err = tc.GetSubscription(subID)
	require.ErrorIs(t, err, ErrUnknownRegistration)

	// A second remove is a no-op.
	require.NoError(t, tc.AsyncRemovePublication(pubID))
	require.NoError(t, tc.AsyncRemoveSubscription(subID))
	require.Equal(t, 2, tc.proxy.called("RemovePublication"))
	require.Equal(t, 1, tc.proxy.called("RemoveSubscription"))
}
