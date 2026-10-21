// Copyright 2026 Kalshi Trading
//
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

package aeron

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/broadcast"
	"github.com/lirm/aeron-go/aeron/util/memmap"
	"github.com/stretchr/testify/require"
)

func TestAsyncRemovePendingPublicationSendsRemove(t *testing.T) {
	tc := newTestConductor(t)
	regID, err := tc.AsyncAddPublication(Channel, StreamId1)
	require.NoError(t, err)

	require.NoError(t, tc.AsyncRemovePublication(regID))
	require.Equal(t, 1, tc.proxy.called("RemovePublication"), "driver must be told to drop the in-flight publication")
	require.False(t, tc.IsCommandActive(regID))
	require.True(t, tc.HasActiveCommands(), "the remove command itself is in flight")

	preloadLogBuffers(t, tc, regID)
	tc.OnNewPublication(StreamId1, SessionId1, 1, 2, "unused", regID, regID)
	_, err = tc.GetPublication(regID)
	require.ErrorIs(t, err, ErrUnknownRegistration, "a late ready must not resurrect a removed publication")
}

func TestAsyncRemovePendingSubscriptionSendsRemove(t *testing.T) {
	tc := newTestConductor(t)
	regID, err := tc.AsyncAddSubscription(Channel, StreamId1)
	require.NoError(t, err)

	require.NoError(t, tc.AsyncRemoveSubscription(regID))
	require.Equal(t, 1, tc.proxy.called("RemoveSubscription"))
	require.False(t, tc.IsCommandActive(regID))

	tc.OnSubscriptionReady(regID, 5)
	_, err = tc.GetSubscription(regID)
	require.ErrorIs(t, err, ErrUnknownRegistration)
}

func TestAsyncRemovePendingCounterIsNotActive(t *testing.T) {
	tc := newTestConductor(t)
	regID, err := tc.AddCounterByLabel(CounterTypeId, CounterLabel)
	require.NoError(t, err)

	require.NoError(t, tc.AsyncRemoveCounter(regID))
	require.Equal(t, 1, tc.proxy.called("RemoveCounter"))
	require.False(t, tc.IsCommandActive(regID))
}

func TestAsyncRemoveWrongResourceType(t *testing.T) {
	tc := newTestConductor(t)
	sub := addReadySubscription(t, tc, nil, nil)
	require.ErrorIs(t, tc.AsyncRemovePublication(sub.RegistrationID()), ErrWrongResourceType)
	require.ErrorIs(t, tc.AsyncRemoveCounter(sub.RegistrationID()), ErrWrongResourceType)
	require.False(t, sub.IsClosed())

	pubID, err := tc.AsyncAddPublication(Channel, StreamId1)
	require.NoError(t, err)
	require.ErrorIs(t, tc.AsyncRemoveSubscription(pubID), ErrWrongResourceType)
	_, err = tc.GetSubscription(pubID)
	require.ErrorIs(t, err, ErrWrongResourceType)

	require.NoError(t, tc.AsyncRemoveSubscription(424242), "unknown ids are ignored")
	require.NoError(t, tc.AsyncRemovePublication(NullValue))
}

func TestRemoveCommandsAreTracked(t *testing.T) {
	tc := newTestConductor(t)
	sub := addReadySubscription(t, tc, nil, nil)
	require.False(t, tc.HasActiveCommands())

	require.NoError(t, sub.Close())
	require.True(t, tc.HasActiveCommands(), "Java adds remove correlation ids to asyncCommandIdSet")
	removeCorr := tc.proxy.nextCorr
	require.True(t, tc.IsCommandActive(removeCorr))
	tc.OnOperationSuccess(removeCorr)
	require.False(t, tc.HasActiveCommands())

	sub2 := addReadySubscription(t, tc, nil, nil)
	require.NoError(t, sub2.Close())
	tc.OnErrorResponse(tc.proxy.nextCorr, 1, "unknown subscription")
	tc.flush()
	require.Len(t, tc.errors(), 1, "an error for a remove is reported to the error handler")
}

func TestCommandQueriesAfterClose(t *testing.T) {
	tc := newTestConductor(t)
	regID, err := tc.AsyncAddSubscription(Channel, StreamId1)
	require.NoError(t, err)
	require.NoError(t, tc.Close())
	require.False(t, tc.IsCommandActive(regID), "Java isCommandActive returns false once closed")
	require.False(t, tc.HasActiveCommands())
}

func TestPollingGetServicesConductor(t *testing.T) {
	tc := newTestConductor(t)
	regID, err := tc.AsyncAddPublication(Channel, StreamId1)
	require.NoError(t, err)
	preloadLogBuffers(t, tc, regID)
	tc.events.respond = func() { tc.OnNewPublication(StreamId1, SessionId1, 1, 2, "unused", regID, regID) }

	pub, err := tc.GetPublication(regID)
	require.NoError(t, err)
	require.NotNil(t, pub, "GetPublication services the conductor like Java, so it progresses with an agent invoker")

	subID, err := tc.AsyncAddSubscription(Channel, StreamId1)
	require.NoError(t, err)
	tc.events.respond = func() { tc.OnSubscriptionReady(subID, 3) }
	require.False(t, tc.IsCommandActive(subID), "IsCommandActive services the conductor like Java")
}

func TestDriverEventHandlerPanicDoesNotTerminate(t *testing.T) {
	tc := newTestConductor(t)
	tc.events.respond = func() { panic("handler bug") }
	_, terminated := tc.doWork()
	require.False(t, terminated)
	require.False(t, tc.IsClosed(), "only an invalid (lapped) adapter terminates the client in Java")
	require.Len(t, tc.errors(), 1)

	tc.events.respond = func() { panic(fmt.Errorf("lapped: %w", broadcast.ErrUnableToKeepUp)) }
	_, terminated = tc.doWork()
	require.True(t, terminated)
	require.True(t, tc.IsClosed())
}

func TestHeartbeatCounterSearchedEveryServiceUntilFound(t *testing.T) {
	tc := newTestConductor(t)
	tc.proxy.keepaliveMs = func() int64 { return time.Now().UnixMilli() }
	now := time.Now().UnixNano()
	before := now - tc.keepAliveIntervalNs - 1
	tc.timeOfLastKeepaliveNs = before
	_, err := tc.checkLiveness(now)
	require.NoError(t, err)
	require.Nil(t, tc.heartbeatTimestamp)
	require.Equal(t, before, tc.timeOfLastKeepaliveNs,
		"Java only advances timeOfLastKeepaliveNs once the heartbeat counter is found and updated")
}

func TestBlockingCallUsesAwaitIdle(t *testing.T) {
	tc := newTestConductor(t)
	idles := 0
	tc.awaitingIdleStrategy = idleFunc(func() {
		idles++
		if idles == 3 {
			tc.events.respond = func() { tc.OnSubscriptionReady(1001, 7) }
		}
	})
	sub, err := tc.AddSubscription(Channel, StreamId1)
	require.NoError(t, err)
	require.NotNil(t, sub)
	require.Equal(t, 3, idles, "awaiting idle (or the driver agent invoker) runs between polls")
}

type closeTrackingImage struct {
	*MockImage
	marked bool
}

func (i *closeTrackingImage) markClosed() { i.marked = true }

func TestImagesMarkedClosedOnSubscriptionClose(t *testing.T) {
	for _, viaClientClose := range []bool{false, true} {
		tc := newTestConductor(t)
		sub := addReadySubscription(t, tc, nil, nil)
		img := &closeTrackingImage{MockImage: newMockImage(CorrelationId1)}
		tc.imageFactory = func(int32, int64, string, int64, string, *atomic.Buffer, int32) Image { return img }
		tc.OnAvailableImage(StreamId1, SessionId1, "log", "src", 1, sub.RegistrationID(), CorrelationId1)
		tc.flush()
		if viaClientClose {
			require.NoError(t, tc.Close())
		} else {
			require.NoError(t, sub.Close())
		}
		require.True(t, img.marked,
			"Java Subscription.internalClose closes each image before its log buffers linger (clientClose=%v)",
			viaClientClose)
	}
}

func TestInvokerTerminationRunsOnClose(t *testing.T) {
	a := &Aeron{context: NewContext()}
	tc := newTestConductorOn(t, &a.conductor)
	a.invoker = &ConductorAgentInvoker{aeron: a}
	closed := 0
	tc.AddCloseHandler(func() { closed++ })

	tc.timeOfLastServiceNs -= tc.interServiceTimeoutNs + int64(time.Second)
	a.invoker.Invoke()
	require.True(t, tc.IsClosed())
	require.Equal(t, 1, closed, "Java AgentInvoker calls ClientConductor.onClose on termination")
	require.Equal(t, 1, tc.proxy.clientClose)
}

func TestCloseFromConductorCallback(t *testing.T) {
	tc := newTestConductor(t)
	closeErr := make(chan error, 1)
	afterExit := make(chan bool, 1)
	tc.events.respond = func() {
		tc.queueCallback(func() {
			closeErr <- tc.Close()
			tc.runAfterExit(func() { afterExit <- tc.IsClosed() })
		})
	}
	tc.Start(idleStrategyForTest{})

	select {
	case err := <-closeErr:
		require.ErrorIs(t, err, ErrCloseFromCallback)
	case <-time.After(2 * time.Second):
		t.Fatal("Close from a conductor callback deadlocked")
	}
	select {
	case closedAtExit := <-afterExit:
		require.True(t, closedAtExit, "post-exit hook runs after onClose")
	case <-time.After(5 * time.Second):
		t.Fatal("post-exit hook did not run")
	}
	<-tc.doneCh
	require.NoError(t, tc.Close())
	require.Equal(t, 1, tc.proxy.clientClose)
}

type idleFunc func()

func (f idleFunc) Idle(int) { f() }

// Close from another goroutine while a conductor callback runs is not a close from a callback: like Java (which
// blocks on the client lock) it must wait for the conductor to finish closing rather than return ErrCloseFromCallback.
func TestCloseConcurrentWithConductorCallback(t *testing.T) {
	tc := newTestConductor(t)
	inCallback := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	tc.events.respond = func() {
		once.Do(func() {
			tc.queueCallback(func() {
				close(inCallback)
				<-release
			})
		})
	}
	tc.Start(idleStrategyForTest{})
	<-inCallback

	closeErr := make(chan error, 1)
	go func() { closeErr <- tc.Close() }()
	select {
	case err := <-closeErr:
		t.Fatalf("Close returned %v while the conductor was still running a callback", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-closeErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not complete after the callback returned")
	}
	require.True(t, tc.IsClosed())
	require.Equal(t, 1, tc.proxy.clientClose)
}

// Aeron.Close requested from a conductor callback completes asynchronously; a later Close from an application
// goroutine must, like Java's idempotent close, wait for that close and succeed instead of repeating
// ErrCloseFromCallback.
func TestAeronCloseAfterCloseFromCallback(t *testing.T) {
	cnc, err := memmap.NewFile(filepath.Join(t.TempDir(), "cnc.dat"), 0, 4096)
	require.NoError(t, err)
	a := &Aeron{context: NewContext(), cncFile: cnc}
	tc := newTestConductorOn(t, &a.conductor)
	closeErr := make(chan error, 1)
	var once sync.Once
	tc.events.respond = func() {
		once.Do(func() { tc.queueCallback(func() { closeErr <- a.Close() }) })
	}
	tc.Start(idleStrategyForTest{})
	select {
	case err := <-closeErr:
		require.ErrorIs(t, err, ErrCloseFromCallback)
	case <-time.After(2 * time.Second):
		t.Fatal("Close from a conductor callback deadlocked")
	}
	require.NoError(t, a.Close())
	require.True(t, a.IsClosed())
	require.NoError(t, a.Close())
}
