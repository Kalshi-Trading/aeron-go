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

// Package conductore2e exercises the ClientConductor against a real Java media driver
// (docker/docker-compose.yml archiving-driver). Run with:
//
//	AERON_E2E=1 AERON_DIR=/dev/shm/aeron-compose go test ./systests/conductore2e/ -v
//
// The driver-kill test additionally needs AERON_E2E_KILL=1 and docker access; it kills and restarts the container
// named by AERON_E2E_CONTAINER (default docker-archiving-driver-1).
package conductore2e

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/stretchr/testify/require"
)

func connect(t *testing.T, opts ...func(*aeron.Context)) *aeron.Aeron {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against the docker media driver")
	}
	dir := os.Getenv("AERON_DIR")
	if dir == "" {
		dir = "/dev/shm/aeron-compose"
	}
	ctx := aeron.NewContext().AeronDir(dir).MediaDriverTimeout(5 * time.Second).
		ErrorHandler(func(err error) { t.Logf("aeron error handler: %v", err) })
	for _, o := range opts {
		o(ctx)
	}
	a, err := aeron.Connect(ctx)
	require.NoError(t, err)
	return a
}

func await[T any](t *testing.T, what string, poll func() (*T, error)) *T {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		v, err := poll()
		require.NoError(t, err, what)
		if v != nil {
			return v
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
	return nil
}

func awaitTrue(t *testing.T, what string, cond func() bool) {
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func offer(t *testing.T, pub any, msg string) {
	buf := ab.MakeBuffer([]byte(msg))
	awaitTrue(t, "offer "+msg, func() bool {
		switch p := pub.(type) {
		case *aeron.Publication:
			return p.Offer(buf, 0, int32(len(msg)), nil) > 0
		case *aeron.ExclusivePublication:
			return p.Offer(buf, 0, int32(len(msg)), nil) > 0
		}
		t.Fatalf("unsupported publication type %T", pub)
		return false
	})
}

// receive polls sub until want messages are received, returning them.
func receive(t *testing.T, sub *aeron.Subscription, want int) []string {
	var got []string
	handler := func(buffer *ab.Buffer, offset int32, length int32, _ *logbuffer.Header) {
		got = append(got, string(buffer.GetBytesArray(offset, length)))
	}
	awaitTrue(t, fmt.Sprintf("%d messages on %s", want, sub.Channel()), func() bool {
		sub.Poll(handler, 10)
		return len(got) >= want
	})
	return got
}

// pollFor polls sub for a while and returns everything received.
func pollFor(sub *aeron.Subscription, d time.Duration) []string {
	var got []string
	handler := func(buffer *ab.Buffer, offset int32, length int32, _ *logbuffer.Header) {
		got = append(got, string(buffer.GetBytesArray(offset, length)))
	}
	for end := time.Now().Add(d); time.Now().Before(end); {
		sub.Poll(handler, 10)
		time.Sleep(time.Millisecond)
	}
	return got
}

func TestAsyncAddsAndData(t *testing.T) {
	a := connect(t)
	defer a.Close()
	const channel = "aeron:udp?endpoint=localhost:24325"

	var available, unavailable atomic.Int32
	subID, err := a.AsyncAddSubscriptionWithHandlers(channel, 1001,
		func(aeron.Image) { available.Add(1) }, func(aeron.Image) { unavailable.Add(1) })
	require.NoError(t, err)
	pubID, err := a.AsyncAddPublication(channel, 1001)
	require.NoError(t, err)
	exID, err := a.AsyncAddExclusivePublication("aeron:ipc", 1003)
	require.NoError(t, err)

	sub := await(t, "subscription", func() (*aeron.Subscription, error) { return a.GetSubscription(subID) })
	pub := await(t, "publication", func() (*aeron.Publication, error) { return a.GetPublication(pubID) })
	ex := await(t, "exclusive publication", func() (*aeron.ExclusivePublication, error) { return a.GetExclusivePublication(exID) })
	require.False(t, a.IsCommandActive(subID))
	require.NotEqual(t, ex.SessionID(), int32(0))

	awaitTrue(t, "publication connected", pub.IsConnected)
	for i := 0; i < 10; i++ {
		offer(t, pub, fmt.Sprintf("msg-%d", i))
	}
	got := receive(t, sub, 10)
	require.Equal(t, "msg-0", got[0])
	require.Equal(t, "msg-9", got[9])
	require.EqualValues(t, 1, available.Load())

	// An invalid channel is rejected asynchronously with a RegistrationError.
	badID, err := a.AsyncAddPublication("aeron:udp?endpoint=", 1001)
	require.NoError(t, err)
	var regErr *aeron.RegistrationError
	awaitTrue(t, "async registration error", func() bool {
		_, err := a.GetPublication(badID)
		return errors.As(err, &regErr)
	})
	t.Logf("async add error: %v", regErr)
	_, err = a.AddSubscription("aeron:bogus", 1)
	require.True(t, errors.As(err, &regErr), "blocking add returns RegistrationError: %v", err)

	sessionID, err := a.NextSessionID(1005)
	require.NoError(t, err)
	t.Logf("next session id: %d", sessionID)

	require.NoError(t, pub.Close())
	awaitTrue(t, "image unavailable after publication close", func() bool { return unavailable.Load() == 1 })
	require.NoError(t, a.AsyncRemoveSubscription(subID))
	require.True(t, sub.IsClosed())
}

func TestMultiDestinationCastByIDAndChannel(t *testing.T) {
	a := connect(t)
	defer a.Close()
	const ep1, ep2 = "aeron:udp?endpoint=localhost:24326", "aeron:udp?endpoint=localhost:24327"

	sub1, err := a.AddSubscription(ep1, 1010)
	require.NoError(t, err)
	sub2, err := a.AddSubscription(ep2, 1010)
	require.NoError(t, err)
	pub, err := a.AddExclusivePublication("aeron:udp?control-mode=manual", 1010)
	require.NoError(t, err)

	dest1, err := a.AddDestination(pub.RegistrationID(), ep1)
	require.NoError(t, err)
	corr, err := a.AsyncAddDestination(pub.RegistrationID(), ep2)
	require.NoError(t, err)
	awaitTrue(t, "async add destination", func() bool { return !a.IsCommandActive(corr) })

	awaitTrue(t, "both subscriptions connected", func() bool { return sub1.IsConnected() && sub2.IsConnected() })
	offer(t, pub, "to-both")
	require.Equal(t, []string{"to-both"}, receive(t, sub1, 1))
	require.Equal(t, []string{"to-both"}, receive(t, sub2, 1))

	require.NoError(t, a.RemoveDestinationByID(pub.RegistrationID(), dest1))
	offer(t, pub, "to-two")
	require.Equal(t, []string{"to-two"}, receive(t, sub2, 1))
	require.Empty(t, pollFor(sub1, 300*time.Millisecond), "removed destination must not receive")

	require.NoError(t, a.RemoveDestination(pub.RegistrationID(), ep2))
	offer(t, pub, "to-none")
	require.Empty(t, pollFor(sub2, 300*time.Millisecond), "removed destination must not receive")
	// The Java driver ignores removal of an unknown destination; an unknown publication is an error.
	err = a.RemoveDestination(pub.RegistrationID()+1_000_000, ep2)
	var regErr *aeron.RegistrationError
	require.True(t, errors.As(err, &regErr), "unknown publication: %v", err)
}

func TestMultiDestinationSubscription(t *testing.T) {
	a := connect(t)
	defer a.Close()
	const ep1, ep2 = "aeron:udp?endpoint=localhost:24328", "aeron:udp?endpoint=localhost:24329"

	sub, err := a.AddSubscription("aeron:udp?control-mode=manual", 1020)
	require.NoError(t, err)
	require.NoError(t, sub.AddDestination(ep1))
	corr, err := a.AsyncAddRcvDestination(sub.RegistrationID(), ep2)
	require.NoError(t, err)
	awaitTrue(t, "async add rcv destination", func() bool { return !a.IsCommandActive(corr) })

	pub1, err := a.AddExclusivePublication(ep1, 1020)
	require.NoError(t, err)
	pub2, err := a.AddExclusivePublication(ep2, 1020)
	require.NoError(t, err)
	awaitTrue(t, "publications connected", func() bool { return pub1.IsConnected() && pub2.IsConnected() })
	offer(t, pub1, "via-1")
	offer(t, pub2, "via-2")
	require.ElementsMatch(t, []string{"via-1", "via-2"}, receive(t, sub, 2))

	require.NoError(t, a.RemoveRcvDestination(sub.RegistrationID(), ep1))
	_, err = a.AsyncRemoveRcvDestination(sub.RegistrationID(), ep2)
	require.NoError(t, err)
}

func TestCountersAndCloseHandlers(t *testing.T) {
	a := connect(t)
	counter, err := a.AddCounterByLabel(1001, "conductor-e2e")
	require.NoError(t, err)
	counter.Counter().Set(42)
	require.EqualValues(t, 42, a.CounterReader().GetCounterValue(counter.ID()))

	static1, err := a.AddStaticCounterByLabel(1002, "conductor-e2e-static", 987654)
	require.NoError(t, err)
	static2, err := a.AddStaticCounterByLabel(1002, "conductor-e2e-static", 987654)
	require.NoError(t, err)
	require.Equal(t, static1.ID(), static2.ID(), "static counters are shared by (type, registration id)")

	var closed atomic.Int32
	a.AddCloseHandler(func() { closed.Add(1) })
	require.NoError(t, a.Close())
	require.NoError(t, a.Close())
	require.EqualValues(t, 1, closed.Load())
	require.True(t, counter.IsClosed())
	require.True(t, a.IsClosed())
	_, err = a.AddPublication("aeron:ipc", 1)
	require.ErrorIs(t, err, aeron.ErrClientClosed)
}

func TestDriverKillTimesOutAndClosesCleanly(t *testing.T) {
	if os.Getenv("AERON_E2E_KILL") != "1" {
		t.Skip("set AERON_E2E_KILL=1 to kill/restart the docker driver")
	}
	container := os.Getenv("AERON_E2E_CONTAINER")
	if container == "" {
		container = "docker-archiving-driver-1"
	}
	var handlerErrs atomic.Value
	var closed atomic.Int32
	a := connect(t, func(ctx *aeron.Context) {
		ctx.MediaDriverTimeout(2 * time.Second).CloseHandler(func() { closed.Add(1) }).
			ErrorHandler(func(err error) { handlerErrs.Store(err) })
	})
	sub, err := a.AddSubscription("aeron:ipc", 1030)
	require.NoError(t, err)
	pub, err := a.AddPublication("aeron:ipc", 1030)
	require.NoError(t, err)
	awaitTrue(t, "ipc connected", sub.IsConnected)
	offer(t, pub, "before-kill")
	receive(t, sub, 1)

	out, err := exec.Command("docker", "kill", container).CombinedOutput()
	require.NoError(t, err, string(out))
	defer func() {
		out, err := exec.Command("docker", "start", container).CombinedOutput()
		require.NoError(t, err, string(out))
	}()

	deadline := time.Now().Add(10 * time.Second)
	for !a.IsClosed() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.True(t, a.IsClosed(), "client terminates on driver timeout")
	var timeoutErr *aeron.DriverTimeoutError
	require.True(t, errors.As(a.TerminalError(), &timeoutErr), "terminal error: %v", a.TerminalError())
	reported, _ := handlerErrs.Load().(error)
	require.True(t, errors.As(reported, &timeoutErr), "error handler got %v", reported)
	t.Logf("driver timeout: %v", timeoutErr)
	awaitTrue(t, "close handler", func() bool { return closed.Load() == 1 })
	require.True(t, pub.IsClosed())
	require.True(t, sub.IsClosed())

	_, err = a.AddPublication("aeron:ipc", 1030)
	require.ErrorIs(t, err, aeron.ErrClientClosed)
	require.NoError(t, a.Close())
	require.EqualValues(t, 1, closed.Load())
}
