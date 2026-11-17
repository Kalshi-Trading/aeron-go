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

package systests

import (
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/archive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression tests for three behaviours a Java 1.53.3 driver and archive depend on, which ktgo carried as patches
// on the pre-parity client. Run with:
//
//	docker compose -f docker/docker-compose.yml up -d --build archiving-driver
//	AERON_E2E=1 go test -race ./systests/ -run TestJava153 -v

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	out := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		out <- string(b)
	}()
	stdout := os.Stdout
	os.Stdout = w
	func() {
		defer func() { os.Stdout = stdout }()
		fn()
	}()
	require.NoError(t, w.Close())
	return <-out
}

// A 1.53 driver sends ON_STATIC_COUNTER, ON_NEXT_AVAILABLE_SESSION_ID and ON_PUBLICATION_ERROR, which older
// clients treated as fatal and exited the process on.
func TestJava153DriverEventsKeepClientRunning(t *testing.T) {
	dir := countersE2EDir(t)
	a := connectE2E(t, dir)

	static, err := a.AddStaticCounterByLabel(1101, "java153-static", time.Now().UnixNano())
	require.NoError(t, err)
	require.NotNil(t, static)

	_, err = a.NextSessionID(4101)
	require.NoError(t, err)

	channel := "aeron:udp?endpoint=localhost:24101"
	sub, err := a.AddSubscription(channel, 4101)
	require.NoError(t, err)
	pub, err := a.AddPublication(channel, 4101)
	require.NoError(t, err)

	var image aeron.Image
	require.Eventually(t, func() bool {
		if sub.ImageCount() == 0 {
			return false
		}
		image = sub.ImageAtIndex(0)
		return image != nil
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, sub.RejectImage(image.CorrelationID(), 0, "java153 e2e reject"))

	payload := []byte("after driver events")
	buf := atomic.MakeBuffer(payload, len(payload))
	pub2, err := a.AddPublication("aeron:ipc", 4102)
	require.NoError(t, err)
	sub2, err := a.AddSubscription("aeron:ipc", 4102)
	require.NoError(t, err)
	got := 0
	require.Eventually(t, func() bool {
		pub2.Offer(buf, 0, int32(len(payload)), nil)
		sub2.Poll(func(*atomic.Buffer, int32, int32, *logbuffer.Header) { got++ }, 10)
		return got > 0
	}, 10*time.Second, 10*time.Millisecond)

	assert.False(t, a.IsClosed())
	assert.NoError(t, a.TerminalError())
	assert.NoError(t, pub.Close())
	assert.NoError(t, a.Close())
}

// A 1.53 archive pings each control session every second. The client must skip the pings without writing to
// stdout and keep serving requests.
func TestJava153ArchivePingIsSkippedQuietly(t *testing.T) {
	dir := countersE2EDir(t)
	opts := archive.DefaultOptions()
	opts.IdleStrategy = idlestrategy.Sleeping{SleepFor: time.Millisecond}
	opts.ClientName = "aeron-go-java153-ping"

	stdout := captureStdout(t, func() {
		a, err := archive.Connect(opts, aeron.NewContext().AeronDir(dir).MediaDriverTimeout(10*time.Second))
		require.NoError(t, err)
		defer func() { assert.NoError(t, a.Close()) }()
		time.Sleep(3500 * time.Millisecond)
		_, err = a.ListRecordings(0, 10)
		require.NoError(t, err)

		time.Sleep(3500 * time.Millisecond)
		stopped, err := a.TryStopRecording("aeron:udp?endpoint=localhost:1", 9999)
		require.NoError(t, err)
		assert.False(t, stopped)
		require.NoError(t, a.KeepAlive())
	})
	assert.Empty(t, stdout)
}

// Close must stop the conductor goroutine before it releases publications, subscriptions and counters, while that
// goroutine is still handling image events from a real driver. Fails under -race otherwise.
func TestJava153CloseWhileDriverEventsArrive(t *testing.T) {
	dir := countersE2EDir(t)
	churner := connectE2E(t, dir)
	defer func() { assert.NoError(t, churner.Close()) }()

	for round := 0; round < 20; round++ {
		channel := "aeron:ipc"
		stream := int32(4110 + round)
		a := connectE2E(t, dir)
		subs := make([]*aeron.Subscription, 0, 4)
		for i := 0; i < 4; i++ {
			sub, err := a.AddSubscription(channel, stream)
			require.NoError(t, err)
			subs = append(subs, sub)
		}
		_, err := a.AddCounterByLabel(1102, "java153-close")
		require.NoError(t, err)
		first, err := churner.AddExclusivePublication(channel, stream)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return subs[0].ImageCount() > 0 }, 10*time.Second, time.Millisecond)

		var wg sync.WaitGroup
		stop := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				pub, err := churner.AddExclusivePublication(channel, stream)
				if err != nil {
					continue
				}
				_ = pub.Close()
			}
		}()
		time.Sleep(20 * time.Millisecond)
		require.NoError(t, a.Close(), "round %d", round)
		close(stop)
		wg.Wait()
		assert.True(t, a.IsClosed())
		for _, sub := range subs {
			assert.True(t, sub.IsClosed())
		}
		assert.NoError(t, first.Close())
	}
}
