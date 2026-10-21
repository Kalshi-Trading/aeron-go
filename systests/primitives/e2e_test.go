// Package primitives contains end-to-end tests of the Agrona-equivalent primitives (ring buffer,
// broadcast receiver, idle strategies, agent invoker) against a real Java media driver.
//
// Run with: AERON_E2E=1 AERON_DIR=/dev/shm/aeron-compose go test -race ./systests/primitives
package primitives

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/agent"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
)

func connect(t *testing.T) *aeron.Aeron {
	t.Helper()
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against a media driver")
	}
	dir := os.Getenv("AERON_DIR")
	if dir == "" {
		dir = "/dev/shm/aeron-compose"
	}
	ctx := aeron.NewContext().AeronDir(dir).
		MediaDriverTimeout(10 * time.Second).
		IdleStrategy(idlestrategy.NewDefaultBackoffIdleStrategy())
	a, err := aeron.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// subscriberAgent polls a subscription when invoked by an agent.Invoker.
type subscriberAgent struct {
	sub     *aeron.Subscription
	handler func(*atomic.Buffer, int32, int32, *logbuffer.Header)
}

func (s *subscriberAgent) OnStart() error   { return nil }
func (s *subscriberAgent) OnClose()         {}
func (s *subscriberAgent) RoleName() string { return "e2e-subscriber" }
func (s *subscriberAgent) DoWork() (int, error) {
	return s.sub.Poll(s.handler, 10), nil
}

func runPubSub(t *testing.T, a *aeron.Aeron, channel string, streamID int32, count int) {
	sub, err := a.AddSubscription(channel, streamID)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	pub, err := a.AddPublication(channel, streamID)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	deadline := time.Now().Add(10 * time.Second)
	for !pub.IsConnected() || !sub.IsConnected() {
		if time.Now().After(deadline) {
			t.Fatal("publication not connected")
		}
		time.Sleep(time.Millisecond)
	}

	received := 0
	sa := &subscriberAgent{sub: sub, handler: func(buf *atomic.Buffer, offset, length int32, _ *logbuffer.Header) {
		want := fmt.Sprintf("msg-%d-%s", received, string(make([]byte, received%200)))
		if got := buf.GetStringWithoutLengthUtf8(offset, length); got != want {
			t.Errorf("message %d mismatch: len got=%d want=%d", received, len(got), len(want))
		}
		received++
	}}
	inv := agent.NewInvoker(func(err error) { t.Error(err) }, nil, sa)
	inv.Start()
	defer inv.Close()

	idle := idlestrategy.NewDefaultBackoffIdleStrategy()
	src := atomic.MakeBuffer(make([]byte, 512))
	for i := 0; i < count; i++ {
		length := src.PutStringWithoutLengthUtf8(0, fmt.Sprintf("msg-%d-%s", i, string(make([]byte, i%200))))
		deadline = time.Now().Add(10 * time.Second)
		for pub.Offer(src, 0, length, nil) < 0 {
			idle.Idle(inv.Invoke())
			if time.Now().After(deadline) {
				t.Fatalf("offer %d timed out", i)
			}
		}
		idle.Idle(inv.Invoke())
	}
	deadline = time.Now().Add(10 * time.Second)
	for received < count {
		idle.Idle(inv.Invoke())
		if time.Now().After(deadline) {
			t.Fatalf("received %d of %d", received, count)
		}
	}
}

func TestE2EPubSubUDP(t *testing.T) {
	runPubSub(t, connect(t), "aeron:udp?endpoint=localhost:24325", 4001, 20000)
}

func TestE2EPubSubIPC(t *testing.T) {
	runPubSub(t, connect(t), "aeron:ipc", 4002, 20000)
}

// TestE2EConcurrentCommands drives many concurrent writers into the to-driver ManyToOne ring buffer and
// checks the driver responds to each (responses arrive via the broadcast receiver).
func TestE2EConcurrentCommands(t *testing.T) {
	a := connect(t)
	const goroutines, perGoroutine = 8, 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	ids := map[int64]bool{}
	errs := make(chan error, goroutines*perGoroutine)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id := a.NextCorrelationID()
				mu.Lock()
				if ids[id] {
					errs <- fmt.Errorf("duplicate correlation id %d", id)
				}
				ids[id] = true
				mu.Unlock()
				pub, err := a.AddPublication("aeron:ipc", int32(5000+g*perGoroutine+i))
				if err != nil {
					errs <- err
					continue
				}
				if err := pub.Close(); err != nil {
					errs <- err
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
