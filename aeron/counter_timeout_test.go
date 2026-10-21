package aeron

import (
	"errors"
	stdatomic "sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/stretchr/testify/require"
)

// Java ClientConductor.addCounter/addStaticCounter call awaitResponse, so an unanswered command throws
// DriverTimeoutException after driverTimeoutMs while the client stays open.
func TestBlockingAddCounterTimesOutWithDriverTimeoutError(t *testing.T) {
	key := atomic.MakeBuffer(make([]byte, 8))
	label := atomic.MakeBuffer([]byte(CounterLabel))
	adds := map[string]func(a *Aeron) (*Counter, error){
		"AddCounter": func(a *Aeron) (*Counter, error) {
			return a.AddCounter(CounterTypeId, key, 0, 8, label, 0, label.Capacity())
		},
		"AddCounterByLabel": func(a *Aeron) (*Counter, error) { return a.AddCounterByLabel(CounterTypeId, CounterLabel) },
		"AddStaticCounter": func(a *Aeron) (*Counter, error) {
			return a.AddStaticCounter(CounterTypeId, key, 0, 8, label, 0, label.Capacity(), staticCounterRegId)
		},
		"AddStaticCounterByLabel": func(a *Aeron) (*Counter, error) {
			return a.AddStaticCounterByLabel(CounterTypeId, CounterLabel, staticCounterRegId)
		},
	}
	for name, add := range adds {
		add := add
		t.Run(name, func(t *testing.T) {
			testBlockingAddCounterUnanswered(t, add)
		})
		t.Run(name+"/KeepaliveLostWhileWaiting", func(t *testing.T) {
			testBlockingAddCounterKeepaliveLost(t, add)
		})
	}
}

// The driver stops heart-beating while the add is waiting and the conductor duty cycle keeps running: Java's
// awaitResponse services the conductor itself and throws the keepalive DriverTimeoutException to the caller.
func testBlockingAddCounterKeepaliveLost(t *testing.T, add func(a *Aeron) (*Counter, error)) {
	a := &Aeron{context: NewContext().IdleStrategy(idlestrategy.NewSleeping())}
	tc := newTestConductorOn(t, &a.conductor)
	tc.driverTimeoutNs = int64(time.Second)
	var stale stdatomic.Bool
	tc.proxy.keepaliveMs = func() int64 {
		if stale.Load() {
			return time.Now().UnixMilli() - 10_000
		}
		return time.Now().UnixMilli()
	}
	stop := make(chan struct{})
	conductorDone := make(chan struct{})
	go func() {
		defer close(conductorDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, terminated := tc.doWork(); terminated {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	defer func() { close(stop); <-conductorDone }()
	done := make(chan error, 1)
	go func() {
		c, err := add(a)
		if c != nil {
			err = errors.New("unexpected counter")
		}
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	stale.Store(true)
	select {
	case err := <-done:
		var dte *DriverTimeoutError
		require.Truef(t, errors.As(err, &dte), "want DriverTimeoutError, got %T %v", err, err)
	case <-time.After(5 * time.Second):
		t.Fatal("blocking add counter did not time out")
	}
	require.True(t, tc.IsClosed())
}

func testBlockingAddCounterUnanswered(t *testing.T, add func(a *Aeron) (*Counter, error)) {
	{
		a := &Aeron{context: NewContext().IdleStrategy(idlestrategy.NewSleeping())}
		tc := newTestConductorOn(t, &a.conductor)
		tc.driverTimeoutNs = int64(200 * time.Millisecond)
		done := make(chan error, 1)
		go func() {
			c, err := add(a)
			if c != nil {
				err = errors.New("unexpected counter")
			}
			done <- err
		}()
		select {
		case err := <-done:
			var dte *DriverTimeoutError
			require.Truef(t, errors.As(err, &dte), "want DriverTimeoutError, got %T %v", err, err)
		case <-time.After(5 * time.Second):
			t.Fatal("blocking add counter did not time out")
		}
		require.False(t, tc.IsClosed())
		tc.adminLock.Lock()
		require.Empty(t, tc.resources, "timed out counter must not stay pending")
		tc.adminLock.Unlock()
	}
}
