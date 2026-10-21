package agent

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron/idlestrategy"
)

type countingAgent struct {
	name                  string
	started, closed, work atomic.Int32
	stopAt                int32
	failEvery             int32
}

func (a *countingAgent) OnStart() error { a.started.Add(1); return nil }
func (a *countingAgent) OnClose()       { a.closed.Add(1) }
func (a *countingAgent) RoleName() string {
	return a.name
}
func (a *countingAgent) DoWork() (int, error) {
	n := a.work.Add(1)
	if a.stopAt > 0 && n >= a.stopAt {
		return 1, ErrAgentTermination
	}
	if a.failEvery > 0 && n%a.failEvery == 0 {
		return 0, errors.New("fail")
	}
	return 1, nil
}

type counter struct{ n atomic.Int64 }

func (c *counter) Increment() int64 { return c.n.Add(1) }

func TestInvoker(t *testing.T) {
	a := &countingAgent{name: "a", stopAt: 3}
	var errs []error
	inv := NewInvoker(func(err error) { errs = append(errs, err) }, nil, a)
	if inv.Invoke() != 0 {
		t.Fatal("not started")
	}
	inv.Start()
	inv.Start()
	for i := 0; i < 5; i++ {
		inv.Invoke()
	}
	if a.started.Load() != 1 || a.work.Load() != 3 || a.closed.Load() != 1 || !inv.IsClosed() || len(errs) != 1 {
		t.Fatalf("started=%d work=%d closed=%d errs=%v", a.started.Load(), a.work.Load(), a.closed.Load(), errs)
	}
}

func TestRunner(t *testing.T) {
	a := &countingAgent{name: "r", failEvery: 10}
	c := &counter{}
	r := NewRunner(idlestrategy.NewDefaultBackoffIdleStrategy(), func(error) {}, c, a)
	StartOnGoroutine(r)
	for a.work.Load() < 100 {
		time.Sleep(time.Millisecond)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if a.closed.Load() != 1 || !r.IsClosed() || c.n.Load() == 0 {
		t.Fatal("bad close")
	}
	_ = r.Close()
	if a.closed.Load() != 1 {
		t.Fatal("closed twice")
	}
}

func TestRunnerTermination(t *testing.T) {
	a := &countingAgent{name: "t", stopAt: 5}
	r := NewRunner(idlestrategy.Busy{}, nil, nil, a)
	done := make(chan struct{})
	go func() { r.Run(); close(done) }()
	<-done
	if a.work.Load() != 5 || a.closed.Load() != 1 {
		t.Fatal("termination")
	}
	_ = r.Close()
}

func TestRunnerCloseBeforeRun(t *testing.T) {
	a := &countingAgent{name: "c"}
	r := NewRunner(idlestrategy.Busy{}, nil, nil, a)
	_ = r.Close()
	r.Run()
	if a.started.Load() != 0 || a.closed.Load() != 1 {
		t.Fatal("close before run")
	}
}

func TestComposite(t *testing.T) {
	a, b := &countingAgent{name: "a"}, &countingAgent{name: "b", failEvery: 2}
	c, err := NewComposite(a, b)
	if err != nil || c.RoleName() != "[a,b]" {
		t.Fatal(err)
	}
	_ = c.OnStart()
	c.DoWork()
	if _, err := c.DoWork(); err == nil {
		t.Fatal("expected error")
	}
	c.DoWork()
	if a.work.Load() != 2 || b.work.Load() != 2 { // Agrona: 3rd call resumes after failed b, i.e. runs nothing
		t.Fatalf("a=%d b=%d", a.work.Load(), b.work.Load())
	}
	c.OnClose()
	if _, err := NewComposite(); err == nil {
		t.Fatal("expected error")
	}
}

type failStartAgent struct{ countingAgent }

func (a *failStartAgent) OnStart() error { return errors.New("start failed") }

// Agrona AgentInvoker.start passes an onStart failure straight to the error handler without
// incrementing the error counter, then closes the agent.
func TestInvokerStartFailureDoesNotCountError(t *testing.T) {
	a := &failStartAgent{countingAgent{name: "f"}}
	c := &counter{}
	var errs []error
	inv := NewInvoker(func(err error) { errs = append(errs, err) }, c, a)
	inv.Start()
	if c.n.Load() != 0 || len(errs) != 1 || !inv.IsClosed() || inv.IsRunning() || a.closed.Load() != 1 {
		t.Fatalf("counter=%d errs=%v closed=%v running=%v", c.n.Load(), errs, inv.IsClosed(), inv.IsRunning())
	}
}

type countingIdler struct{ calls []int }

func (c *countingIdler) Idle(n int) { c.calls = append(c.calls, n) }

// Agrona AgentRunner does not call the idle strategy when doWork throws.
func TestRunnerDoesNotIdleAfterError(t *testing.T) {
	a := &countingAgent{name: "e", failEvery: 2, stopAt: 4}
	idler := &countingIdler{}
	r := NewRunner(idler, func(error) {}, nil, a)
	r.Run()
	// work 1 ok, 2 fails, 3 ok, 4 terminates: idle only after 1 and 3.
	if len(idler.calls) != 2 {
		t.Fatalf("idle calls=%v", idler.calls)
	}
}
