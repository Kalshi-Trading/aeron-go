package agent

import (
	"errors"
	"sync"
	syncatomic "sync/atomic"
	"time"

	"github.com/lirm/aeron-go/aeron/idlestrategy"
)

// RetryCloseTimeout is how long Close waits for the agent goroutine before reporting a failure.
const RetryCloseTimeout = 5 * time.Second

// ErrCloseTimeout is reported via CloseWithTimeout when the agent does not stop in time.
var ErrCloseTimeout = errors.New("timeout waiting for agent to close")

// Runner runs an Agent on its own goroutine with an idle strategy (Agrona AgentRunner).
type Runner struct {
	agent        Agent
	idleStrategy idlestrategy.Idler
	errorHandler ErrorHandler
	errorCounter ErrorCounter

	isRunning syncatomic.Bool
	isClosed  syncatomic.Bool
	started   syncatomic.Bool
	closeOnce sync.Once
	done      chan struct{}
}

// NewRunner creates a Runner; errorHandler and errorCounter may be nil.
func NewRunner(idleStrategy idlestrategy.Idler, errorHandler ErrorHandler, errorCounter ErrorCounter, agent Agent) *Runner {
	r := &Runner{agent: agent, idleStrategy: idleStrategy, errorHandler: errorHandler, errorCounter: errorCounter,
		done: make(chan struct{})}
	r.isRunning.Store(true)
	return r
}

// StartOnGoroutine starts r.Run on a new goroutine.
func StartOnGoroutine(r *Runner) {
	go r.Run()
}

func (r *Runner) Agent() Agent   { return r.agent }
func (r *Runner) IsClosed() bool { return r.isClosed.Load() }

// Run executes the duty cycle until Close or ErrAgentTermination. It may be called only once.
func (r *Runner) Run() {
	if !r.started.CompareAndSwap(false, true) {
		return
	}
	defer close(r.done)
	if !r.isRunning.Load() {
		return
	}
	if err := r.agent.OnStart(); err != nil {
		r.isRunning.Store(false)
		r.handleError(err)
		r.agent.OnClose()
		return
	}
	if reset, ok := r.idleStrategy.(interface{ Reset() }); ok {
		reset.Reset()
	}
	for r.isRunning.Load() {
		n, err := r.agent.DoWork()
		if err != nil {
			if isTermination(err) {
				r.isRunning.Store(false)
			}
			r.handleError(err)
		} else {
			r.idleStrategy.Idle(n)
		}
	}
	r.agent.OnClose()
}

// Close stops the agent and waits up to RetryCloseTimeout for it to finish.
func (r *Runner) Close() error { return r.CloseWithTimeout(RetryCloseTimeout) }

// CloseWithTimeout stops the agent and waits up to timeout for OnClose to complete. If Run was never
// called, OnClose is invoked directly.
func (r *Runner) CloseWithTimeout(timeout time.Duration) error {
	var err error
	r.closeOnce.Do(func() {
		r.isRunning.Store(false)
		if r.started.CompareAndSwap(false, true) {
			close(r.done)
			r.agent.OnClose()
		} else {
			select {
			case <-r.done:
			case <-time.After(timeout):
				err = ErrCloseTimeout
			}
		}
		r.isClosed.Store(true)
	})
	return err
}

func (r *Runner) handleError(err error) {
	if r.errorCounter != nil && r.isRunning.Load() {
		r.errorCounter.Increment()
	}
	if r.errorHandler != nil {
		r.errorHandler(err)
	}
}
