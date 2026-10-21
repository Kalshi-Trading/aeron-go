// Package agent provides Agrona-style Agent, AgentRunner, AgentInvoker and CompositeAgent so that work
// can be run on a dedicated goroutine or invoked by the caller's own duty cycle (invoker mode).
package agent

import (
	"errors"
	"fmt"
	"strings"
)

// Agent is a unit of work run by a Runner or Invoker (Agrona Agent).
type Agent interface {
	// OnStart is called once on the running goroutine before the first DoWork.
	OnStart() error
	// DoWork performs one duty cycle and returns the amount of work done (0 means idle). Returning an
	// error wrapping ErrAgentTermination stops the agent.
	DoWork() (int, error)
	// OnClose is called once when the agent is closed.
	OnClose()
	// RoleName identifies the agent, e.g. for goroutine labels and logs.
	RoleName() string
}

// ErrAgentTermination is returned from DoWork (or OnStart) to request the agent stop
// (Agrona AgentTerminationException).
var ErrAgentTermination = errors.New("agent termination")

// ErrorHandler receives errors returned by an agent.
type ErrorHandler func(err error)

// ErrorCounter is incremented on each error, e.g. *counters.AtomicCounter.
type ErrorCounter interface {
	Increment() int64
}

func isTermination(err error) bool { return errors.Is(err, ErrAgentTermination) }

// Composite runs several agents in turn as one (Agrona CompositeAgent).
type Composite struct {
	agents   []Agent
	roleName string
	index    int
}

// NewComposite combines agents; at least one is required.
func NewComposite(agents ...Agent) (*Composite, error) {
	if len(agents) == 0 {
		return nil, errors.New("requires at least one sub-agent")
	}
	names := make([]string, len(agents))
	for i, a := range agents {
		if a == nil {
			return nil, fmt.Errorf("agent at index %d is nil", i)
		}
		names[i] = a.RoleName()
	}
	return &Composite{agents: append([]Agent(nil), agents...), roleName: "[" + strings.Join(names, ",") + "]"}, nil
}

func (c *Composite) RoleName() string { return c.roleName }

// OnStart starts every agent, returning all errors joined.
func (c *Composite) OnStart() error {
	var errs []error
	for _, a := range c.agents {
		if err := a.OnStart(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// DoWork runs each agent once. Like Agrona, if an agent fails, the remaining agents run on the next call.
func (c *Composite) DoWork() (int, error) {
	work := 0
	for c.index < len(c.agents) {
		a := c.agents[c.index]
		c.index++
		n, err := a.DoWork()
		work += n
		if err != nil {
			return work, err
		}
	}
	c.index = 0
	return work, nil
}

// OnClose closes every agent.
func (c *Composite) OnClose() {
	for _, a := range c.agents {
		a.OnClose()
	}
}
