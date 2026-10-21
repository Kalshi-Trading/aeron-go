package idlestrategy

import (
	"fmt"
	"runtime"
	"time"
)

// IdleStrategy is the full equivalent of Agrona's IdleStrategy. Idler (Idle(workCount)) remains the
// minimal interface accepted throughout aeron-go; every strategy in this package implements both.
type IdleStrategy interface {
	Idler
	// IdleNoWork idles unconditionally (Java idle()).
	IdleNoWork()
	// Reset the internal state, e.g. after work has been done when using IdleNoWork.
	Reset()
	// Alias is the short name of the strategy, as used in Aeron configuration.
	Alias() string
}

// Aliases matching Agrona's strategy ALIAS constants.
const (
	AliasNoOp          = "noop"
	AliasBusySpin      = "spin"
	AliasYield         = "yield"
	AliasSleep         = "sleep-ns"
	AliasSleepMillis   = "sleep-ms"
	AliasBackoff       = "backoff"
	AliasControllable  = "controllable"
	DefaultSleepPeriod = time.Microsecond
)

var (
	_ IdleStrategy = Busy{}
	_ IdleStrategy = NoOp{}
	_ IdleStrategy = Yielding{}
	_ IdleStrategy = Sleeping{}
	_ IdleStrategy = SleepingMillis{}
	_ IdleStrategy = (*BackoffIdleStrategy)(nil)
	_ IdleStrategy = (*Controllable)(nil)
)

// BusySpin spins without yielding (Agrona BusySpinIdleStrategy). Busy is the same type.
type BusySpin = Busy

func (s Busy) IdleNoWork()   {}
func (s Busy) Reset()        {}
func (s Busy) Alias() string { return AliasBusySpin }

// NoOp does nothing when idle (Agrona NoOpIdleStrategy).
type NoOp struct{}

func (NoOp) Idle(int)         {}
func (NoOp) IdleNoWork()      {}
func (NoOp) Reset()           {}
func (NoOp) Alias() string    { return AliasNoOp }
func (NoOp) String() string   { return "NoOpIdleStrategy{alias=noop}" }
func (s Busy) String() string { return "BusySpinIdleStrategy{alias=spin}" }

func (s Yielding) IdleNoWork()    { runtime.Gosched() }
func (s Yielding) Reset()         {}
func (s Yielding) Alias() string  { return AliasYield }
func (s Yielding) String() string { return "YieldingIdleStrategy{alias=yield}" }

// NewSleeping returns a Sleeping strategy with Agrona's default period of 1µs.
func NewSleeping() Sleeping { return Sleeping{SleepFor: DefaultSleepPeriod} }

func (s Sleeping) IdleNoWork()   { time.Sleep(s.SleepFor) }
func (s Sleeping) Reset()        {}
func (s Sleeping) Alias() string { return AliasSleep }
func (s Sleeping) String() string {
	return fmt.Sprintf("SleepingIdleStrategy{alias=sleep-ns, sleepPeriodNs=%d}", s.SleepFor.Nanoseconds())
}

// SleepingMillis sleeps for a period of milliseconds when idle (Agrona SleepingMillisIdleStrategy).
type SleepingMillis struct {
	SleepPeriodMs int64
}

// NewSleepingMillis returns a SleepingMillis strategy with Agrona's default period of 1ms.
func NewSleepingMillis() SleepingMillis { return SleepingMillis{SleepPeriodMs: 1} }

func (s SleepingMillis) Idle(workCount int) {
	if workCount <= 0 {
		s.IdleNoWork()
	}
}
func (s SleepingMillis) IdleNoWork()   { time.Sleep(time.Duration(s.SleepPeriodMs) * time.Millisecond) }
func (s SleepingMillis) Reset()        {}
func (s SleepingMillis) Alias() string { return AliasSleepMillis }
func (s SleepingMillis) String() string {
	return fmt.Sprintf("SleepingMillisIdleStrategy{alias=sleep-ms, sleepPeriodMs=%d}", s.SleepPeriodMs)
}

// StatusIndicatorReader is a source of a status value, e.g. *counters.AtomicCounter or a
// *atomic.Long wrapper (Agrona StatusIndicatorReader).
type StatusIndicatorReader interface {
	Get() int64
}

// Status values understood by Controllable.
const (
	NotControlled int64 = 0
	ControlNoOp   int64 = 1
	ControlSpin   int64 = 2
	ControlYield  int64 = 3
	ControlPark   int64 = 4
)

const controllableParkPeriod = time.Microsecond

// Controllable picks its behaviour on each idle from a status indicator (Agrona ControllableIdleStrategy).
type Controllable struct {
	statusIndicator StatusIndicatorReader
}

// NewControllable returns a Controllable strategy reading statusIndicator.
func NewControllable(statusIndicator StatusIndicatorReader) *Controllable {
	return &Controllable{statusIndicator: statusIndicator}
}

func (s *Controllable) Idle(workCount int) {
	if workCount <= 0 {
		s.IdleNoWork()
	}
}

func (s *Controllable) IdleNoWork() {
	switch s.statusIndicator.Get() {
	case ControlNoOp, ControlSpin:
	case ControlYield:
		runtime.Gosched()
	default:
		time.Sleep(controllableParkPeriod)
	}
}

func (s *Controllable) Reset()        {}
func (s *Controllable) Alias() string { return AliasControllable }
func (s *Controllable) String() string {
	return fmt.Sprintf("ControllableIdleStrategy{alias=controllable, statusIndicator=%v}", s.statusIndicator)
}

// FromAlias returns a strategy with default parameters for an Agrona alias
// (noop, spin, yield, sleep-ns, sleep-ms, backoff).
func FromAlias(alias string) (IdleStrategy, error) {
	switch alias {
	case AliasNoOp:
		return NoOp{}, nil
	case AliasBusySpin:
		return Busy{}, nil
	case AliasYield:
		return Yielding{}, nil
	case AliasSleep:
		return NewSleeping(), nil
	case AliasSleepMillis:
		return NewSleepingMillis(), nil
	case AliasBackoff:
		return NewDefaultBackoffIdleStrategy(), nil
	}
	return nil, fmt.Errorf("unknown idle strategy alias %q", alias)
}
