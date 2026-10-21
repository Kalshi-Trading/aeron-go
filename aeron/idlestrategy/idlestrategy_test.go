package idlestrategy

import (
	"testing"
	"time"
)

type status struct{ v int64 }

func (s *status) Get() int64 { return s.v }

func TestAliases(t *testing.T) {
	for _, a := range []string{AliasNoOp, AliasBusySpin, AliasYield, AliasSleep, AliasSleepMillis, AliasBackoff} {
		s, err := FromAlias(a)
		if err != nil || s.Alias() != a {
			t.Fatalf("%s: %v %v", a, s, err)
		}
		s.Idle(1)
		s.Idle(0)
		s.IdleNoWork()
		s.Reset()
	}
	if _, err := FromAlias("x"); err == nil {
		t.Fatal("expected error")
	}
}

func TestBackoffDefaults(t *testing.T) {
	s := NewDefaultBackoffIdleStrategy()
	if s.maxSpins != 10 || s.maxYields != 20 || s.minParkPeriodNs != 1000 || s.maxParkPeriodNs != int64(time.Millisecond) {
		t.Fatal(s)
	}
}

func TestBackoffStates(t *testing.T) {
	s := NewBackoffIdleStrategy(2, 2, 1000, 4000)
	want := []backoffState{backoffSpinning, backoffSpinning, backoffYielding, backoffYielding, backoffYielding, backoffParking}
	for i, w := range want {
		s.Idle(0)
		if s.state != w {
			t.Fatalf("step %d: state=%d want %d", i, s.state, w)
		}
	}
	for i := 0; i < 5; i++ {
		s.Idle(0)
	}
	if s.parkPeriodNs != 4000 {
		t.Fatalf("park=%d", s.parkPeriodNs)
	}
	s.Idle(1)
	if s.state != backoffNotIdle {
		t.Fatal("work should reset")
	}
}

func TestControllable(t *testing.T) {
	st := &status{v: ControlSpin}
	c := NewControllable(st)
	start := time.Now()
	for i := 0; i < 1000; i++ {
		c.Idle(0)
	}
	st.v = ControlYield
	c.Idle(0)
	st.v = ControlPark
	c.Idle(0)
	if time.Since(start) > time.Second {
		t.Fatal("too slow")
	}
}
