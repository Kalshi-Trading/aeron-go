package drivercrash

import (
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/stretchr/testify/require"
)

// TestDriverCrashRestartLoop repeats connect -> traffic on several publisher/subscriber kinds -> driver fault ->
// detection -> Close -> driver restart (new CnC) for AERON_ADV_CYCLES cycles (default 50). Faults alternate between
// SIGKILL and SIGSTOP (then SIGKILL while frozen), and every fifth cycle uses the archiving driver. Every cycle must
// detect the fault within the driver timeout with a single DriverTimeoutError, deliver intact data and unmap
// everything on Close; after the loop no goroutine, fd or mapping may have leaked.
func TestDriverCrashRestartLoop(t *testing.T) {
	requireAdv(t)
	cycles := 50
	if v, err := strconv.Atoi(os.Getenv("AERON_ADV_CYCLES")); err == nil && v > 0 {
		cycles = v
	}
	const loopDriverTimeout = time.Second
	kind := plain
	startDriver(t, kind)
	base := snapshot(t)
	var worst time.Duration
	for c := 0; c < cycles; c++ {
		want := plain
		if c%5 == 4 {
			want = archiving
		}
		if want != kind {
			kind = want
			startDriver(t, kind)
		} else if c > 0 {
			restartDriver(t, kind)
		}

		o := &observer{}
		a, err := aeron.Connect(o.context(loopDriverTimeout))
		require.NoError(t, err, "cycle %d connect", c)
		all := specs(c % 6)
		var flows []*flow
		for _, s := range []flowSpec{all[0], all[1], all[3], all[6]} {
			flows = append(flows, startFlow(t, a, s))
		}
		for _, f := range flows {
			f.awaitProgress(t, 100, 10*time.Second)
		}

		if c%2 == 1 {
			signalDriver(t, kind, "STOP")
		} else {
			signalDriver(t, kind, "KILL")
		}
		faulted := time.Now()
		awaitTrue(t, "fault detected", loopDriverTimeout+slack, a.IsClosed)
		if d := time.Since(faulted); d > worst {
			worst = d
		}
		var dt *aeron.DriverTimeoutError
		require.True(t, errors.As(a.TerminalError(), &dt), "cycle %d: %v", c, a.TerminalError())
		require.Len(t, o.errors(), 1, "cycle %d: %v", c, o.errors())
		require.EqualValues(t, 1, o.closes.Load(), "cycle %d", c)
		for _, f := range flows {
			f.awaitExit(t, 2*time.Second)
			f.requireIntact(t)
		}
		within(t, "Close", 5*time.Second, func() { require.NoError(t, a.Close()) })
		require.Zero(t, mappedFiles(t), "cycle %d", c)
	}
	t.Logf("%d cycles, slowest detection %v", cycles, worst)
	requireNoLeaks(t, base)
}
