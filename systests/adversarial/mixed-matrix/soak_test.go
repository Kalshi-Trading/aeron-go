package mixedmatrix

import (
	"errors"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/systests/adversarial/mixed-matrix/internal/mm"
	"github.com/stretchr/testify/require"
)

// driverCrash SIGKILLs the driver under a mixed set of live Go (in-process and child) and Java (child) clients and
// asserts that every one of them detects it like Java does (DriverTimeout, unavailable images, closed resources,
// child processes exit), then restarts the driver and returns the new counter baseline.
func driverCrash(t *testing.T, rnd *rand.Rand, port int, stream int32) map[int32]int {
	t.Helper()
	ch := "aeron:udp?endpoint=localhost:" + strconv.Itoa(port) + "|" + termParams
	subCh := "aeron:udp?endpoint=localhost:" + strconv.Itoa(port)
	streamArg := "stream=" + strconv.Itoa(int(stream))
	cl := connect(t)
	anchor, err := mm.AddSub(cl.Aeron, mm.SubControlled, subCh, stream, rand.New(rand.NewSource(rnd.Int63())), true)
	require.NoError(t, err)
	pl := startPoller(t, anchor)
	goKinds := []string{mm.PubConcurrent, mm.PubExclusive, mm.PubTryClaim, mm.PubOfferBlock}
	peers := []*peer{
		startPeer(t, "go", "pub", "channel="+ch, streamArg, "kind="+goKinds[rnd.Intn(4)], "writer=1",
			"sizes="+mm.FormatSizes(cellSizes), "rateUs=400"),
		startPeer(t, "java", "pub", "channel="+ch, streamArg, "exclusive="+strconv.FormatBool(rnd.Intn(2) == 0),
			"writer=2", "sizes="+mm.FormatSizes(cellSizes), "rateUs=400"),
		startPeer(t, "go", "sub", "channel="+subCh, streamArg, "kind=controlled"),
	}
	for _, p := range peers {
		p.await("ready", 30*time.Second)
	}
	awaitTrue(t, "traffic before the crash", 20*time.Second, func() bool { return anchor.Tracker.Messages() > 200 })
	driverSignal(t, "KILL")
	awaitTrue(t, "client to close after the driver crash", driverTimeout+5*time.Second, cl.IsClosed)
	var dte *aeron.DriverTimeoutError
	found := false
	for _, e := range cl.Errors() {
		found = found || errors.As(e, &dte)
	}
	require.True(t, found, "DriverTimeoutError via ErrorHandler, got %v", cl.Errors())
	for _, p := range peers {
		requireTerminatedWith(t, p, "drivertimeout", driverTimeout+10*time.Second)
	}
	pl.Stop()
	for _, img := range anchor.Tracker.Images() {
		require.True(t, img.Unavailable, "unavailable handler for image %d after the driver crash", img.CorrelationID)
	}
	requireCleanTracker(t, "anchor", anchor.Tracker)
	require.NoError(t, cl.Close())
	restartDriver(t)
	return driverCounters(t)
}

// TestDriverCrashMixedClients is one driverCrash round on its own.
func TestDriverCrashMixedClients(t *testing.T) {
	requireAdv(t)
	rnd := seed(t)
	base := snapshot()
	freshStack(t)
	counters := driverCrash(t, rnd, 27734, 9790)
	// fresh clients of every kind work on the restarted driver
	runCell(t, rnd, cell{pub: pubTypes[rnd.Intn(len(pubTypes))], consumer: consGoControlled, transport: transports[1],
		stream: 9791, port0: 27736, port1: 27737, duration: 4 * time.Second, mixed: true})
	requireCountersReturn(t, counters)
	requireNoLeaks(t, base)
}

// TestSoakChaosMonkey runs random matrix cells under churn for MM_SOAK (e.g. 20m), with a chaos monkey that
// between cells SIGKILLs and restarts the driver under live mixed clients, or freezes it briefly. At the end
// goroutines, fds, mmaps and driver counters must be back to their baselines.
func TestSoakChaosMonkey(t *testing.T) {
	requireAdv(t)
	d, err := time.ParseDuration(os.Getenv("MM_SOAK"))
	if err != nil {
		t.Skip("set MM_SOAK=20m to run the chaos monkey soak")
	}
	rnd := seed(t)
	base := snapshot()
	freshStack(t)
	counters := driverCounters(t)
	deadline := time.Now().Add(d)
	stats := map[string]int{}
	for i := 0; time.Now().Before(deadline); i++ {
		ok := t.Run(strconv.Itoa(i), func(t *testing.T) {
			switch r := rnd.Intn(10); {
			case r < 2:
				stats["driver-kill"]++
				counters = driverCrash(t, rnd, 27740, int32(9792))
			case r < 3:
				stats["driver-freeze"]++
				driverSignal(t, "STOP")
				time.Sleep(time.Duration(200+rnd.Intn(1000)) * time.Millisecond)
				driverSignal(t, "CONT")
			default:
				stats["cell"]++
				ti := rnd.Intn(len(transports))
				runCell(t, rnd, cell{pub: pubTypes[rnd.Intn(len(pubTypes))],
					consumer: consumerTypes[rnd.Intn(len(consumerTypes))], transport: transports[ti],
					stream: int32(9793 + i%6), port0: 27742 + 2*(i%4), port1: 27743 + 2*(i%4),
					duration: time.Duration(4+rnd.Intn(6)) * time.Second, mixed: true})
			}
		})
		if !ok {
			break
		}
	}
	t.Logf("soak rounds: %v", stats)
	requireCountersReturn(t, counters)
	requireNoLeaks(t, base)
}
