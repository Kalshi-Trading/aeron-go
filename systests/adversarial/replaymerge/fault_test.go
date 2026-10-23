package replaymerge

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lirm/aeron-go/archive/replaymerge"
	"github.com/lirm/aeron-go/systests/adversarial/replaymerge/internal/rmrun"
)

// faultCase injects fault when a ReplayMerge (Go, then separately the Java oracle, each on a fresh stack) first
// enters state, then compares the outcomes.
type faultCase struct {
	name    string
	state   replaymerge.State
	base    int
	stream  int32
	pub     rmrun.PubConfig
	backlog int64 // messages published before the merge starts
	total   int64
	retries int
	// fault is injected at state; it runs on the consumer's goroutine for Go, so it must not block for long.
	fault func(t *testing.T, e *env)
	// pollDelay slows the consumer so that states last long enough.
	pollDelay time.Duration
	// racy marks states that last only a few duty cycles: the Java fault is injected after the oracle reports the
	// state, by which time it may have moved on, so only data integrity (not the outcome class) is compared.
	racy bool
}

type faultOutcome struct {
	merged   bool
	failures int
	next     int64
	messages string
}

func (fc faultCase) env(t *testing.T) *env {
	pc := fc.pub
	if pc.Rate == 0 {
		pc.Rate = 4000
	}
	if len(pc.Sizes) == 0 {
		pc.Sizes = []int{64, 1500, 64, 300}
	}
	pc.Count = fc.total
	e := newEnv(t, fc.base, fc.stream, pc)
	e.publish()
	e.awaitSent(fc.backlog, 30*time.Second)
	return e
}

func (fc faultCase) runGo(t *testing.T) (faultOutcome, rmrun.MergeResult) {
	e := fc.env(t)
	cfg := e.mergeConfig(fc.total)
	cfg.Retries = fc.retries
	cfg.PollDelay = fc.pollDelay
	cfg.OnState = atState(fc.state, func() { fc.fault(t, e) })
	r := rmrun.RunReplayMerge(e.c.Aeron, cfg, func(ev string, kv ...any) { t.Logf("go %s %v", ev, kv) })
	require.False(t, r.TimedOut, "go merge timed out: %s", failMessages(r))
	require.Zero(t, r.Gaps, "go gaps: %s", r.Checker)
	require.Zero(t, r.Duplicates, "go duplicates: %s", r.Checker)
	require.Zero(t, r.Corrupt, "go corrupt: %s", r.Checker)
	require.Less(t, r.MaxCloseDur, 5*time.Second, "ReplayMerge.Close blocked")
	for _, s := range r.FailStates {
		require.Equal(t, replaymerge.StateFailed, s, "failed merge must report FAILED")
	}
	return faultOutcome{r.Merged, len(r.Failures), r.Next, failMessages(r)}, r
}

func (fc faultCase) runJava(t *testing.T) faultOutcome {
	e := fc.env(t)
	extra := []string{fmt.Sprintf("retries=%d", fc.retries)}
	o := e.oracleMerge(fc.total, extra...)
	o.await("state "+fc.state.String(), 60*time.Second, func(ev rmrun.Event) bool {
		return ev["event"] == "state" && ev["state"] == fc.state.String()
	})
	fc.fault(t, e)
	res := o.result(120 * time.Second)
	require.Equal(t, false, res["timedOut"], "java timed out %v", res)
	require.Zero(t, num(res, "gaps"), "java %v", res)
	require.Zero(t, num(res, "duplicates"), "java %v", res)
	require.Zero(t, num(res, "corrupt"), "java %v", res)
	var msgs []string
	for _, ev := range o.all() {
		if ev["event"] == "failed" {
			msgs = append(msgs, fmt.Sprintf("%v: %v", ev["class"], ev["message"]))
		}
	}
	return faultOutcome{res["merged"] == true, int(num(res, "failures")), num(res, "next"), strings.Join(msgs, "; ")}
}

// compare runs the case for Go and Java and requires the same outcome class.
func (fc faultCase) compare(t *testing.T) {
	var goOut, javaOut faultOutcome
	t.Run("java", func(t *testing.T) { javaOut = fc.runJava(t) })
	t.Run("go", func(t *testing.T) { goOut, _ = fc.runGo(t) })
	t.Logf("java: %+v", javaOut)
	t.Logf("go:   %+v", goOut)
	if fc.racy {
		return
	}
	require.Equal(t, javaOut.merged, goOut.merged, "merged differs: java %+v go %+v", javaOut, goOut)
	require.Equal(t, javaOut.failures > 0, goOut.failures > 0, "failure differs: java %+v go %+v", javaOut, goOut)
	if javaOut.merged {
		require.Equal(t, fc.total, goOut.next)
	}
}

func killContainer(t *testing.T, name string) {
	out, err := exec.Command("docker", "kill", "-s", "KILL", name).CombinedOutput()
	require.NoError(t, err, "docker kill %s: %s", name, out)
}

// TestArchiveKillAtEachState SIGKILLs the archive when the merge reaches each state and restarts it, with one retry
// from the last delivered position. Before the merge the replay is lost: the merge must fail and the retry delivers
// the rest of the (now stopped) recording without gaps or duplicates and then fails as there is no live overlap.
// After the merge the archive is no longer needed and the merge must keep delivering.
func TestArchiveKillAtEachState(t *testing.T) {
	requireAdversarial(t)
	for i, state := range []replaymerge.State{replaymerge.StateReplay, replaymerge.StateCatchup,
		replaymerge.StateAttemptLiveJoin, replaymerge.StateMerged} {
		fc := faultCase{name: state.String(), state: state, base: 27420 + i*10, stream: int32(9410 + i),
			backlog: 8000, total: 40000, retries: 1, racy: state == replaymerge.StateAttemptLiveJoin,
			fault: func(t *testing.T, e *env) {
				killContainer(t, archiveName)
				go func() { _ = restartArchive() }()
			}}
		t.Run(fc.name, fc.compare)
	}
}

// TestDriverPauseAtEachState SIGSTOPs the media driver for 1.5s (below the client and image liveness timeouts) at
// each state: the merge must ride it out and merge with exact ordering, as Java does.
func TestDriverPauseAtEachState(t *testing.T) {
	requireAdversarial(t)
	for i, state := range []replaymerge.State{replaymerge.StateReplay, replaymerge.StateCatchup,
		replaymerge.StateAttemptLiveJoin, replaymerge.StateMerged} {
		fc := faultCase{name: state.String(), state: state, base: 27420 + i*10, stream: int32(9420 + i),
			backlog: 8000, total: 30000,
			fault: func(t *testing.T, e *env) {
				docker(t, "pause", driverName)
				time.Sleep(1500 * time.Millisecond)
				docker(t, "unpause", driverName)
			}}
		t.Run(fc.name, func(t *testing.T) {
			fc.compare(t)
		})
	}
}
