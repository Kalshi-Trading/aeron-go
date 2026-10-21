package replaymerge

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	ps "github.com/lirm/aeron-go/archive/persistentsubscription"
	"github.com/lirm/aeron-go/systests/adversarial/replaymerge/internal/rmrun"
)

func (e *env) liveSubChannel(port int) string {
	return fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d", port, e.p.control)
}

func (e *env) psConfig(count int64) rmrun.PSConfig {
	return rmrun.PSConfig{AeronDir: aeronDir, RecordingID: e.recID, LiveChannel: e.liveSubChannel(e.p.live),
		StreamID: e.stream, ReplayChannel: "aeron:udp?endpoint=localhost:0", ReplayStream: e.stream + 50,
		Count: count, Timeout: 2 * time.Minute}
}

// oraclePS starts the Java PersistentSubscription oracle on live port base+4.
func (e *env) oraclePS(count int64, extra ...string) *child {
	args := []string{
		fmt.Sprintf("recordingId=%d", e.recID), fmt.Sprintf("stream=%d", e.stream), fmt.Sprintf("count=%d", count),
		"liveChannel=" + e.liveSubChannel(e.p.control+4), "replayChannel=aeron:udp?endpoint=localhost:0",
		fmt.Sprintf("replayStream=%d", e.stream+50), "timeoutMs=120000",
	}
	o := startOracle(e.t, "ps", append(args, extra...)...)
	o.awaitEvent("ready", 60*time.Second)
	return o
}

func requirePSExact(t *testing.T, r rmrun.PSResult, to int64) {
	t.Helper()
	require.False(t, r.TimedOut, "ps timed out in %v: %s errors %v", r.States, r.Checker, r.Errors)
	require.False(t, r.Failed, "ps failed: %v", r.FailureErr)
	require.Zero(t, r.Gaps, "gaps: %s", r.Checker)
	require.Zero(t, r.Duplicates, "duplicates: %s", r.Checker)
	require.Zero(t, r.Corrupt, "corrupt: %s", r.Checker)
	require.Equal(t, to, r.Next, "%s", r.Checker)
	require.Less(t, r.CloseDur, 5*time.Second, "PersistentSubscription.Close blocked")
}

// statesContain reports whether want occurs as a subsequence of states.
func statesContain(states []ps.State, want ...ps.State) bool {
	i := 0
	for _, s := range states {
		if i < len(want) && s == want[i] {
			i++
		}
	}
	return i == len(want)
}

// TestPSLivePartitionRecovers starts a PersistentSubscription (Go and Java side by side) from the start of the
// recording, lets it join live, then blackholes the live stream for longer than the image liveness timeout. It must
// leave live (OnLiveLeft), fall back to replay, rejoin live (OnLiveJoined) and deliver every message exactly once.
func TestPSLivePartitionRecovers(t *testing.T) {
	requireAdversarial(t)
	const total = 50000
	e := newEnv(t, 27420, 9450, rmrun.PubConfig{Count: total, Rate: 2500, Sizes: []int{64, 1500, 64, 5000}})
	e.publish()
	e.awaitSent(3000, 30*time.Second)
	o := e.oraclePS(total)
	base := snapshot(t)
	cfg := e.psConfig(total)
	partitioned := false
	cfg.OnState = func(s ps.State, c *rmrun.Checker) {
		if s == ps.StateLive && !partitioned {
			partitioned = true
			clear := netem(t, "loss 100%", e.p.live, e.p.control+4)
			go func() {
				time.Sleep(5 * time.Second)
				clear()
			}()
		}
	}
	r, err := rmrun.RunPersistentSubscription(e.c.Aeron, cfg, func(ev string, kv ...any) { t.Logf("go %s %v", ev, kv) })
	require.NoError(t, err)
	requirePSExact(t, r, total)
	require.True(t, statesContain(r.States, ps.StateLive, ps.StateReplay, ps.StateLive), "states %v", r.States)
	require.GreaterOrEqual(t, r.Joined, 2)
	require.GreaterOrEqual(t, r.Left, 1)
	res := o.result(2 * time.Minute)
	require.Equal(t, false, res["failed"], "java %v", res)
	requireOracleExact(t, res, total)
	require.GreaterOrEqual(t, num(res, "joined"), int64(2), "java %v", res)
	require.GreaterOrEqual(t, num(res, "left"), int64(1), "java %v", res)
	requireNoLeaks(t, base)
}

// psFaultCase injects fault when the Go PersistentSubscription (and, on a fresh stack, the Java one) first
// reaches state and compares the outcomes.
type psFaultCase struct {
	state   ps.State
	base    int
	stream  int32
	total   int64
	backlog int64
	fault   func(t *testing.T, e *env)
}

type psOutcome struct {
	failed       bool
	next         int64
	joined, left int64
}

func (fc psFaultCase) env(t *testing.T) *env {
	e := newEnv(t, fc.base, fc.stream, rmrun.PubConfig{Count: fc.total, Rate: 3000, Sizes: []int{64, 1500, 64, 300}})
	e.publish()
	e.awaitSent(fc.backlog, 30*time.Second)
	return e
}

func (fc psFaultCase) compare(t *testing.T) (javaOut, goOut psOutcome) {
	t.Run("java", func(t *testing.T) {
		e := fc.env(t)
		o := e.oraclePS(fc.total)
		switch fc.state {
		case ps.StateLive:
			o.awaitEvent("liveJoined", 60*time.Second)
		default:
			// Java exposes no state getter: inject while replaying (before the first live join).
			time.Sleep(300 * time.Millisecond)
		}
		fc.fault(t, e)
		res := o.result(3 * time.Minute)
		require.Equal(t, false, res["timedOut"], "java %v", res)
		require.Zero(t, num(res, "gaps"), "java %v", res)
		require.Zero(t, num(res, "duplicates"), "java %v", res)
		javaOut = psOutcome{res["failed"] == true, num(res, "next"), num(res, "joined"), num(res, "left")}
	})
	t.Run("go", func(t *testing.T) {
		e := fc.env(t)
		cfg := e.psConfig(fc.total)
		cfg.Timeout = 3 * time.Minute
		done := false
		cfg.OnState = func(s ps.State, c *rmrun.Checker) {
			if s == fc.state && !done {
				done = true
				fc.fault(t, e)
			}
		}
		r, err := rmrun.RunPersistentSubscription(e.c.Aeron, cfg, func(ev string, kv ...any) {
			if ev != "progress" {
				t.Logf("go %s %v", ev, kv)
			}
		})
		require.NoError(t, err)
		require.False(t, r.TimedOut, "go timed out: %v %s", r.States, r.Checker)
		require.Zero(t, r.Gaps, "%s", r.Checker)
		require.Zero(t, r.Duplicates, "%s", r.Checker)
		require.Less(t, r.CloseDur, 5*time.Second)
		goOut = psOutcome{r.Failed, r.Next, int64(r.Joined), int64(r.Left)}
	})
	t.Logf("java %+v go %+v", javaOut, goOut)
	require.Equal(t, javaOut.failed, goOut.failed, "failed: java %+v go %+v", javaOut, goOut)
	require.Equal(t, javaOut.next == fc.total, goOut.next == fc.total, "completion: java %+v go %+v", javaOut, goOut)
	return javaOut, goOut
}

// TestPSArchiveKill SIGKILLs and restarts the archive while the PersistentSubscription is replaying and while it is
// live. Live: the archive is not needed and delivery continues. Replaying: the replay is lost and the recording
// stops; Java reconnects and keeps retrying, Go must do the same and never deliver a gap or duplicate.
func TestPSArchiveKill(t *testing.T) {
	requireAdversarial(t)
	for i, state := range []ps.State{ps.StateReplay, ps.StateLive} {
		// While replaying, the backlog is large enough that the replay is still far behind live when the kill lands:
		// once a PersistentSubscription has observed a live position beyond what the dying archive recorded, Java
		// and Go both fail terminally with INVALID_START_POSITION, which is not the scenario under test here.
		total, backlog := int64(40000), int64(15000)
		if state == ps.StateReplay {
			total, backlog = 70000, 45000
		}
		fc := psFaultCase{state: state, base: 27430 + i*10, stream: int32(9451 + i), total: total, backlog: backlog,
			fault: func(t *testing.T, e *env) {
				killContainer(t, archiveName)
				go func() { _ = restartArchive() }()
			}}
		t.Run(state.String(), func(t *testing.T) { fc.compare(t) })
	}
}

// TestPSConsumerKill SIGKILLs a PersistentSubscription consumer process (rmhelper) mid replay and mid live, then
// starts a new PersistentSubscription from the last position the dead process reported: the stream must continue
// exactly, and the driver must clean up the dead client.
func TestPSConsumerKill(t *testing.T) {
	requireAdversarial(t)
	for i, phase := range []string{"replay", "live"} {
		t.Run(phase, func(t *testing.T) {
			const total = 40000
			e := newEnv(t, 27450+i*10, int32(9455+i), rmrun.PubConfig{Count: total, Rate: 3000,
				Sizes: []int{64, 1500, 64, 300}})
			e.publish()
			e.awaitSent(15000, 30*time.Second)
			cfg := e.psConfig(total)
			c := startHelper(t, "ps", "-recording", fmt.Sprint(e.recID), "-channel", cfg.LiveChannel,
				"-stream", fmt.Sprint(e.stream), "-replayChannel", cfg.ReplayChannel,
				"-replayStream", fmt.Sprint(cfg.ReplayStream), "-count", fmt.Sprint(total), "-every", "1000")
			last := c.await("progress in "+phase, 60*time.Second, func(ev rmrun.Event) bool {
				if ev["event"] != "progress" {
					return false
				}
				if phase == "live" {
					return ev["state"] == "LIVE"
				}
				return ev["state"] == "REPLAY" && num(ev, "next") > 2000
			})
			c.kill()
			cfg.From, cfg.Position, cfg.Count = num(last, "next"), num(last, "position"), total-num(last, "next")
			r, err := rmrun.RunPersistentSubscription(e.c.Aeron, cfg, rmrun.Nop)
			require.NoError(t, err)
			requirePSExact(t, r, total)
		})
	}
}

// TestPSPublisherKillWhileLive SIGKILLs the publisher process while the PersistentSubscription is live. The live
// image goes away (OnLiveLeft); the subscription falls back to replaying the now stopped recording and must have
// delivered every message the publisher sent exactly once, without failing, matching Java.
func TestPSPublisherKillWhileLive(t *testing.T) {
	requireAdversarial(t)
	type outcome struct {
		next, left int64
		failed     bool
	}
	var javaOut, goOut outcome
	run := func(t *testing.T, java bool) outcome {
		e, pub := childPubEnv(t, 27470, 9457, "excl", "-rate", "3000", "-sizes", "64,1500,64,300")
		awaitSentEvent(pub, 5000)
		cfg := e.psConfig(1 << 40)
		if java {
			o := e.oraclePS(1<<40, "timeoutMs=25000")
			o.awaitEvent("liveJoined", 60*time.Second)
			time.Sleep(time.Second)
			pub.kill()
			stop, err := waitStopped(e, 20*time.Second)
			require.NoError(t, err)
			res := o.result(60 * time.Second)
			require.Zero(t, num(res, "gaps")+num(res, "duplicates"), "java %v", res)
			require.Equal(t, stop, num(res, "lastPosition"), "java %v", res)
			return outcome{num(res, "next"), num(res, "left"), res["failed"] == true}
		}
		cfg.Timeout = 25 * time.Second
		killed := false
		cfg.OnState = func(s ps.State, c *rmrun.Checker) {
			if s == ps.StateLive && !killed {
				killed = true
				go func() {
					time.Sleep(time.Second)
					pub.kill()
				}()
			}
		}
		r, err := rmrun.RunPersistentSubscription(e.c.Aeron, cfg, rmrun.Nop)
		require.NoError(t, err)
		stop, err := waitStopped(e, 20*time.Second)
		require.NoError(t, err)
		require.Zero(t, r.Gaps+r.Duplicates+r.Corrupt, "%s", r.Checker)
		require.Equal(t, stop, r.LastPosition, "%s states %v", r.Checker, r.States)
		require.GreaterOrEqual(t, r.Left, 1)
		return outcome{r.Next, int64(r.Left), r.Failed}
	}
	t.Run("java", func(t *testing.T) { javaOut = run(t, true) })
	t.Run("go", func(t *testing.T) { goOut = run(t, false) })
	t.Logf("java %+v go %+v", javaOut, goOut)
	require.Equal(t, javaOut.failed, goOut.failed)
	require.Equal(t, javaOut.left >= 1, goOut.left >= 1)
}

// TestPSDriverKill SIGKILLs the media driver while the PersistentSubscription is live. The client times out the
// driver; the error must surface (ErrorHandler / OnError), Poll must not hang or panic, Close must return promptly,
// and a new client and PersistentSubscription must resume from the last position after the stack restarts.
func TestPSDriverKill(t *testing.T) {
	requireAdversarial(t)
	const total = 30000
	e := newEnv(t, 27480, 9458, rmrun.PubConfig{Count: total, Rate: 3000, Sizes: []int{64, 1500}})
	e.publish()
	e.awaitSent(5000, 30*time.Second)
	cfg := e.psConfig(total)
	cfg.Timeout = 20 * time.Second
	killed := false
	cfg.OnState = func(s ps.State, c *rmrun.Checker) {
		if s == ps.StateLive && !killed {
			killed = true
			killContainer(t, driverName)
		}
	}
	r, err := rmrun.RunPersistentSubscription(e.c.Aeron, cfg, rmrun.Nop)
	require.NoError(t, err)
	require.True(t, killed)
	require.Zero(t, r.Gaps+r.Duplicates+r.Corrupt, "%s", r.Checker)
	require.Less(t, r.CloseDur, 5*time.Second)
	require.NotEmpty(t, e.c.errs.list(), "driver timeout must reach the client ErrorHandler")
	t.Logf("after driver kill: failed=%v reason=%v states=%v errors=%v", r.Failed, r.FailureErr, r.States, r.Errors)

	e.stopPublishing()
	restartStack(t)
	// The recording stopped when the archive died with the driver; a new client replays the rest of it exactly.
	c2 := connect(t)
	arch2 := connectArchive(t, c2.Aeron)
	d, err := arch2.ListRecording(e.recID)
	require.NoError(t, err)
	require.NotNil(t, d)
	require.GreaterOrEqual(t, d.StopPosition, r.LastPosition, "delivered data must be durable in the recording")
}
