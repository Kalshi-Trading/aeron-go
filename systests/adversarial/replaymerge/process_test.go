package replaymerge

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lirm/aeron-go/archive/replaymerge"
	"github.com/lirm/aeron-go/systests/adversarial/replaymerge/internal/rmrun"
)

// childPubEnv recreates the stack and runs the recorded live publisher as an rmhelper child process.
func childPubEnv(t *testing.T, base int, stream int32, kind string, flags ...string) (*env, *child) {
	freshStack(t)
	e := &env{t: t, p: portsFor(base), stream: stream}
	e.c = connect(t)
	e.arch = connectArchive(t, e.c.Aeron)
	pub := startHelper(t, "pub", append([]string{"-channel", e.p.pubChannel(""), "-stream", fmt.Sprint(stream),
		"-kind", kind, "-every", "500"}, flags...)...)
	e.session = int32(num(pub.awaitEvent("ready", 20*time.Second), "sessionId"))
	e.recID = startRecording(t, e.arch, e.p, stream, e.session)
	return e, pub
}

func awaitSentEvent(c *child, n int64) {
	c.await(fmt.Sprintf("sent %d", n), 60*time.Second, func(ev rmrun.Event) bool {
		return ev["event"] == "sent" && num(ev, "next") >= n
	})
}

// TestPublisherKillAtEachState SIGKILLs the publisher process when the merge reaches each state. The recording
// stops; the live image goes away after the image liveness timeout and the unavailable image handler fires. Before
// the merge there is no live stream to join so, as in Java, the merge delivers the whole recording exactly and then
// fails with no progress. After the merge the merge stays merged (Java does not fail a merged ReplayMerge) having
// delivered everything the publisher sent before dying.
func TestPublisherKillAtEachState(t *testing.T) {
	requireAdversarial(t)
	for i, state := range []replaymerge.State{replaymerge.StateReplay, replaymerge.StateCatchup,
		replaymerge.StateAttemptLiveJoin, replaymerge.StateMerged} {
		base, stream := 27420+i*10, int32(9430+i)
		const total = 1 << 40 // until failure / stop
		type outcome struct {
			merged, failed bool
			next           int64
		}
		var javaOut, goOut outcome
		t.Run(state.String()+"/java", func(t *testing.T) {
			e, pub := childPubEnv(t, base, stream, "excl", "-rate", "4000", "-sizes", "64,1500,64,300")
			awaitSentEvent(pub, 8000)
			o := e.oracleMerge(total, "progressTimeoutMs=3000", "timeoutMs=60000")
			o.await("state", 60*time.Second, func(ev rmrun.Event) bool {
				return ev["event"] == "state" && ev["state"] == state.String()
			})
			pub.kill()
			rec, err := waitStopped(e, 20*time.Second)
			require.NoError(t, err)
			res := o.result(60 * time.Second)
			require.Zero(t, num(res, "gaps"), "java %v", res)
			require.Zero(t, num(res, "duplicates"), "java %v", res)
			javaOut = outcome{merged: res["merged"] == true, failed: num(res, "failures") > 0, next: num(res, "next")}
			require.Equal(t, rec, num(res, "lastPosition"), "java delivered up to the recording stop position")
		})
		t.Run(state.String()+"/go", func(t *testing.T) {
			e, pub := childPubEnv(t, base, stream, "excl", "-rate", "4000", "-sizes", "64,1500,64,300")
			awaitSentEvent(pub, 8000)
			base := snapshot(t)
			cfg := e.mergeConfig(total)
			cfg.ProgressTimeoutMs = 3000
			cfg.Timeout = 60 * time.Second
			var stopPos int64
			cfg.OnState = atState(state, func() { pub.kill() })
			done := make(chan rmrun.MergeResult, 1)
			// After the merge nothing fails, so stop consuming once the live image is gone and the recording stopped.
			stopAt := make(chan struct{})
			go func() {
				p, err := waitStopped(e, 30*time.Second)
				stopPos = p
				if err == nil {
					awaitTrue(t, "unavailable image", 20*time.Second, func() bool {
						return e.c.images.count(true, e.session) > 0
					})
				}
				time.Sleep(3500 * time.Millisecond)
				close(stopAt)
			}()
			go func() {
				c := cfg
				c.Timeout = 50 * time.Second
				orig := c.OnState
				c.OnState = func(s replaymerge.State, ch *rmrun.Checker) { orig(s, ch) }
				done <- runUntil(e, c, stopAt)
			}()
			r := <-done
			<-stopAt
			require.Zero(t, r.Gaps, "%s", r.Checker)
			require.Zero(t, r.Duplicates, "%s", r.Checker)
			require.Zero(t, r.Corrupt, "%s", r.Checker)
			require.Equal(t, stopPos, r.LastPosition, "go delivered up to the recording stop position: %s", r.Checker)
			goOut = outcome{merged: r.Merged, failed: len(r.Failures) > 0, next: r.Next}
			for _, err := range r.Failures {
				require.ErrorIs(t, err, replaymerge.ErrNoProgress)
			}
			requireNoLeaks(t, base)
		})
		t.Logf("%s java %+v go %+v", state, javaOut, goOut)
		require.Equal(t, javaOut.merged, goOut.merged, "%s merged: java %+v go %+v", state, javaOut, goOut)
		require.Equal(t, javaOut.failed, goOut.failed, "%s failed: java %+v go %+v", state, javaOut, goOut)
	}
}

// runUntil runs a merge until it fails or stop is closed.
func runUntil(e *env, cfg rmrun.MergeConfig, stop <-chan struct{}) rmrun.MergeResult {
	orig := cfg.OnState
	stopped := false
	cfg.Count = 1 << 40
	cfg.OnState = func(s replaymerge.State, c *rmrun.Checker) {
		if orig != nil {
			orig(s, c)
		}
	}
	return rmrun.RunReplayMergeUntil(e.c.Aeron, cfg, rmrun.Nop, func() bool {
		if !stopped {
			select {
			case <-stop:
				stopped = true
			default:
			}
		}
		return stopped
	})
}

func waitStopped(e *env, timeout time.Duration) (int64, error) {
	deadline := time.Now().Add(timeout)
	for {
		d, err := e.arch.ListRecording(e.recID)
		if err == nil && d.StopPosition >= 0 {
			return d.StopPosition, nil
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("recording %d not stopped: %v", e.recID, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
