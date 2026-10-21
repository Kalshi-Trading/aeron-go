package replaymerge

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lirm/aeron-go/systests/adversarial/replaymerge/internal/rmrun"
)

// TestMergeConsumerKill SIGKILLs a Go ReplayMerge consumer process (rmhelper) in each ReplayMerge state, then
// resumes with a new ReplayMerge from the last position the dead process reported. The stream must continue with
// no gap or duplicate, as with Java: a dead client only loses its own subscription, which the driver reclaims.
func TestMergeConsumerKill(t *testing.T) {
	requireAdversarial(t)
	for _, state := range []string{"REPLAY", "CATCHUP", "MERGED"} {
		t.Run(state, func(t *testing.T) {
			const total = 40000
			e := newEnv(t, 27400, 9400, rmrun.PubConfig{Count: total, Rate: 3000,
				Sizes: []int{64, 1500, 64, 5000}})
			e.publish()
			e.awaitSent(15000, 30*time.Second)
			c := startHelper(t, "merge", "-recording", fmt.Sprint(e.recID), "-session", fmt.Sprint(e.session),
				"-stream", fmt.Sprint(e.stream),
				"-replayDest", fmt.Sprintf("aeron:udp?endpoint=localhost:%d", e.p.control+4),
				"-liveDest", fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d", e.p.control+5,
					e.p.control),
				"-count", fmt.Sprint(total), "-every", "500", "-timeout", "90s")
			// REPLAY lasts until the first replayed fragment, so it is killed on the state event (nothing delivered).
			last := c.await(state, 60*time.Second, func(ev rmrun.Event) bool {
				if state == "REPLAY" {
					return ev["event"] == "state" && ev["state"] == state
				}
				return ev["event"] == "progress" && ev["state"] == state && num(ev, "next") > 1000
			})
			c.kill()
			cfg := e.mergeConfig(total - num(last, "next"))
			cfg.From, cfg.Position = num(last, "next"), num(last, "position")
			r := rmrun.RunReplayMerge(e.c.Aeron, cfg, rmrun.Nop)
			require.Empty(t, r.Failures, "%s", failMessages(r))
			require.True(t, r.Merged)
			requireExact(t, r, total)
		})
	}
}

// TestJavaMergeConsumerKill SIGKILLs the Java ReplayMerge oracle mid catch-up and checks that the driver reclaims
// the dead client without disturbing the recording, and that a Go ReplayMerge on the same live destination port
// merges the whole stream exactly once the driver has timed the dead client out.
func TestJavaMergeConsumerKill(t *testing.T) {
	requireAdversarial(t)
	const total = 70000
	e := newEnv(t, 27400, 9403, rmrun.PubConfig{Count: total, Rate: 3000, Sizes: []int{64, 1500}})
	e.publish()
	e.awaitSent(15000, 30*time.Second)
	o := e.oracleMerge(total)
	o.await("CATCHUP", 60*time.Second, func(ev rmrun.Event) bool {
		return ev["event"] == "state" && ev["state"] == "CATCHUP"
	})
	o.kill()
	cfg := e.mergeConfig(total)
	// Reuse the dead Java client's ports: they are freed when the driver times the client out.
	cfg.ReplayDestination = fmt.Sprintf("aeron:udp?endpoint=localhost:%d", e.p.control+5)
	cfg.LiveDestination = fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d", e.p.control+4,
		e.p.control)
	// Twice the driver's client liveness timeout (3s): the dead client's subscriptions and ports are gone by then.
	time.Sleep(6 * time.Second)
	r := rmrun.RunReplayMerge(e.c.Aeron, cfg, rmrun.Nop)
	require.Empty(t, r.Failures, "%s", failMessages(r))
	require.True(t, r.Merged)
	requireExact(t, r, total)
}
