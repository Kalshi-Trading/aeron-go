package replaymerge

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lirm/aeron-go/archive/replaymerge"
	"github.com/lirm/aeron-go/systests/adversarial/replaymerge/internal/rmrun"
)

// TestLiveLossDuringJoin drops 10% of the live (MDC control + data) packets from the catch-up through the merge.
// Aeron is reliable over loss (NAK/retransmit), so the merge must complete with exact ordering, as Java does.
func TestLiveLossDuringJoin(t *testing.T) {
	requireAdversarial(t)
	fc := faultCase{name: "loss", state: replaymerge.StateCatchup, base: 27460, stream: 9440, backlog: 8000,
		total: 40000,
		fault: func(t *testing.T, e *env) {
			netem(t, "loss 10%", e.p.live, e.p.control+4)
		}}
	fc.compare(t)
}

// TestReplayPartitionDuringCatchup blackholes the replay destination ports for longer than the image liveness
// timeout during the catch-up. The replay image goes away: like Java (IllegalStateException "ReplayMerge Image
// closed unexpectedly") the merge fails, and the retry from the last delivered position merges exactly.
func TestReplayPartitionDuringCatchup(t *testing.T) {
	requireAdversarial(t)
	fc := faultCase{name: "replay-partition", state: replaymerge.StateCatchup, base: 27470, stream: 9441,
		backlog: 8000, total: 60000, retries: 1,
		fault: func(t *testing.T, e *env) {
			clear := netem(t, "loss 100%", e.p.replay, e.p.control+5)
			go func() {
				time.Sleep(6 * time.Second)
				clear()
			}()
		}}
	fc.compare(t)
}

// TestLivePartitionAfterMerge blackholes the live stream after the merge for longer than the image liveness
// timeout. A merged ReplayMerge does not fail (Java's MERGED state does no work): the image goes away, the
// unavailable image handler fires, and a new ReplayMerge from the last delivered position merges exactly.
func TestLivePartitionAfterMerge(t *testing.T) {
	requireAdversarial(t)
	const total = 60000
	e := newEnv(t, 27480, 9442, rmrun.PubConfig{Count: total, Rate: 4000, Sizes: []int{64, 1500, 64, 300}})
	e.publish()
	e.awaitSent(8000, 30*time.Second)
	cfg := e.mergeConfig(total)
	var partitioned bool
	stop := func() bool { return partitioned && e.c.images.count(true, e.session) > 0 }
	// Only the live destination's port: the publication's control port also carries the recording's data and
	// status messages, and cutting it stops the recording, which is a different scenario.
	cfg.OnState = atState(replaymerge.StateMerged, func() {
		netem(t, "loss 100%", e.p.live)
		partitioned = true
	})
	r := rmrun.RunReplayMergeUntil(e.c.Aeron, cfg, rmrun.Nop, stop)
	require.Empty(t, r.Failures, "a merged ReplayMerge must not fail when the live image goes away")
	require.True(t, r.Merged)
	require.Zero(t, r.Gaps+r.Duplicates+r.Corrupt, "%s", r.Checker)
	clearNetem()

	cfg2 := e.mergeConfig(total - r.Next)
	cfg2.From, cfg2.Position = r.Next, r.LastPosition
	r2 := rmrun.RunReplayMerge(e.c.Aeron, cfg2, rmrun.Nop)
	require.Empty(t, r2.Failures)
	require.True(t, r2.Merged)
	requireExact(t, r2, total)
}

// TestReplaySlowerThanLive rate-limits the replay path with netem so the live stream outruns the replay while the
// publisher is fast. The merge must not join while it is behind (no gap, no early MERGED), and must merge exactly
// once the publisher slows down, as Java does.
func TestReplaySlowerThanLive(t *testing.T) {
	requireAdversarial(t)
	const fast, total = 30000, 45000
	e := newEnv(t, 27490, 9443, rmrun.PubConfig{Count: total, Rate: 6000, SlowAfter: fast, SlowRate: 500,
		Sizes: []int{64, 1500, 64, 1000}})
	e.publish()
	e.awaitSent(5000, 30*time.Second)
	netem(t, "rate 16mbit", e.p.replay)
	cfg := e.mergeConfig(total)
	cfg.Timeout = 3 * time.Minute
	var states []string
	cfg.OnState = func(s replaymerge.State, c *rmrun.Checker) {
		states = append(states, fmt.Sprintf("%s@%d/sent=%d", s, c.Next, e.sent.Load()))
	}
	r := rmrun.RunReplayMerge(e.c.Aeron, cfg, rmrun.Nop)
	t.Logf("states %v", states)
	require.Empty(t, r.Failures, "%s", failMessages(r))
	require.True(t, r.Merged)
	requireExact(t, r, total)
}
