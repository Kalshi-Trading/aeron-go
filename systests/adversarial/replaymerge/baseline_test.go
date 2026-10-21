package replaymerge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lirm/aeron-go/systests/adversarial/replaymerge/internal/rmrun"
)

// TestBaselinePublisherKinds merges a recorded live stream for each publisher kind (concurrent Publication,
// ExclusivePublication, TryClaim) with fragmented messages, Poll and ControlledPoll, Go and Java side by side.
func TestBaselinePublisherKinds(t *testing.T) {
	requireAdversarial(t)
	for i, tc := range []struct {
		kind       string
		controlled bool
	}{{"pub", false}, {"excl", true}, {"tryclaim", false}} {
		t.Run(tc.kind, func(t *testing.T) {
			const total = 30000
			e := newEnv(t, 27420+i*10, int32(9400+i), rmrun.PubConfig{Kind: tc.kind, Count: total,
				Sizes: []int{64, 200, 5000, 64, 20000}, Rate: 2000})
			e.publish()
			e.awaitSent(5000, 20*time.Second)
			oracle := e.oracleMerge(total)
			cfg := e.mergeConfig(total)
			cfg.Controlled = tc.controlled
			r := rmrun.RunReplayMerge(e.c.Aeron, cfg, rmrun.Nop)
			require.Empty(t, r.Failures)
			require.True(t, r.Merged)
			requireExact(t, r, total)
			res := oracle.result(60 * time.Second)
			require.Equal(t, true, res["merged"], "java %v", res)
			requireOracleExact(t, res, total)
		})
	}
}
