package cluster

import (
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/stretchr/testify/require"
)

// variants drives three clients with different ingress/egress types at once: concurrent Publication + Offer +
// PollEgress, ExclusivePublication + TryClaim + ControlledPollEgress, ExclusivePublication + Offer + PollEgress.
var variants = []senderOpts{{}, {exclusive: true, claim: true, controlled: true}, {exclusive: true}}

func connectVariants(t *testing.T, s *stack) []*client {
	var cs []*client
	for i, o := range variants {
		cs = append(cs, s.connect(int64(i+1), o))
	}
	for _, c := range cs {
		c.sendAcked(t, 120, 30*time.Second)
	}
	return cs
}

// allowedOutage are the offer results Java AeronCluster returns while the leader is unavailable.
var allowedOutage = map[int64]bool{aeron.NotConnected: true, aeron.BackPressured: true, aeron.AdminAction: true,
	aeron.PublicationClosed: true}

// rideOut keeps offering on every client until each has seen a NewLeader event for a member other than old and is
// connected again, asserting no client closes and only Java-equivalent back-pressure codes are returned.
func rideOut(t *testing.T, cs []*client, old int32, timeout time.Duration) int32 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		done := true
		for _, c := range cs {
			r := c.step()
			if r < 0 {
				require.True(t, allowedOutage[r], "tag %d: unexpected offer result %d", c.Tag, r)
			}
			require.False(t, c.AC.IsClosed(), "tag %d closed during failover; errors=%v", c.Tag, c.Errors())
			if len(c.NewLeaders()) == 0 || !c.AC.IsConnected() || c.AC.LeaderMemberId() == old {
				done = false
			}
		}
		if done {
			break
		}
		require.False(t, time.Now().After(deadline), "no failover within %v", timeout)
		time.Sleep(200 * time.Microsecond)
	}
	leader := cs[0].AC.LeaderMemberId()
	for _, c := range cs {
		require.Equal(t, leader, c.AC.LeaderMemberId())
		t.Logf("tag %d: new leaders %v, outage offer results %v, errors %v", c.Tag, c.NewLeaders(), c.results, c.Errors())
	}
	return leader
}

func closeAll(t *testing.T, cs []*client) {
	for _, c := range cs {
		require.NoError(t, c.AC.Close())
		require.True(t, c.AC.IsClosed())
		require.Equal(t, aeron.PublicationClosed, c.Offer(1, 1<<40, 8, false))
	}
}

func TestLeaderKillFailover(t *testing.T) {
	s := newStack(t)
	leaks := leakCheck(t)
	cs := connectVariants(t, s)
	old := s.member(cs[0].AC.LeaderMemberId())
	s.kill(old)
	start := time.Now()
	leader := rideOut(t, cs, int32(old.id), 30*time.Second)
	t.Logf("failover %d -> %d in %v", old.id, leader, time.Since(start))
	for _, c := range cs {
		c.sendAcked(t, 120, 30*time.Second)
	}
	s.restart(old)
	for _, c := range cs {
		c.sendAcked(t, 20, 30*time.Second)
	}
	verify(t, s.dump(t, cs[0], s.members), cs...)
	closeAll(t, cs)
	leaks()
}
