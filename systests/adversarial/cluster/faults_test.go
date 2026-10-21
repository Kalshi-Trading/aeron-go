package cluster

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/systests/adversarial/cluster/internal/advproto"
	"github.com/stretchr/testify/require"
)

func (s *stack) leader(c *client) *member { return s.member(c.AC.LeaderMemberId()) }

func (s *stack) running(m *member) bool {
	out, err := runErr("docker", "inspect", "-f", "{{.State.Running}}", m.container)
	return err == nil && strings.TrimSpace(out) == "true"
}

// snapshot requests a snapshot through the client's admin request and waits for every live service to take it.
func (s *stack) snapshot(t *testing.T, c *client, live []*member) {
	t.Helper()
	before := make([]int, len(live))
	for i, m := range live {
		before[i] = m.svc.count("EVENT snapshot")
	}
	waitForPoll(t, 10*time.Second, "admin snapshot request", func() bool {
		ok, err := c.AC.SendAdminRequestToTakeASnapshot(time.Now().UnixNano())
		require.NoError(t, err)
		return ok
	}, func() { c.Poll(c.o.controlled) })
	for i, m := range live {
		i, m := i, m
		waitForPoll(t, 30*time.Second, "snapshot on "+m.name, func() bool {
			return m.svc.count("EVENT snapshot") > before[i]
		}, func() { c.Poll(c.o.controlled) })
	}
}

// pumpFor keeps every client offering for d.
func pumpFor(t *testing.T, cs []*client, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		for _, c := range cs {
			if r := c.step(); r < 0 {
				require.True(t, allowedOutage[r], "tag %d: unexpected offer result %d", c.Tag, r)
			}
			require.False(t, c.AC.IsClosed(), "tag %d closed; errors=%v", c.Tag, c.Errors())
		}
		time.Sleep(time.Millisecond)
	}
}

func idleAll(cs []*client) {
	for _, c := range cs {
		c.idle()
	}
}

func requireContiguous(t *testing.T, r advproto.Ranges, what string) {
	t.Helper()
	if len(r) > 0 {
		require.Len(t, r, 1, "%s: gap in committed messages %v", what, r)
		require.Equal(t, int64(1), r[0][0], "%s: committed messages do not start at 1: %v", what, r)
	}
}

func TestFollowerKillNoInterruption(t *testing.T) {
	s := newStack(t)
	leaks := leakCheck(t)
	cs := connectVariants(t, s)
	leader := s.leader(cs[0])
	followers := s.others(leader)
	s.kill(followers[0])
	pumpFor(t, cs, 3*time.Second)
	for _, c := range cs {
		c.sendAcked(t, 150, 20*time.Second)
		require.Empty(t, c.NewLeaders(), "tag %d: follower kill must not change leader", c.Tag)
		require.True(t, c.AC.IsConnected())
	}
	s.restart(followers[0])
	// Kill only the second follower's container: its Go service must notice the dead driver (driver timeout 10s,
	// Java: "unexpected Aeron close") and exit on its own rather than hang.
	run(t, "docker", "kill", "-s", "KILL", followers[1].container)
	for _, c := range cs {
		c.sendAcked(t, 150, 20*time.Second)
	}
	waitForPoll(t, 10*time.Second+15*time.Second, followers[1].svc.name+" exit after driver death",
		followers[1].svc.exited, func() { idleAll(cs) })
	t.Logf("service after driver SIGKILL: %s", followers[1].svc.output())
	s.restart(followers[1])
	d := s.dump(t, cs[0], s.members)
	verify(t, d, cs...)
	for _, c := range cs {
		requireContiguous(t, d.State.Applied[c.Tag], fmt.Sprint("tag ", c.Tag))
	}
	closeAll(t, cs)
	leaks()
}

func TestMajorityKillClosesCleanlyAndRecovers(t *testing.T) {
	s := newStack(t)
	leaks := leakCheck(t)
	cs := connectVariants(t, s)
	leader := s.leader(cs[0])
	survivor := s.others(leader)[0]
	killed := s.others(survivor)
	for _, m := range killed {
		s.kill(m)
	}
	start := time.Now()
	// Java: egress image closes (image liveness), AWAIT_NEW_LEADER, then close() once newLeaderTimeout expires.
	closedAt := map[int64]time.Duration{}
	waitForPoll(t, clientLiveness+newLeaderTimeout+15*time.Second, "all clients closed",
		func() bool { return len(closedAt) == len(cs) }, func() {
			for _, c := range cs {
				if _, ok := closedAt[c.Tag]; ok {
					continue
				}
				if r := c.step(); r < 0 {
					require.True(t, allowedOutage[r], "tag %d: unexpected offer result %d", c.Tag, r)
				}
				if c.AC.IsClosed() {
					closedAt[c.Tag] = time.Since(start)
				}
			}
		})
	for _, c := range cs {
		t.Logf("tag %d closed after %v; results %v errors %v", c.Tag, closedAt[c.Tag], c.results, c.Errors())
		require.Equal(t, aeron.PublicationClosed, c.Offer(advproto.KindData, c.next, 8, false))
		require.GreaterOrEqual(t, closedAt[c.Tag], newLeaderTimeout, "closed before newLeaderTimeout")
	}
	// A new client cannot connect without quorum and must give up after its message timeout.
	connStart := time.Now()
	_, err := s.dial(99, senderOpts{timeout: 3 * time.Second})
	require.Error(t, err)
	took := time.Since(connStart)
	t.Logf("no-quorum connect failed after %v: %v", took, err)
	require.Less(t, took, 3*time.Second+2*time.Second)
	for _, m := range killed {
		s.restart(m)
	}
	c := s.connect(4, senderOpts{})
	c.sendAcked(t, 100, 30*time.Second)
	d := s.dump(t, c, s.members)
	verify(t, d, append(cs, c)...)
	require.NoError(t, c.AC.Close())
	leaks()
}

func TestFollowersKillLeaderWithoutQuorum(t *testing.T) {
	s := newStack(t)
	cs := connectVariants(t, s)
	leader := s.leader(cs[0])
	followers := s.others(leader)
	acked := make([]int64, len(cs))
	for i, c := range cs {
		acked[i] = c.Acked().Count()
	}
	for _, m := range followers {
		s.kill(m)
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		for _, c := range cs {
			if !c.AC.IsClosed() {
				if r := c.step(); r < 0 {
					require.True(t, allowedOutage[r], "tag %d: unexpected offer result %d", c.Tag, r)
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	for i, c := range cs {
		// Nothing can commit without a quorum.
		require.LessOrEqual(t, c.Acked().Count(), acked[i]+0, "tag %d acked without quorum", c.Tag)
		t.Logf("tag %d: closed=%v leaders=%v results=%v", c.Tag, c.AC.IsClosed(), c.NewLeaders(), c.results)
	}
	_, err := s.dial(98, senderOpts{timeout: 2 * time.Second})
	require.Error(t, err, "connect must time out without quorum")
	for _, m := range followers {
		s.restart(m)
	}
	c := s.connect(5, senderOpts{})
	c.sendAcked(t, 100, 30*time.Second)
	d := s.dump(t, c, s.members)
	verify(t, d, append(cs, c)...)
}

func TestSigstopLeader(t *testing.T) {
	s := newStack(t)
	leaks := leakCheck(t)
	cs := connectVariants(t, s)
	old := s.leader(cs[0])
	s.signal(old, "STOP")
	start := time.Now()
	leader := rideOut(t, cs, int32(old.id), 30*time.Second)
	t.Logf("failover from stopped %d -> %d in %v", old.id, leader, time.Since(start))
	// Resume before the frozen driver would time out its own consensus module/archive clients (liveness 5s).
	if d := 4*time.Second - time.Since(start); d > 0 {
		pumpFor(t, cs, d)
	}
	s.signal(old, "CONT")
	for _, c := range cs {
		c.sendAcked(t, 150, 30*time.Second)
	}
	d := s.dump(t, cs[0], s.members)
	verify(t, d, cs...)
	require.False(t, old.svc.exited(), "service of the stopped member exited: %s", old.svc.output())
	closeAll(t, cs)
	leaks()
}

// A SIGSTOP longer than the driver's client liveness timeout: on SIGCONT the frozen member's driver times out its
// own consensus module and archive clients and the member terminates (Java behaviour); its Go service must observe
// the closed commit-pos counter and exit instead of hanging, and the member must rejoin after a restart.
func TestSigstopLeaderBeyondLiveness(t *testing.T) {
	s := newStack(t)
	leaks := leakCheck(t)
	cs := connectVariants(t, s)
	old := s.leader(cs[0])
	s.signal(old, "STOP")
	rideOut(t, cs, int32(old.id), 30*time.Second)
	pumpFor(t, cs, 2*clientLiveness)
	s.signal(old, "CONT")
	for _, c := range cs {
		c.sendAcked(t, 100, 30*time.Second)
	}
	waitForPoll(t, 30*time.Second, old.svc.name+" exit", old.svc.exited, func() { idleAll(cs) })
	t.Logf("service of resumed member: %s", old.svc.output())
	s.restart(old)
	for _, c := range cs {
		c.sendAcked(t, 50, 30*time.Second)
	}
	verify(t, s.dump(t, cs[0], s.members), cs...)
	closeAll(t, cs)
	leaks()
}

func TestPartitionLeader(t *testing.T) {
	s := newStack(t)
	leaks := leakCheck(t)
	cs := connectVariants(t, s)
	old := s.leader(cs[0])
	s.partition(old)
	start := time.Now()
	leader := rideOut(t, cs, int32(old.id), 30*time.Second)
	t.Logf("failover from partitioned %d -> %d in %v", old.id, leader, time.Since(start))
	for _, c := range cs {
		c.sendAcked(t, 100, 30*time.Second)
	}
	s.heal(old)
	for _, c := range cs {
		c.sendAcked(t, 100, 30*time.Second)
	}
	verify(t, s.dump(t, cs[0], s.members), cs...)
	closeAll(t, cs)
	leaks()
}

func TestPartitionFollowerAndNetemLeader(t *testing.T) {
	s := newStack(t)
	cs := connectVariants(t, s)
	leader := s.leader(cs[0])
	f := s.others(leader)
	s.partition(f[0])
	s.netns(leader, "tc", "qdisc", "add", "dev", "eth0", "root", "netem", "loss", "5%", "delay", "10ms", "5ms")
	for _, c := range cs {
		c.sendAcked(t, 200, 60*time.Second)
		require.Empty(t, c.NewLeaders(), "tag %d", c.Tag)
	}
	s.heal(f[0])
	s.netns(leader, "tc", "qdisc", "del", "dev", "eth0", "root")
	for _, c := range cs {
		c.sendAcked(t, 50, 30*time.Second)
	}
	d := s.dump(t, cs[0], s.members)
	verify(t, d, cs...)
	for _, c := range cs {
		requireContiguous(t, d.State.Applied[c.Tag], fmt.Sprint("tag ", c.Tag))
	}
}

// Killing the Go service terminates its consensus module (Java: "Aeron client in service closed unexpectedly"
// once the driver times the service client out); the restarted member must load the snapshot and replay the log.
func TestServiceProcessKillRecoversFromSnapshot(t *testing.T) {
	s := newStack(t)
	cs := connectVariants(t, s)
	s.snapshot(t, cs[0], s.members)
	for _, c := range cs {
		c.sendAcked(t, 100, 30*time.Second)
	}
	leader := s.leader(cs[0])
	for _, m := range []*member{s.others(leader)[0], leader} {
		m.svc.kill()
		waitForPoll(t, clientLiveness+30*time.Second, m.container+" to terminate", func() bool { return !s.running(m) },
			func() { idleAll(cs) })
		out, _ := runErr("docker", "logs", "--tail", "40", m.container)
		t.Logf("%s terminated after service kill: %s", m.container, out)
		if m == leader {
			rideOut(t, cs, int32(leader.id), 30*time.Second)
		} else {
			pumpFor(t, cs, time.Second)
		}
		s.restart(m)
		m.svc.waitLine(t, "EVENT start", 60*time.Second)
		require.Contains(t, m.svc.output(), "snapshot=true", "restarted service did not load the snapshot")
		for _, c := range cs {
			c.sendAcked(t, 100, 30*time.Second)
		}
	}
	verify(t, s.dump(t, cs[0], s.members), cs...)
}

func sessionID(t *testing.T, p *proc, polls ...func()) int64 {
	l := p.waitLine(t, "SESSION ", 60*time.Second, polls...)
	id, err := strconv.ParseInt(strings.TrimPrefix(l, "SESSION "), 10, 64)
	require.NoError(t, err)
	return id
}

func (s *stack) clientProc(t *testing.T, tag int64, args ...string) *proc {
	return startProc(t, fmt.Sprintf("advclient-%d", tag), "advclient", append([]string{"-aeron-dir", clientDir,
		"-endpoints", s.endpoints, "-egress", s.egressChannel(), "-tag", fmt.Sprint(tag)}, args...)...)
}

func TestClientProcessKillSessionTimeout(t *testing.T) {
	s := newStack(t)
	live := s.connect(1, senderOpts{})
	live.sendAcked(t, 50, 30*time.Second)
	obs, err := aeron.Connect(aeron.NewContext().AeronDir(clientDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = obs.Close() })
	live.obs = obs
	obsLdr, err := aeron.Connect(aeron.NewContext().AeronDir(s.leader(live).aeronDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = obsLdr.Close() })
	live.obsLdr = obsLdr

	idle := s.clientProc(t, 10, "-count", "20", "-hold")
	idleID := sessionID(t, idle, live.idle)
	idle.waitLine(t, "ACKED 20", 30*time.Second, live.idle)
	busy := s.clientProc(t, 11, "-count", "1000000", "-size", "300")
	busyID := sessionID(t, busy, live.idle)
	for end := time.Now().Add(500 * time.Millisecond); time.Now().Before(end); time.Sleep(time.Millisecond) {
		live.idle()
	}
	idle.kill()
	busy.kill()
	killed := time.Now()
	for _, m := range s.members {
		for _, id := range []int64{idleID, busyID} {
			line := fmt.Sprintf("close session=%d reason=TIMEOUT", id)
			waitForPoll(t, clientLiveness+sessionTimeout+15*time.Second, m.name+" "+line, func() bool {
				_, ok := m.svc.find(line)
				return ok
			}, live.idle)
		}
	}
	took := time.Since(killed)
	t.Logf("sessions %d,%d timed out after %v", idleID, busyID, took)
	require.GreaterOrEqual(t, took, sessionTimeout-time.Second)
	live.sendAcked(t, 50, 30*time.Second)
	require.Empty(t, live.NewLeaders())
	d := s.dump(t, live, s.members)
	verify(t, d, live)
	require.Equal(t, "TIMEOUT", d.State.ClosedSessions[idleID])
	require.Equal(t, "TIMEOUT", d.State.ClosedSessions[busyID])
	require.Equal(t, int64(20), d.State.Applied[10].Count())
	requireContiguous(t, d.State.Applied[10], "idle client")
	requireContiguous(t, d.State.Applied[11], "killed mid-stream client")
	t.Logf("killed mid-stream client committed %d messages", d.State.Applied[11].Count())
}

func TestRollingLeaderKills(t *testing.T) {
	s := newStack(t)
	leaks := leakCheck(t)
	cs := connectVariants(t, s)
	for round := 0; round < 3; round++ {
		old := s.leader(cs[0])
		s.kill(old)
		rideOut(t, cs, int32(old.id), 30*time.Second)
		for _, c := range cs {
			c.sendAcked(t, 60, 30*time.Second)
		}
		s.restart(old)
		for _, c := range cs {
			c.sendAcked(t, 20, 30*time.Second)
		}
	}
	verify(t, s.dump(t, cs[0], s.members), cs...)
	closeAll(t, cs)
	leaks()
}

func TestSoloNodeCrashRestartFromSnapshotAndLog(t *testing.T) {
	s := newSoloStack(t)
	leaks := leakCheck(t)
	c := s.connect(1, senderOpts{})
	c.sendAcked(t, 100, 30*time.Second)
	s.snapshot(t, c, s.members)
	c.sendAcked(t, 100, 30*time.Second)
	m := s.members[0]
	s.kill(m)
	start := time.Now()
	waitForPoll(t, clientLiveness+newLeaderTimeout+15*time.Second, "client close", func() bool { return c.AC.IsClosed() },
		func() {
			if r := c.step(); r < 0 {
				require.True(t, allowedOutage[r], "unexpected offer result %d", r)
			}
		})
	t.Logf("client closed %v after solo member kill; results %v", time.Since(start), c.results)
	s.restart(m)
	m.svc.waitLine(t, "EVENT start", 60*time.Second)
	require.Contains(t, m.svc.output(), "snapshot=true")
	c2 := s.connect(2, senderOpts{exclusive: true, claim: true, controlled: true})
	c2.sendAcked(t, 100, 30*time.Second)
	d := s.dump(t, c2, s.members)
	verify(t, d, c, c2)
	requireContiguous(t, d.State.Applied[2], "tag 2")
	require.NoError(t, c2.AC.Close())
	require.NoError(t, c.AC.Close())
	leaks()
}
