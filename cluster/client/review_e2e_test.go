package client

import (
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/stretchr/testify/require"
)

func countCountersWithLabel(a *aeron.Aeron, substr string) int {
	n := 0
	a.CounterReader().Scan(func(c counters.Counter) {
		if strings.Contains(c.Label, substr) {
			n++
		}
	})
	return n
}

func awaitCounterCount(t *testing.T, a *aeron.Aeron, substr string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for countCountersWithLabel(a, substr) != want {
		require.True(t, time.Now().Before(deadline), "counters with %q: got %d want %d", substr,
			countCountersWithLabel(a, substr), want)
		time.Sleep(10 * time.Millisecond)
	}
}

func e2eAeron(t *testing.T, ctx *Context) *aeron.Aeron {
	t.Helper()
	a, err := aeron.Connect(aeron.NewContext().AeronDir(ctx.AeronDirectoryName))
	require.NoError(t, err)
	return a
}

// Java: a second conclude throws ConcurrentConcludeException and AeronCluster.connect rethrows it without closing
// the context, so the Aeron client owned by the live AeronCluster survives.
func TestE2EConcludeTwiceKeepsOwnedAeronOpen(t *testing.T) {
	nodes := e2eNodes(t)
	listener := &recordingListener{}
	cl, err := Connect(e2eContext(nodes, listener))
	require.NoError(t, err)
	defer cl.Close()
	require.True(t, cl.Context().OwnsAeronClient)

	_, err = NewAsyncConnect(cl.Context())
	require.ErrorIs(t, err, ErrConcurrentConclude)
	_, err = Connect(cl.Context())
	require.ErrorIs(t, err, ErrConcurrentConclude)
	require.False(t, cl.Context().Aeron.IsClosed())

	offerAll(t, cl, []byte("still-alive"))
	pollUntil(t, cl, 10*time.Second, func() bool { return len(listener.messages) == 1 })
	require.Equal(t, "still-alive", string(listener.messages[0]))
}

// Java AsyncConnect.close calls asyncRemoveSubscription/asyncRemovePublication for registrations still in flight.
func TestE2EAsyncConnectCloseRemovesPendingRegistrations(t *testing.T) {
	nodes := e2eNodes(t)
	ctx := e2eContext(nodes, &recordingListener{})
	a := e2eAeron(t, ctx)
	defer a.Close()
	tag := "asyncclose" + strconv.FormatInt(time.Now().UnixNano(), 36)
	ctx.Aeron = a
	ctx.EgressChannel = "aeron:udp?endpoint=localhost:0|alias=" + tag + "-egress"
	ctx.IngressChannel = "aeron:udp?term-length=64k|alias=" + tag + "-ingress"
	ctx.IngressEndpoints = ""
	ctx.IngressChannel += "|endpoint=localhost:9002"

	async, err := NewAsyncConnect(ctx)
	require.NoError(t, err)
	_, err = async.Poll() // sends ADD_SUBSCRIPTION for egress, the driver has not answered yet
	require.NoError(t, err)
	require.Equal(t, AsyncConnectCreateEgressSubscription, async.State())
	async.Close()
	time.Sleep(500 * time.Millisecond)
	awaitCounterCount(t, a, tag+"-egress", 0)

	ctx2 := e2eContext(nodes, &recordingListener{})
	ctx2.Aeron = a
	ctx2.EgressChannel = "aeron:udp?endpoint=localhost:0|alias=" + tag + "-egress2"
	ctx2.IngressChannel = "aeron:udp?term-length=64k|alias=" + tag + "-ingress2|endpoint=localhost:9002"
	ctx2.IngressEndpoints = ""
	async, err = NewAsyncConnect(ctx2)
	require.NoError(t, err)
	deadline := time.Now().Add(10 * time.Second)
	for async.State() != AsyncConnectCreateIngressPublications {
		require.True(t, time.Now().Before(deadline))
		_, err = async.Poll()
		require.NoError(t, err)
	}
	_, err = async.Poll() // sends ADD_EXCLUSIVE_PUBLICATION for ingress
	require.NoError(t, err)
	async.Close()
	time.Sleep(500 * time.Millisecond)
	awaitCounterCount(t, a, tag+"-egress2", 0)
	awaitCounterCount(t, a, tag+"-ingress2", 0)
}

// Java addNewLeaderIngressPublication rethrows a RegistrationException that is not
// RESOURCE_TEMPORARILY_UNAVAILABLE, and onNewLeader then does not notify the listeners.
func TestE2ENewLeaderIngressErrorFailsFast(t *testing.T) {
	nodes := e2eNodes(t)
	listener := &recordingListener{}
	ctx := e2eContext(nodes, listener)
	ctx.MessageTimeout = 8 * time.Second
	cl, err := Connect(ctx)
	require.NoError(t, err)
	defer cl.Close()

	start := time.Now()
	err = cl.OnNewLeader(cl.ClusterSessionId(), cl.LeadershipTermId(), cl.LeaderMemberId(),
		strconv.Itoa(int(cl.LeaderMemberId()))+"=localhost:notaport")
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrTimeout), "%v", err)
	var regErr *aeron.RegistrationError
	require.True(t, errors.As(err, &regErr), "%v", err)
	require.Less(t, time.Since(start), 2*time.Second)
	require.Empty(t, listener.newLeaders, "listeners are not notified when the new leader publication fails")
	require.False(t, cl.IsConnected())
}

// Java onNewLeader clears the fragment assemblers in place; replacing the controlled assembler loses fragments
// already buffered by the instance the in-progress ControlledPoll is using.
func TestE2EOnNewLeaderReconnectsAndKeepsAssemblers(t *testing.T) {
	nodes := e2eNodes(t)
	listener := &recordingListener{}
	cl, err := Connect(e2eContext(nodes, listener))
	require.NoError(t, err)
	defer cl.Close()

	fa, cfa := cl.fragmentAssembler, cl.controlledFragmentAssembler
	oldPub := cl.IngressPublication()
	require.NoError(t, cl.OnNewLeader(cl.ClusterSessionId(), cl.LeadershipTermId(), cl.LeaderMemberId(),
		cl.Context().IngressEndpoints))
	require.Same(t, fa, cl.fragmentAssembler)
	require.Same(t, cfa, cl.controlledFragmentAssembler)
	require.Equal(t, []int32{cl.LeaderMemberId()}, listener.newLeaders)
	require.True(t, oldPub.IsClosed())
	require.False(t, cl.IsConnected(), "AWAIT_NEW_LEADER_CONNECTION until an offer succeeds")

	offerAll(t, cl, []byte("after-new-leader"))
	require.True(t, cl.IsConnected())
	pollUntil(t, cl, 10*time.Second, func() bool { return len(listener.messages) == 1 })
	require.Equal(t, "after-new-leader", string(listener.messages[0]))
}

// Java AsyncConnect.updateMembers: a follower answers REDIRECT with the full ingress endpoint list and the client
// reconnects to the leader even though the leader was not in Context.IngressEndpoints.
func TestE2ERedirectFromFollowerOnlyEndpoint(t *testing.T) {
	nodes := e2eNodes(t)
	if nodes != 3 {
		t.Skip("requires CLUSTER_E2E_NODES=3")
	}
	first, err := Connect(e2eContext(nodes, &recordingListener{}))
	require.NoError(t, err)
	leader := first.LeaderMemberId()
	require.NoError(t, first.Close())

	follower := (leader + 1) % 3
	listener := &recordingListener{}
	ctx := e2eContext(nodes, listener)
	ctx.IngressEndpoints = strconv.Itoa(int(follower)) + "=localhost:" + strconv.Itoa(9002+100*int(follower))
	cl, err := Connect(ctx)
	require.NoError(t, err)
	defer cl.Close()
	require.Equal(t, leader, cl.LeaderMemberId())
	require.Contains(t, cl.IngressPublication().Channel(), "localhost:"+strconv.Itoa(9002+100*int(leader)))

	offerAll(t, cl, []byte("redirected"))
	pollUntil(t, cl, 10*time.Second, func() bool { return len(listener.messages) == 1 })
	require.Equal(t, "redirected", string(listener.messages[0]))
}

// Java pollEgress: once the leader's egress image closes the client waits newLeaderTimeoutNs (2x the leader
// heartbeat timeout reported on connect) for a NewLeaderEvent, then closes itself.
func TestE2ENoQuorumNewLeaderTimeoutCloses(t *testing.T) {
	nodes := e2eNodes(t)
	if nodes != 3 {
		t.Skip("requires CLUSTER_E2E_NODES=3")
	}
	listener := &recordingListener{}
	cl, err := Connect(e2eContext(nodes, listener))
	require.NoError(t, err)
	defer cl.Close()
	require.Equal(t, 4*time.Second, cl.Context().NewLeaderTimeout, "2x leader.heartbeat.timeout=2s")

	leader := cl.LeaderMemberId()
	follower := (leader + 1) % 3
	victims := []string{"node" + strconv.Itoa(int(leader)), "node" + strconv.Itoa(int(follower))}
	composeFile := os.Getenv("CLUSTER_COMPOSE_FILE")
	if composeFile == "" {
		composeFile = "../../docker/cluster.compose.yml"
	}
	out, err := exec.Command("docker", append([]string{"compose", "-f", composeFile, "--profile", "three", "kill"},
		victims...)...).CombinedOutput()
	require.NoError(t, err, string(out))
	defer func() {
		out, err := exec.Command("docker", append([]string{"compose", "-f", composeFile, "--profile", "three", "up",
			"-d"}, victims...)...).CombinedOutput()
		if err != nil {
			t.Logf("restart failed: %v %s", err, out)
		}
		awaitClusterUsable(t, nodes)
	}()

	start := time.Now()
	sawAwait := false
	deadline := time.Now().Add(40 * time.Second)
	for !cl.IsClosed() {
		require.True(t, time.Now().Before(deadline), "client did not close; state=%v", cl.state)
		cl.PollEgress()
		if cl.state == stateAwaitNewLeader {
			sawAwait = true
		}
		time.Sleep(time.Millisecond)
	}
	t.Logf("closed after %v", time.Since(start))
	require.True(t, sawAwait)
	require.Empty(t, listener.newLeaders)
	require.True(t, cl.IngressPublication().IsClosed())
	require.Zero(t, cl.PollEgress())
}

func awaitClusterUsable(t *testing.T, nodes int) {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		listener := &recordingListener{}
		ctx := e2eContext(nodes, listener)
		ctx.MessageTimeout = 3 * time.Second
		cl, err := Connect(ctx)
		if err == nil {
			_ = cl.Close()
			return
		}
		time.Sleep(time.Second)
	}
	t.Logf("cluster did not become usable again")
}
