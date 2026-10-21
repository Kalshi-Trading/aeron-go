package client

import (
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/require"
)

// E2E tests against a Java 1.53.3 cluster started from docker/cluster.compose.yml. See docker/README.md.
//
//	AERON_E2E=1 go test ./cluster/client -run E2E -v                          # 1 node
//	AERON_E2E=1 CLUSTER_E2E_NODES=3 go test ./cluster/client -run E2E -v      # 3 nodes incl. failover

type recordingListener struct {
	messages       [][]byte
	sessionEvents  []codecs.EventCodeEnum
	newLeaders     []int32
	adminResponses []codecs.AdminResponseCodeEnum
	adminCorrIds   []int64
	adminMessages  []string
}

func (l *recordingListener) OnMessage(_ int64, _ int64, buffer *atomic.Buffer, offset int32, length int32,
	_ *logbuffer.Header) {
	l.messages = append(l.messages, buffer.GetBytesArray(offset, length))
}

func (l *recordingListener) OnSessionEvent(_ int64, _ int64, _ int64, _ int32, code codecs.EventCodeEnum,
	_ string) {
	l.sessionEvents = append(l.sessionEvents, code)
}

func (l *recordingListener) OnNewLeader(_ int64, _ int64, leaderMemberId int32, _ string) {
	l.newLeaders = append(l.newLeaders, leaderMemberId)
}

func (l *recordingListener) OnAdminResponse(_ int64, correlationId int64, _ codecs.AdminRequestTypeEnum,
	code codecs.AdminResponseCodeEnum, message string, _ *atomic.Buffer, _ int32, _ int32) {
	l.adminResponses = append(l.adminResponses, code)
	l.adminCorrIds = append(l.adminCorrIds, correlationId)
	l.adminMessages = append(l.adminMessages, message)
}

type controlledRecordingListener struct {
	recordingListener
	aborted bool
}

func (l *controlledRecordingListener) OnMessage(s int64, ts int64, buffer *atomic.Buffer, offset int32,
	length int32, h *logbuffer.Header) term.ControlledPollAction {
	if !l.aborted {
		l.aborted = true
		return term.ControlledPollActionAbort
	}
	l.recordingListener.OnMessage(s, ts, buffer, offset, length, h)
	return term.ControlledPollActionCommit
}

func e2eNodes(t *testing.T) int {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 and start docker/cluster.compose.yml")
	}
	n, _ := strconv.Atoi(os.Getenv("CLUSTER_E2E_NODES"))
	if n == 0 {
		n = 1
	}
	return n
}

func e2eContext(nodes int, listener EgressListener) *Context {
	ctx := NewContext()
	ctx.AeronDirectoryName = os.Getenv("AERON_DIR")
	if ctx.AeronDirectoryName == "" {
		ctx.AeronDirectoryName = "/dev/shm/aeron-cluster-client"
	}
	ctx.IngressChannel = "aeron:udp?term-length=64k"
	ctx.IngressEndpoints = "0=localhost:9002"
	if nodes == 3 {
		ctx.IngressEndpoints = "0=localhost:9002,1=localhost:9102,2=localhost:9202"
	}
	ctx.EgressChannel = "aeron:udp?endpoint=localhost:0"
	ctx.MessageTimeout = 10 * time.Second
	ctx.EgressListener = listener
	return ctx
}

func pollUntil(t *testing.T, cl *AeronCluster, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		require.True(t, time.Now().Before(deadline), "timed out")
		if cl.PollEgress() == 0 {
			time.Sleep(time.Millisecond)
		}
	}
}

func offerAll(t *testing.T, cl *AeronCluster, msg []byte) {
	t.Helper()
	buf := atomic.MakeBuffer(msg)
	deadline := time.Now().Add(10 * time.Second)
	for cl.Offer(buf, 0, int32(len(msg))) < 0 {
		require.True(t, time.Now().Before(deadline), "offer timed out")
		cl.PollEgress()
		time.Sleep(time.Millisecond)
	}
}

func TestE2EEchoKeepAliveAdminSnapshot(t *testing.T) {
	nodes := e2eNodes(t)
	listener := &recordingListener{}
	cl, err := Connect(e2eContext(nodes, listener))
	require.NoError(t, err)
	defer cl.Close()
	t.Logf("connected sessionId=%d leader=%d term=%d newLeaderTimeout=%v", cl.ClusterSessionId(),
		cl.LeaderMemberId(), cl.LeadershipTermId(), cl.Context().NewLeaderTimeout)
	require.True(t, cl.IsConnected())

	const count = 100
	for i := 0; i < count; i++ {
		offerAll(t, cl, []byte("echo-"+strconv.Itoa(i)))
	}
	// TryClaim path.
	var claim logbuffer.Claim
	payload := []byte("claimed-message")
	for cl.TryClaim(int32(len(payload)), &claim) < 0 {
		cl.PollEgress()
	}
	claim.Buffer().PutBytesArray(claim.Offset()+SessionHeaderLength, &payload, 0, int32(len(payload)))
	claim.Commit()
	// Large fragmented message exercises the fragment assembler.
	large := make([]byte, 6000)
	for i := range large {
		large[i] = byte(i)
	}
	offerAll(t, cl, large)

	pollUntil(t, cl, 10*time.Second, func() bool { return len(listener.messages) == count+2 })
	for i := 0; i < count; i++ {
		require.Equal(t, "echo-"+strconv.Itoa(i), string(listener.messages[i]))
	}
	require.Equal(t, "claimed-message", string(listener.messages[count]))
	require.Equal(t, large, listener.messages[count+1])

	ok, err := cl.SendKeepAlive()
	require.NoError(t, err)
	require.True(t, ok)

	corrId := cl.Context().Aeron.NextCorrelationID()
	ok, err = cl.SendAdminRequestToTakeASnapshot(corrId)
	require.NoError(t, err)
	require.True(t, ok)
	pollUntil(t, cl, 10*time.Second, func() bool { return len(listener.adminResponses) == 1 })
	require.Equal(t, corrId, listener.adminCorrIds[0])
	require.Equal(t, codecs.AdminResponseCode.OK, listener.adminResponses[0], listener.adminMessages[0])

	require.NoError(t, cl.Close())
	require.True(t, cl.IsClosed())
	require.NoError(t, cl.Close())
}

func TestE2EAsyncConnectAndControlledPoll(t *testing.T) {
	nodes := e2eNodes(t)
	listener := &controlledRecordingListener{}
	ctx := e2eContext(nodes, nil)
	ctx.ControlledEgressListener = listener
	async, err := NewAsyncConnect(ctx)
	require.NoError(t, err)
	var cl *AeronCluster
	var steps []AsyncConnectState
	for cl == nil {
		if len(steps) == 0 || steps[len(steps)-1] != async.State() {
			steps = append(steps, async.State())
		}
		cl, err = async.Poll()
		require.NoError(t, err)
		time.Sleep(100 * time.Microsecond)
	}
	defer cl.Close()
	t.Logf("async connect steps: %v", steps)
	require.Equal(t, AsyncConnectDone, async.State())

	offerAll(t, cl, []byte("controlled"))
	deadline := time.Now().Add(10 * time.Second)
	for len(listener.messages) == 0 {
		require.True(t, time.Now().Before(deadline))
		cl.ControlledPollEgress()
	}
	require.True(t, listener.aborted)
	require.Equal(t, "controlled", string(listener.messages[0]))
}

func TestE2EIngressSessionDecoratorAndEgressAdapter(t *testing.T) {
	nodes := e2eNodes(t)
	cl, err := Connect(e2eContext(nodes, &recordingListener{}))
	require.NoError(t, err)
	defer cl.Close()

	listener := &recordingListener{}
	adapter := NewEgressAdapter(listener, nil, cl.ClusterSessionId(), cl.EgressSubscription(), 10)
	decorator := NewIngressSessionDecorator(cl.ClusterSessionId(), cl.LeadershipTermId())
	msg := []byte("decorated")
	buf := atomic.MakeBuffer(msg)
	deadline := time.Now().Add(10 * time.Second)
	for decorator.Offer(cl.IngressPublication(), buf, 0, int32(len(msg))) < 0 {
		require.True(t, time.Now().Before(deadline))
	}
	for len(listener.messages) == 0 {
		require.True(t, time.Now().Before(deadline))
		adapter.Poll()
	}
	require.Equal(t, "decorated", string(listener.messages[0]))
}

func TestE2ELeaderFailover(t *testing.T) {
	nodes := e2eNodes(t)
	if nodes != 3 {
		t.Skip("requires CLUSTER_E2E_NODES=3")
	}
	listener := &recordingListener{}
	cl, err := Connect(e2eContext(nodes, listener))
	require.NoError(t, err)
	defer cl.Close()

	offerAll(t, cl, []byte("before"))
	pollUntil(t, cl, 10*time.Second, func() bool { return len(listener.messages) == 1 })

	oldLeader := cl.LeaderMemberId()
	oldTerm := cl.LeadershipTermId()
	t.Logf("killing leader node%d (term %d)", oldLeader, oldTerm)
	composeFile := os.Getenv("CLUSTER_COMPOSE_FILE")
	if composeFile == "" {
		composeFile = "../../docker/cluster.compose.yml"
	}
	out, err := exec.Command("docker", "compose", "-f", composeFile, "--profile", "three", "kill",
		"node"+strconv.Itoa(int(oldLeader))).CombinedOutput()
	require.NoError(t, err, string(out))
	defer func() {
		out, err := exec.Command("docker", "compose", "-f", composeFile, "--profile", "three", "up", "-d",
			"node"+strconv.Itoa(int(oldLeader))).CombinedOutput()
		if err != nil {
			t.Logf("restart failed: %v %s", err, out)
		}
	}()

	start := time.Now()
	pollUntil(t, cl, 30*time.Second, func() bool { return len(listener.newLeaders) > 0 })
	t.Logf("new leader %d term %d after %v", cl.LeaderMemberId(), cl.LeadershipTermId(), time.Since(start))
	require.NotEqual(t, oldLeader, cl.LeaderMemberId())
	require.Greater(t, cl.LeadershipTermId(), oldTerm)
	require.False(t, cl.IsClosed())

	offerAll(t, cl, []byte("after"))
	pollUntil(t, cl, 10*time.Second, func() bool { return len(listener.messages) == 2 })
	require.Equal(t, "after", string(listener.messages[1]))
	require.True(t, cl.IsConnected())

	corrId := cl.Context().Aeron.NextCorrelationID()
	ok, err := cl.SendAdminRequestToTakeASnapshot(corrId)
	require.NoError(t, err)
	require.True(t, ok)
	pollUntil(t, cl, 10*time.Second, func() bool { return len(listener.adminResponses) == 1 })
	require.Equal(t, codecs.AdminResponseCode.OK, listener.adminResponses[0])
}
