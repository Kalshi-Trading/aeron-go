package cluster_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster"
	"github.com/lirm/aeron-go/cluster/client"
	"github.com/lirm/aeron-go/cluster/codecs"
	"go.uber.org/zap/zapcore"
)

// E2E against the Java 1.53.3 ClusteredMediaDriver from docker/cluster-service.compose.yml:
//
//	docker compose -f docker/cluster-service.compose.yml up -d --build cluster-node
//	AERON_E2E=1 go test ./cluster -run TestE2E -v
//
// The Go service keeps a running total of client messages, takes a snapshot (triggered via Java ClusterTool), the
// cluster node is restarted and the state is recovered from the snapshot plus log replay.

const (
	e2eComposeFile = "../docker/cluster-service.compose.yml"
	e2eService     = "cluster-node"
)

func e2eAeronDir() string {
	if d := os.Getenv("AERON_CLUSTER_E2E_DIR"); d != "" {
		return d
	}
	return "/dev/shm/aeron-cluster-svc"
}

type totalService struct {
	mu              sync.Mutex
	cluster         cluster.Cluster
	total           int64
	loadedSnapshot  bool
	snapshotsTaken  int
	sessionsOpened  int
	roles           []cluster.Role
	leadershipTerms int
	timerFired      chan int64
}

func (s *totalService) OnStart(c cluster.Cluster, image aeron.Image) {
	s.cluster = c
	if image == nil {
		return
	}
	done := false
	for !done {
		image.Poll(func(buf *atomic.Buffer, offset, length int32, _ *logbuffer.Header) {
			s.total = buf.GetInt64(offset)
			done = true
		}, 1)
		if !done {
			if image.IsClosed() {
				panic("snapshot image closed before state read")
			}
			c.IdleStrategy().Idle(0)
		}
	}
	s.mu.Lock()
	s.loadedSnapshot = true
	s.mu.Unlock()
}

func (s *totalService) OnSessionOpen(session cluster.ClientSession, _ int64) {
	s.mu.Lock()
	s.sessionsOpened++
	s.mu.Unlock()
}

func (s *totalService) OnSessionClose(cluster.ClientSession, int64, codecs.CloseReasonEnum) {}

func (s *totalService) OnSessionMessage(session cluster.ClientSession, timestamp int64, buffer *atomic.Buffer,
	offset, length int32, _ *logbuffer.Header) {
	v := buffer.GetInt64(offset)
	s.mu.Lock()
	s.total += v
	total := s.total
	s.mu.Unlock()
	if v == 1000 {
		// exercise timers: fire one 1 unit of time later
		s.cluster.ScheduleTimer(42, timestamp+1)
	}
	if session == nil {
		return
	}
	reply := atomic.MakeBuffer(make([]byte, 8))
	reply.PutInt64(0, total)
	var claim logbuffer.Claim
	for i := 0; i < 1000; i++ {
		r := session.TryClaim(8, &claim)
		if r > 0 {
			claim.Buffer().PutInt64(claim.Offset()+cluster.SBEHeaderLength+cluster.SessionMessageHeaderLength, total)
			claim.Commit()
			return
		}
		if r == cluster.ClientSessionMockedOffer {
			return
		}
		s.cluster.IdleStrategy().Idle(0)
	}
}

func (s *totalService) OnTimerEvent(correlationId, _ int64) {
	if s.timerFired != nil {
		select {
		case s.timerFired <- correlationId:
		default:
		}
	}
}

func (s *totalService) OnTakeSnapshot(publication *aeron.ExclusivePublication) {
	buf := atomic.MakeBuffer(make([]byte, 8))
	s.mu.Lock()
	buf.PutInt64(0, s.total)
	s.mu.Unlock()
	for publication.Offer(buf, 0, 8, nil) < 0 {
		s.cluster.IdleStrategy().Idle(0)
	}
	s.mu.Lock()
	s.snapshotsTaken++
	s.mu.Unlock()
}

func (s *totalService) OnRoleChange(role cluster.Role) {
	s.mu.Lock()
	s.roles = append(s.roles, role)
	s.mu.Unlock()
}

func (s *totalService) OnTerminate(cluster.Cluster) {}

func (s *totalService) OnNewLeadershipTermEvent(int64, int64, int64, int64, int32, int32,
	codecs.ClusterTimeUnitEnum, int32) {
	s.mu.Lock()
	s.leadershipTerms++
	s.mu.Unlock()
}

func (s *totalService) get(f func(*totalService) interface{}) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return f(s)
}

type egress struct {
	client.NoOpEgressEvents
	mu   sync.Mutex
	last int64
	n    int
}

func (e *egress) OnMessage(_ int64, _ int64, buf *atomic.Buffer, offset, length int32, _ *logbuffer.Header) {
	if length >= 8 {
		e.mu.Lock()
		e.last = buf.GetInt64(offset)
		e.n++
		e.mu.Unlock()
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool, poll func()) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		if poll != nil {
			poll()
		}
		time.Sleep(time.Millisecond)
	}
}

func startAgent(t *testing.T, svc *totalService, clusterDir string) chan error {
	t.Helper()
	ctx := aeron.NewContext().AeronDir(e2eAeronDir())
	opts := cluster.NewOptions()
	opts.ClusterDir = clusterDir
	if os.Getenv("AERON_E2E_DEBUG") == "1" {
		opts.Loglevel = zapcore.DebugLevel
	}
	agent, err := cluster.NewClusteredServiceAgent(ctx, opts, svc)
	if err != nil {
		t.Fatalf("NewClusteredServiceAgent: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- agent.StartAndRun() }()
	return done
}

func connectClient(t *testing.T) (*client.AeronCluster, *egress) {
	t.Helper()
	l := &egress{}
	ctx := client.NewContext()
	ctx.AeronDirectoryName = e2eAeronDir()
	ctx.IngressChannel = "aeron:udp"
	ctx.IngressEndpoints = "0=localhost:20110"
	ctx.EgressChannel = "aeron:udp?endpoint=localhost:0"
	ctx.EgressListener = l
	ac, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	return ac, l
}

func send(t *testing.T, ac *client.AeronCluster, l *egress, v int64, expectTotal int64) {
	t.Helper()
	buf := atomic.MakeBuffer(make([]byte, 8))
	buf.PutInt64(0, v)
	waitFor(t, 10*time.Second, "ingress offer", func() bool { return ac.Offer(buf, 0, 8) > 0 }, func() { ac.PollEgress() })
	waitFor(t, 10*time.Second, "egress total", func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.last == expectTotal
	}, func() { ac.PollEgress() })
}

func compose(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose", "-f", e2eComposeFile}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
}

func clusterTool(t *testing.T, command string) string {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-f", e2eComposeFile, "exec", "-T", e2eService, "sh", "-c",
		"java $JAVA_OPTS -cp /opt/aeron/aeron-all.jar io.aeron.cluster.ClusterTool /data/cluster/cluster "+command)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ClusterTool %s: %v\n%s", command, err, out)
	}
	return string(out)
}

func awaitDriver(t *testing.T) {
	t.Helper()
	waitFor(t, 60*time.Second, "media driver cnc", func() bool {
		_, err := os.Stat(filepath.Join(e2eAeronDir(), "cnc.dat"))
		return err == nil
	}, func() { time.Sleep(100 * time.Millisecond) })
	time.Sleep(2 * time.Second)
}

func TestE2EClusteredServiceSnapshotAndRestart(t *testing.T) {
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 with docker/cluster-service.compose.yml running")
	}
	compose(t, "down")
	dataDir := os.Getenv("CLUSTER_SVC_DATA")
	if dataDir == "" {
		dataDir = "/tmp/aeron-cluster-svc-data"
	}
	_ = os.RemoveAll(dataDir)
	if err := os.MkdirAll(dataDir, 0o777); err != nil {
		t.Fatal(err)
	}
	compose(t, "up", "-d", e2eService)
	awaitDriver(t)
	clusterDir := t.TempDir()

	svc1 := &totalService{timerFired: make(chan int64, 1)}
	done1 := startAgent(t, svc1, clusterDir)

	ac, l := connectClient(t)
	var total int64
	for v := int64(1); v <= 5; v++ {
		total += v
		send(t, ac, l, v, total)
	}
	if role := svc1.get(func(s *totalService) interface{} { return s.cluster.Role() }); role != cluster.Leader {
		t.Fatalf("expected leader role, got %v", role)
	}
	if n := svc1.get(func(s *totalService) interface{} { return len(s.cluster.ClientSessions()) }); n != 1 {
		t.Fatalf("expected 1 client session, got %v", n)
	}

	total += 1000
	send(t, ac, l, 1000, total)
	select {
	case id := <-svc1.timerFired:
		if id != 42 {
			t.Fatalf("unexpected timer id %d", id)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timer did not fire")
	}

	t.Log(clusterTool(t, "snapshot"))
	waitFor(t, 20*time.Second, "service snapshot", func() bool {
		return svc1.get(func(s *totalService) interface{} { return s.snapshotsTaken }).(int) > 0
	}, nil)
	snapshotTotal := total

	// appended to the log after the snapshot, must be recovered by replay
	total += 7
	send(t, ac, l, 7, total)
	ac.Close()

	compose(t, "restart", "-t", "5", e2eService)
	select {
	case err := <-done1:
		t.Logf("first service agent exited: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("service agent did not exit after cluster node restart")
	}
	awaitDriver(t)

	svc2 := &totalService{}
	done2 := startAgent(t, svc2, clusterDir)
	waitFor(t, 30*time.Second, "snapshot load", func() bool {
		return svc2.get(func(s *totalService) interface{} { return s.loadedSnapshot }).(bool)
	}, nil)
	if got := svc2.get(func(s *totalService) interface{} { return s.total }); got.(int64) < snapshotTotal {
		t.Fatalf("snapshot state not restored: got %v want >= %d", got, snapshotTotal)
	}

	ac2, l2 := connectClient(t)
	total += 4
	send(t, ac2, l2, 4, total)
	ac2.Close()

	compose(t, "stop", "-t", "5", e2eService)
	select {
	case err := <-done2:
		t.Logf("second service agent exited: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("second service agent did not exit")
	}
}
