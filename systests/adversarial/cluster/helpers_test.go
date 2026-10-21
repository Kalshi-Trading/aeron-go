package cluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/e2e"
	"github.com/lirm/aeron-go/systests/adversarial/cluster/internal/advproto"
	"github.com/stretchr/testify/require"
)

const (
	composeFile = "../../../docker/adversarial-cluster.compose.yml"
	clientDir   = "/dev/shm/aeron-adv-cluster"
	endpoints3  = "0=172.30.76.10:27600,1=172.30.76.11:27610,2=172.30.76.12:27620"
	endpoints1  = "0=172.30.76.30:27630"
	netemImage  = "aeron-adv-cluster-netem"
	// sessionTimeout and the leader heartbeat timeout as configured in the compose file.
	sessionTimeout = 5 * time.Second
	// newLeaderTimeout is the client's derived timeout: 2 x the cluster's 2s leader heartbeat timeout.
	newLeaderTimeout = 4 * time.Second
	clientLiveness   = 5 * time.Second
)

var (
	binsOnce sync.Once
	binsDir  string
	binsErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if binsDir != "" {
		_ = os.RemoveAll(binsDir)
	}
	os.Exit(code)
}

func requireAdversarial(t *testing.T) {
	t.Helper()
	if !e2e.Enabled() || os.Getenv("AERON_ADVERSARIAL") != "1" {
		t.Skip("set AERON_E2E=1 AERON_ADVERSARIAL=1 (kills/partitions the docker/adversarial-cluster.compose.yml stack)")
	}
	binsOnce.Do(func() {
		binsDir, binsErr = os.MkdirTemp("", "adv-cluster-bin")
		if binsErr != nil {
			return
		}
		for _, b := range []string{"advsvc", "advclient"} {
			out, err := exec.Command("go", "build", "-o", filepath.Join(binsDir, b), "./internal/"+b).CombinedOutput()
			if err != nil {
				binsErr = fmt.Errorf("go build %s: %v\n%s", b, err, out)
				return
			}
		}
	})
	require.NoError(t, binsErr)
}

func dataRoot() string {
	if d := os.Getenv("ADV_CLUSTER_DATA"); d != "" {
		return d
	}
	return filepath.Join(os.Getenv("HOME"), "adv-cluster-data")
}

func run(t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := runErr(name, args...)
	require.NoError(t, err, "%s %v: %s", name, args, out)
	return out
}

func runErr(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Env = append(os.Environ(), "ADV_CLUSTER_DATA="+dataRoot())
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func compose(t *testing.T, args ...string) string {
	t.Helper()
	return run(t, "docker", append([]string{"compose", "-f", composeFile, "--profile", "one"}, args...)...)
}

// proc is a helper child process whose stdout lines are captured.
type proc struct {
	name  string
	cmd   *exec.Cmd
	mu    sync.Mutex
	lines []string
	done  chan struct{}
	err   error
}

func startProc(t *testing.T, name string, bin string, args ...string) *proc {
	t.Helper()
	p := &proc{name: name, cmd: exec.Command(filepath.Join(binsDir, bin), args...), done: make(chan struct{})}
	out, err := p.cmd.StdoutPipe()
	require.NoError(t, err)
	p.cmd.Stderr = p.cmd.Stdout
	require.NoError(t, p.cmd.Start())
	go func() {
		sc := bufio.NewScanner(out)
		sc.Buffer(make([]byte, 64*1024), 1<<20)
		for sc.Scan() {
			p.mu.Lock()
			p.lines = append(p.lines, sc.Text())
			p.mu.Unlock()
		}
		p.err = p.cmd.Wait()
		close(p.done)
	}()
	return p
}

func (p *proc) find(substr string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, l := range p.lines {
		if strings.Contains(l, substr) {
			return l, true
		}
	}
	return "", false
}

func (p *proc) count(substr string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, l := range p.lines {
		if strings.Contains(l, substr) {
			n++
		}
	}
	return n
}

func (p *proc) output() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return strings.Join(p.lines, "\n")
}

// waitLine waits for an output line containing substr, calling the poll funcs (e.g. to keep sessions alive).
func (p *proc) waitLine(t *testing.T, substr string, timeout time.Duration, polls ...func()) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if l, ok := p.find(substr); ok {
			return l
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: no %q within %v; output:\n%s", p.name, substr, timeout, p.output())
		}
		for _, poll := range polls {
			poll()
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (p *proc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *proc) waitExit(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(timeout):
		t.Fatalf("%s did not exit within %v; output:\n%s", p.name, timeout, p.output())
	}
}

func (p *proc) kill() {
	if !p.exited() {
		_ = p.cmd.Process.Signal(syscall.SIGKILL)
	}
	<-p.done
}

// member is one cluster member: its Java container and the Go service process.
type member struct {
	id        int
	name      string
	container string
	svc       *proc
	starts    int
}

func (m *member) aeronDir() string { return "/dev/shm/aeron-adv-cluster-" + m.name }

// stack is one test's cluster.
type stack struct {
	t         *testing.T
	data      string
	members   []*member
	endpoints string
	egress    int
	dumpID    int64
}

func newStack(t *testing.T) *stack     { return startStack(t, false) }
func newSoloStack(t *testing.T) *stack { return startStack(t, true) }

func startStack(t *testing.T, solo bool) *stack {
	t.Helper()
	requireAdversarial(t)
	s := &stack{t: t, data: dataRoot()}
	compose(t, "down", "-t", "1", "--remove-orphans")
	require.NoError(t, os.RemoveAll(s.data))
	services := []string{"client-driver"}
	if solo {
		s.members = []*member{{id: 0, name: "solo", container: "adv-cluster-solo"}}
		s.endpoints = endpoints1
	} else {
		for i := 0; i < 3; i++ {
			s.members = append(s.members, &member{id: i, name: fmt.Sprintf("n%d", i),
				container: fmt.Sprintf("adv-cluster-node%d", i)})
		}
		s.endpoints = endpoints3
	}
	for _, m := range s.members {
		dir := strings.TrimPrefix(m.container, "adv-cluster-")
		require.NoError(t, os.MkdirAll(filepath.Join(s.data, dir), 0o755))
		require.NoError(t, os.MkdirAll(filepath.Join(s.data, "svc-"+m.name), 0o755))
		_ = os.RemoveAll(m.aeronDir())
		services = append(services, dir)
	}
	require.NoError(t, os.MkdirAll(filepath.Join(s.data, "dumps"), 0o755))
	_ = os.RemoveAll(clientDir)
	t.Cleanup(s.teardown)
	compose(t, append([]string{"up", "-d", "--wait"}, services...)...)
	for _, m := range s.members {
		s.startService(m)
	}
	return s
}

func (s *stack) teardown() {
	t := s.t
	for _, m := range s.members {
		if m.svc != nil {
			m.svc.kill()
			if t.Failed() {
				t.Logf("=== service %s output:\n%s", m.name, m.svc.output())
			}
		}
		if t.Failed() {
			out, _ := runErr("docker", "logs", "--tail", "60", m.container)
			t.Logf("=== %s logs:\n%s", m.container, out)
		}
		_, _ = runErr("docker", "kill", "-s", "CONT", m.container)
	}
	if out, err := runErr("docker", "compose", "-f", composeFile, "--profile", "one", "down", "-t", "1"); err != nil {
		t.Logf("compose down: %v %s", err, out)
	}
}

func (s *stack) startService(m *member) {
	t := s.t
	t.Helper()
	waitFor(t, 60*time.Second, "cnc.dat of "+m.name, func() bool {
		_, err := os.Stat(filepath.Join(m.aeronDir(), "cnc.dat"))
		return err == nil
	})
	m.starts++
	m.svc = startProc(t, fmt.Sprintf("advsvc-%s#%d", m.name, m.starts), "advsvc",
		"-aeron-dir", m.aeronDir(), "-cluster-dir", filepath.Join(s.data, "svc-"+m.name),
		"-state-dir", filepath.Join(s.data, "dumps"), "-node", fmt.Sprint(m.id))
}

// kill SIGKILLs the member container and its service process.
func (s *stack) kill(m *member) {
	s.t.Helper()
	run(s.t, "docker", "kill", "-s", "KILL", m.container)
	m.svc.kill()
}

// restart starts a killed member from its persisted cluster/archive state and a new service process.
func (s *stack) restart(m *member) {
	s.t.Helper()
	if !m.svc.exited() {
		m.svc.kill()
	}
	_, _ = runErr("docker", "kill", "-s", "KILL", m.container)
	require.NoError(s.t, os.RemoveAll(m.aeronDir()))
	run(s.t, "docker", "start", m.container)
	s.startService(m)
}

func (s *stack) signal(m *member, sig string) { run(s.t, "docker", "kill", "-s", sig, m.container) }

// netns runs a command in the member's network namespace via the netem sidecar.
func (s *stack) netns(m *member, args ...string) {
	s.t.Helper()
	run(s.t, "docker", append([]string{"run", "--rm", "--net", "container:" + m.container, "--cap-add", "NET_ADMIN",
		netemImage}, args...)...)
}

func (s *stack) partition(m *member) {
	s.netns(m, "sh", "-c", "iptables -A INPUT -i eth0 -j DROP && iptables -A OUTPUT -o eth0 -j DROP")
}

func (s *stack) heal(m *member) { s.netns(m, "sh", "-c", "iptables -F INPUT && iptables -F OUTPUT") }

func (s *stack) member(id int32) *member {
	for _, m := range s.members {
		if int32(m.id) == id {
			return m
		}
	}
	s.t.Fatalf("no member %d", id)
	return nil
}

func (s *stack) others(m *member) []*member {
	var out []*member
	for _, o := range s.members {
		if o != m {
			out = append(out, o)
		}
	}
	return out
}

type senderOpts struct {
	controlled bool
	exclusive  bool
	claim      bool
	timeout    time.Duration
}

// client is an in-process cluster client used to drive traffic.
type client struct {
	*advproto.Sender
	s       *stack
	o       senderOpts
	next    int64
	offered advproto.Ranges
	results map[int64]int
	lastKA  time.Time
}

// idle polls and keeps the session alive (Java clients must send keep-alives within the session timeout).
func (c *client) idle() {
	c.Poll(c.o.controlled)
	if !c.AC.IsClosed() && time.Since(c.lastKA) > 500*time.Millisecond {
		c.lastKA = time.Now()
		_, _ = c.AC.SendKeepAlive()
	}
}

func (s *stack) egressChannel() string {
	s.egress++
	return fmt.Sprintf("aeron:udp?endpoint=172.30.76.20:%d", 27650+s.egress%50)
}

func (s *stack) dial(tag int64, o senderOpts) (*client, error) {
	snd, err := advproto.NewSender(advproto.Options{AeronDir: clientDir, Endpoints: s.endpoints,
		Egress: s.egressChannel(), Tag: tag, Controlled: o.controlled, Exclusive: o.exclusive, MessageTimeout: o.timeout})
	if err != nil {
		return nil, err
	}
	return &client{Sender: snd, s: s, o: o, next: 1, results: map[int64]int{}}, nil
}

// connect retries until the cluster has elected a leader and accepted the session.
func (s *stack) connect(tag int64, o senderOpts) *client {
	s.t.Helper()
	var lastErr error
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		c, err := s.dial(tag, o)
		if err == nil {
			s.t.Cleanup(func() { _ = c.AC.Close() })
			return c
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	s.t.Fatalf("connect tag %d: %v", tag, lastErr)
	return nil
}

func payloadSize(seq int64) int {
	switch {
	case seq%50 == 0:
		return 200 * 1024 // fragmented across many frames
	case seq%7 == 0:
		return 3000
	default:
		return int(seq % 500)
	}
}

// step polls and offers the next message, returning the publication result (0 if nothing was offered).
func (c *client) step() int64 {
	c.Poll(c.o.controlled)
	if c.AC.IsClosed() {
		return aeron.PublicationClosed
	}
	size := payloadSize(c.next)
	claim := c.o.claim && size <= 1000
	r := c.Offer(advproto.KindData, c.next, size, claim)
	if r > 0 {
		c.offered.Add(c.next)
		c.next++
	} else {
		c.results[r]++
	}
	return r
}

// sendAcked sends n messages and waits until all of them are echoed.
func (c *client) sendAcked(t *testing.T, n int64, timeout time.Duration) {
	t.Helper()
	target := c.next + n
	deadline := time.Now().Add(timeout)
	for c.next < target {
		if c.step() <= 0 {
			time.Sleep(100 * time.Microsecond)
		}
		require.False(t, time.Now().After(deadline), "offer of %d messages timed out at seq %d", n, c.next)
		require.False(t, c.AC.IsClosed(), "client closed while sending")
	}
	c.awaitAcked(t, target-1, timeout)
}

func (c *client) awaitAcked(t *testing.T, upTo int64, timeout time.Duration) {
	t.Helper()
	waitForPoll(t, timeout, fmt.Sprintf("acks up to %d (tag %d)", upTo, c.Tag), func() bool {
		a := c.Acked()
		return len(a) > 0 && a[len(a)-1][1] >= upTo
	}, func() { c.Poll(c.o.controlled) })
}

// dump appends a KindDump message and returns every live member's dump, asserting they are identical.
func (s *stack) dump(t *testing.T, c *client, live []*member) *advproto.Dump {
	t.Helper()
	s.dumpID++
	id := s.dumpID
	waitForPoll(t, 30*time.Second, "dump offer", func() bool {
		return c.Offer(advproto.KindDump, id, 0, false) > 0
	}, func() { c.Poll(c.o.controlled) })
	waitForPoll(t, 30*time.Second, "dump echo", func() bool { return c.DumpAcked(id) }, func() { c.Poll(c.o.controlled) })
	var first *advproto.Dump
	for _, m := range live {
		path := advproto.DumpPath(filepath.Join(s.data, "dumps"), int32(m.id), id)
		var d *advproto.Dump
		waitFor(t, 60*time.Second, "dump from "+m.name, func() bool {
			var err error
			d, err = advproto.ReadDump(path)
			return err == nil
		})
		if first == nil {
			first = d
			continue
		}
		require.Equal(t, first.LogPosition, d.LogPosition, "dump log position differs: %s", m.name)
		if !reflect.DeepEqual(first.State, d.State) {
			a, _ := json.Marshal(first.State)
			b, _ := json.Marshal(d.State)
			t.Fatalf("service state diverged on %s at position %d:\n%s\n%s", m.name, d.LogPosition, a, b)
		}
	}
	require.Zero(t, first.State.Dups, "duplicate committed messages")
	require.Zero(t, first.State.Corrupt, "corrupt committed messages")
	return first
}

// verify asserts every acknowledged message was applied exactly once and nothing that was not offered was applied.
func verify(t *testing.T, d *advproto.Dump, clients ...*client) {
	t.Helper()
	for _, c := range clients {
		applied := d.State.Applied[c.Tag]
		for _, r := range c.Acked() {
			for v := r[0]; v <= r[1]; v++ {
				require.True(t, applied.Contains(v), "acked seq %d (tag %d) lost", v, c.Tag)
			}
		}
		for _, r := range applied {
			for v := r[0]; v <= r[1]; v++ {
				require.True(t, c.offered.Contains(v), "seq %d (tag %d) applied but never offered", v, c.Tag)
			}
		}
		require.Empty(t, c.BadEchoes(), "tag %d", c.Tag)
	}
}

// resources is a snapshot of process resources for leak checks.
type resources struct {
	goroutines int
	maps       int
	fds        int
	counters   int
}

func countCounters(r *counters.Reader) int {
	n := 0
	r.Scan(func(counters.Counter) { n++ })
	return n
}

func sample(t *testing.T, observer *aeron.Aeron) resources {
	t.Helper()
	maps, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	fds, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return resources{goroutines: runtime.NumGoroutine(), maps: strings.Count(string(maps), clientDir+"/"),
		fds: len(fds), counters: countCounters(observer.CounterReader())}
}

// leakCheck returns a func asserting that resources returned to the baseline taken now. The observer client is
// used to read the client driver's counters.
func leakCheck(t *testing.T) func() {
	t.Helper()
	observer, err := aeron.Connect(aeron.NewContext().AeronDir(clientDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = observer.Close() })
	base := sample(t, observer)
	return func() {
		t.Helper()
		var now resources
		ok := waitUntil(20*time.Second, func() bool {
			now = sample(t, observer)
			return now.goroutines <= base.goroutines && now.maps <= base.maps && now.fds <= base.fds &&
				now.counters <= base.counters
		})
		if !ok {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("leak: before=%+v after=%+v\n%s", base, now, buf[:n])
		}
	}
}

func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
	return true
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	if !waitUntil(timeout, cond) {
		t.Fatalf("timed out after %v waiting for %s", timeout, what)
	}
}

func waitForPoll(t *testing.T, timeout time.Duration, what string, cond func() bool, poll func()) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		poll()
		time.Sleep(time.Millisecond)
	}
}
