// Package networkfaults is an adversarial end-to-end suite for the Go client under network faults: packet loss
// (1% to 50%), reordering, duplication, delay/jitter, corruption, MTU-limited paths and full partitions (shorter and
// longer than the image liveness timeout) between Go and Java 1.53.3 publishers and subscribers, over UDP unicast,
// multicast, MDC (dynamic and manual) and multi-destination subscriptions, plus loss and partitions on the archive
// control channel and processes killed while the network is faulty.
//
// Faults are injected with tc netem on the host loopback from a NET_ADMIN sidecar (docker service nf-netem, host
// network): a prio qdisc on lo sends only UDP traffic to or from the suite's ports (27500-27599) through netem, so
// other traffic on the host is not affected. A partition is netem loss 100%. Two Java drivers (nf-driver-a with an
// archive, nf-driver-b) stand for two hosts, so a Go publisher on one and a Go subscriber on the other talk across
// the faulty path.
//
// Resources: compose project adv-network-faults (docker/adversarial-network-faults.compose.yml), AERON_DIR
// /dev/shm/aeron-adv-network-faults (driver a) and /dev/shm/aeron-adv-network-faults-b (driver b), UDP ports
// 27500-27599 (archive control 27590, recording events 27591), stream ids 9500-9599.
//
// Run (needs docker; the tests bring the stack up themselves and restart containers as needed):
//
//	make docker-mirror jar
//	docker compose -f docker/interop/docker-compose.yml --profile tools build
//	docker compose -f docker/adversarial-network-faults.compose.yml up -d --build --wait
//	AERON_E2E=1 AERON_ADVERSARIAL=1 go test -count=1 -p 1 -timeout 30m -v ./systests/adversarial/network-faults/
//	docker compose -f docker/adversarial-network-faults.compose.yml down
package networkfaults

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

const (
	dirA           = "/dev/shm/aeron-adv-network-faults"
	dirB           = "/dev/shm/aeron-adv-network-faults-b"
	driverA        = "adv-nf-driver-a"
	driverB        = "adv-nf-driver-b"
	netemContainer = "adv-nf-netem"
	composeFile    = "../../../docker/adversarial-network-faults.compose.yml"
	archiveControl = "aeron:udp?endpoint=localhost:27590"
	archiveCtlPort = 27590
	controlStream  = 10

	firstPort   = 27500
	lastPort    = 27589 // 27590-27599 belong to the archive
	firstStream = 9500
	lastStream  = 9599

	imageLiveness = 3 * time.Second // aeron.image.liveness.timeout in the compose file
	fcRcvTimeout  = 2 * time.Second // aeron.flow.control.receiver.timeout
)

var portSeq, streamSeq atomic.Int32

// port returns the next suite UDP port, cycling through 27500-27589 so -count=N runs don't reuse a port recently.
func port() int {
	return firstPort + int(portSeq.Add(1)-1)%(lastPort-firstPort+1)
}

func streamID() int32 {
	return firstStream + (streamSeq.Add(1)-1)%(lastStream-firstStream+1)
}

var stackOnce sync.Once
var stackErr error
var stackUp atomic.Bool
var stackLock *os.File

// TestMain tears the compose stack down after the run so the next adversarial suite starts from a clean host.
func TestMain(m *testing.M) {
	code := m.Run()
	if stackUp.Load() {
		_ = exec.Command("docker", "exec", netemContainer, "tc", "qdisc", "del", "dev", "lo", "root").Run()
		_ = compose("down", "--remove-orphans", "--timeout", "5")
	}
	os.Exit(code)
}

// requireStack skips unless AERON_E2E=1 AERON_ADVERSARIAL=1, brings the compose stack up, makes sure both drivers are
// running (an earlier test may have killed or frozen one) and clears any netem rules.
func requireStack(t *testing.T) {
	t.Helper()
	if !e2e.Enabled() || os.Getenv("AERON_ADVERSARIAL") != "1" {
		t.Skip("set AERON_E2E=1 AERON_ADVERSARIAL=1 (injects netem faults and kills docker/adversarial-network-faults.compose.yml containers)")
	}
	// Recreate the drivers once per test binary so no state (archive recordings, lingering channels) from an earlier
	// run leaks into this one.
	stackOnce.Do(func() {
		// Concurrent runs of this suite (two go test invocations) would fight over the same containers and qdiscs,
		// so a run holds an exclusive lock until it exits.
		if lock, err := os.OpenFile("/tmp/aeron-adv-network-faults.lock", os.O_CREATE|os.O_RDWR, 0o666); err == nil {
			_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX)
			stackLock = lock // kept reachable so the finalizer doesn't close it and drop the lock
		}
		if exec.Command("docker", "image", "inspect", interopImage).Run() != nil {
			out, err := exec.Command("docker", "compose", "-f", "../../../docker/interop/docker-compose.yml",
				"--profile", "tools", "build").CombinedOutput()
			if err != nil {
				stackErr = fmt.Errorf("building %s: %v: %s", interopImage, err, out)
				return
			}
		}
		stackErr = compose("up", "-d", "--build", "--wait", "--force-recreate")
		stackUp.Store(true)
	})
	require.NoError(t, stackErr)
	for _, c := range []string{driverA, driverB} {
		if paused(c) {
			_ = exec.Command("docker", "kill", "-s", "CONT", c).Run()
		}
	}
	if !running(driverA) || !running(driverB) {
		require.NoError(t, compose("up", "-d", "--wait", "nf-driver-a", "nf-driver-b"))
	}
	clearNetem(t)
	t.Cleanup(func() { clearNetem(t) })
}

func compose(args ...string) error {
	out, err := exec.Command("docker", append([]string{"compose", "-f", composeFile}, args...)...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose %v: %v: %s", args, err, out)
	}
	return nil
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	require.NoError(t, err, "docker %v: %s", args, out)
	return string(out)
}

func running(container string) bool {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", container).Output()
	return err == nil && strings.TrimSpace(string(out)) == "true"
}

func paused(container string) bool {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Status}}", container).Output()
	return err == nil && strings.TrimSpace(string(out)) == "paused"
}

// --------------------------------------------------------------------------------------------------------- netem

// netemRule sends UDP packets matching ports through a netem qdisc with spec (e.g. "loss 10%"). dport matches packets
// sent to a port (data to a receiver, SMs/NAKs to a publisher's control port), sport packets sent from it.
type netemRule struct {
	dports, sports []int
	spec           string
}

func toBoth(spec string, ports ...int) netemRule {
	return netemRule{dports: ports, sports: ports, spec: spec}
}
func toPorts(spec string, ports ...int) netemRule { return netemRule{dports: ports, spec: spec} }
func fromPorts(spec string, ports ...int) netemRule {
	return netemRule{sports: ports, spec: spec}
}

func tc(t *testing.T, args ...string) {
	t.Helper()
	docker(t, append([]string{"exec", netemContainer, "tc"}, args...)...)
}

func clearNetem(t *testing.T) {
	_ = exec.Command("docker", "exec", netemContainer, "tc", "qdisc", "del", "dev", "lo", "root").Run()
	_ = exec.Command("docker", "exec", netemContainer, "ip", "route", "del", "local", mtuLimitedAddr+"/32",
		"dev", "lo", "table", "local").Run()
}

// netem replaces the loopback fault rules. Each rule gets its own band (1:2, 1:3, ...) of a prio qdisc whose default
// band 1:1 carries all other traffic untouched.
func netem(t *testing.T, rules ...netemRule) {
	t.Helper()
	clearNetem(t)
	tc(t, "qdisc", "add", "dev", "lo", "root", "handle", "1:", "prio", "bands", "16",
		"priomap", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")
	for i, r := range rules {
		band := fmt.Sprintf("1:%d", i+2)
		tc(t, append([]string{"qdisc", "add", "dev", "lo", "parent", band, "handle", fmt.Sprintf("%d:", 10+i),
			"netem", "limit", "100000"}, strings.Fields(r.spec)...)...)
		add := func(dir string, p int) {
			tc(t, "filter", "add", "dev", "lo", "parent", "1:", "protocol", "ip", "prio", "1", "u32",
				"match", "ip", "protocol", "17", "0xff", "match", "ip", dir, fmt.Sprint(p), "0xffff", "flowid", band)
		}
		for _, p := range r.dports {
			add("dport", p)
		}
		for _, p := range r.sports {
			add("sport", p)
		}
	}
	t.Logf("netem: %+v", rules)
}

// netemStats returns the tc statistics of the loopback qdiscs (packets sent/dropped per rule).
func netemStats(t *testing.T) string {
	out, _ := exec.Command("docker", "exec", netemContainer, "tc", "-s", "qdisc", "show", "dev", "lo").CombinedOutput()
	return string(out)
}

const mtuLimitedAddr = "127.0.0.2"

// limitMTU routes mtuLimitedAddr over a path with the given MTU, so larger datagrams are IP fragmented.
func limitMTU(t *testing.T, mtu int) {
	t.Helper()
	docker(t, "exec", netemContainer, "ip", "route", "add", "local", mtuLimitedAddr+"/32", "dev", "lo",
		"table", "local", "mtu", "lock", fmt.Sprint(mtu))
}

// ---------------------------------------------------------------------------------------------------- go clients

type client struct {
	*aeron.Aeron
	errors      *errorLog
	available   chan aeron.Image
	unavailable chan aeron.Image
}

type errorLog struct {
	mu   sync.Mutex
	errs []error
}

func (l *errorLog) add(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errs = append(l.errs, err)
}

func (l *errorLog) all() []error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]error(nil), l.errs...)
}

// connect connects a Go client to the driver in dir, recording errors and image events.
func connect(t *testing.T, dir string) *client {
	t.Helper()
	c := &client{errors: &errorLog{}, available: make(chan aeron.Image, 64), unavailable: make(chan aeron.Image, 64)}
	ctx := aeron.NewContext().AeronDir(dir).MediaDriverTimeout(10 * time.Second).
		ErrorHandler(c.errors.add).
		AvailableImageHandler(func(img aeron.Image) { nonBlockingSend(c.available, img) }).
		UnavailableImageHandler(func(img aeron.Image) { nonBlockingSend(c.unavailable, img) })
	var err error
	deadline := time.Now().Add(20 * time.Second)
	for {
		c.Aeron, err = aeron.Connect(ctx)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func nonBlockingSend(ch chan aeron.Image, img aeron.Image) {
	select {
	case ch <- img:
	default:
	}
}

func awaitImage(t *testing.T, ch chan aeron.Image, what string, timeout time.Duration) aeron.Image {
	t.Helper()
	select {
	case img := <-ch:
		return img
	case <-time.After(timeout):
		t.Fatalf("no %s image event after %v", what, timeout)
		return nil
	}
}

func noImageEvent(t *testing.T, ch chan aeron.Image, what string) {
	t.Helper()
	select {
	case img := <-ch:
		t.Fatalf("unexpected %s image event: session %d correlation %d", what, img.SessionID(), img.CorrelationID())
	default:
	}
}

func awaitTrue(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// systemCounter returns the value of the driver system counter whose label starts with label (e.g. "NAKs sent").
func systemCounter(c *client, label string) int64 {
	v := int64(-1)
	c.CounterReader().Scan(func(ctr counters.Counter) {
		if ctr.TypeId == 0 && strings.HasPrefix(ctr.Label, label) {
			v = ctr.Value
		}
	})
	return v
}

// channelCounters returns the labels of driver counters mentioning tag (use a unique |alias=tag on the channel).
func channelCounters(c *client, tag string) []string {
	var labels []string
	c.CounterReader().Scan(func(ctr counters.Counter) {
		if strings.Contains(ctr.Label, tag) {
			labels = append(labels, fmt.Sprintf("%d:%s", ctr.TypeId, ctr.Label))
		}
	})
	return labels
}

// awaitNoChannelCounters waits for the driver behind observer to free every counter of channels tagged tag.
func awaitNoChannelCounters(t *testing.T, observer *client, tag string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		labels := channelCounters(observer, tag)
		if len(labels) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("driver counters for %s not freed after %v: %v", tag, timeout, labels)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ------------------------------------------------------------------------------------------------------- leaks

type resources struct {
	goroutines int
	fds        int
	maps       int
}

func snapshot(t *testing.T) resources {
	t.Helper()
	return resources{goroutines: runtime.NumGoroutine(), fds: openFds(t), maps: mappedSuiteFiles(t)}
}

func openFds(t *testing.T) int {
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(entries)
}

// mappedSuiteFiles counts this process's mappings of files under the suite's aeron dirs (log buffers, cnc.dat).
func mappedSuiteFiles(t *testing.T) int {
	maps, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	n := 0
	for _, line := range strings.Split(string(maps), "\n") {
		if strings.Contains(line, dirA+"/") || strings.Contains(line, dirB+"/") {
			n++
		}
	}
	return n
}

// requireNoLeaks closes the given clients and checks goroutines, fds and mappings return to the baseline.
func requireNoLeaks(t *testing.T, base resources, clients ...*client) {
	t.Helper()
	for _, c := range clients {
		require.NoError(t, c.Close())
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		now := snapshot(t)
		if now.goroutines <= base.goroutines+2 && now.fds <= base.fds && now.maps <= base.maps {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("leak after close: baseline %+v now %+v\n%s", base, now, buf[:n])
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// --------------------------------------------------------------------------------------------------- go helper

var peerOnce sync.Once
var peerPath string
var peerErr error

// peerBinary builds ./nfpeer, a Go publisher/subscriber process the tests kill -9.
func peerBinary(t *testing.T) string {
	t.Helper()
	peerOnce.Do(func() {
		dir, err := os.MkdirTemp("", "nfpeer")
		if err != nil {
			peerErr = err
			return
		}
		peerPath = filepath.Join(dir, "nfpeer")
		out, err := exec.Command("go", "build", "-o", peerPath, "./nfpeer").CombinedOutput()
		if err != nil {
			peerErr = fmt.Errorf("go build ./nfpeer: %v: %s", err, out)
		}
	})
	require.NoError(t, peerErr)
	return peerPath
}
