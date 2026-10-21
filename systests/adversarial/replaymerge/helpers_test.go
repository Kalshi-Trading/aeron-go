// Package replaymerge holds adversarial end-to-end fault tests for archive/replaymerge (Java ReplayMerge parity) and
// archive/persistentsubscription, run against the Java Aeron 1.53.3 media driver and archive in
// docker/adversarial-replaymerge.compose.yml (compose project adv-replaymerge). Faults are real: the driver and
// archive containers are SIGKILLed / SIGSTOPped, publishers and consumers run as child processes (./rmhelper, or the
// Java oracle docker/adversarial-replaymerge/RmOracle.java) that are killed with SIGKILL, and network loss and
// partitions are injected with tc netem on lo (sudo), filtered to this suite's UDP ports. Every scenario that has a
// Java equivalent also runs the Java ReplayMerge / PersistentSubscription oracle against the same fault and compares
// outcomes.
//
// Resources: AERON_DIR /dev/shm/aeron-adv-replaymerge, UDP ports 27400-27499 (archive control 27410, events 27411),
// stream ids 9400-9499. Each test recreates the stack, so tests do not depend on order.
//
//	make docker-mirror jar
//	docker compose -p adv-replaymerge -f docker/adversarial-replaymerge.compose.yml build rm-driver
//	AERON_E2E=1 AERON_ADVERSARIAL=1 go test -count=1 -p 1 -v -timeout 60m ./systests/adversarial/replaymerge/
//	docker compose -p adv-replaymerge -f docker/adversarial-replaymerge.compose.yml down
//
// Network fault tests need passwordless sudo for tc; they are skipped with a reason otherwise.
package replaymerge

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/systests/adversarial/replaymerge/internal/rmrun"
)

const (
	aeronDir       = "/dev/shm/aeron-adv-replaymerge"
	archiveDataDir = "/dev/shm/aeron-adv-replaymerge-archive"
	driverName     = "adv-replaymerge-driver"
	archiveName    = "adv-replaymerge-archive"
	composeFile    = "../../../docker/adversarial-replaymerge.compose.yml"
	project        = "adv-replaymerge"
)

var helperBin string

func TestMain(m *testing.M) {
	code := 0
	if enabled() {
		dir, err := os.MkdirTemp("", "rmhelper")
		if err != nil {
			panic(err)
		}
		helperBin = filepath.Join(dir, "rmhelper")
		if out, err := exec.Command("go", "build", "-o", helperBin, "./rmhelper").CombinedOutput(); err != nil {
			panic(fmt.Sprintf("build rmhelper: %v: %s", err, out))
		}
		code = m.Run()
		_ = os.RemoveAll(dir)
	} else {
		code = m.Run()
	}
	os.Exit(code)
}

func enabled() bool { return os.Getenv("AERON_E2E") == "1" && os.Getenv("AERON_ADVERSARIAL") == "1" }

func requireAdversarial(t *testing.T) {
	t.Helper()
	if !enabled() {
		t.Skip("set AERON_E2E=1 AERON_ADVERSARIAL=1 (see package doc)")
	}
}

func compose(args ...string) error {
	out, err := exec.Command("docker", append([]string{"compose", "-p", project, "-f", composeFile}, args...)...).
		CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose %v: %v: %s", args, err, out)
	}
	return nil
}

func docker(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	require.NoError(t, err, "docker %v: %s", args, out)
}

func running(container string) bool {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", container).Output()
	return err == nil && strings.TrimSpace(string(out)) == "true"
}

// freshStack recreates the driver and archive with an empty archive catalog and registers teardown (unpause, clear
// netem) so every test starts from the same state.
func freshStack(t *testing.T) {
	t.Helper()
	requireAdversarial(t)
	clearNetem()
	stopStack(t)
	require.NoError(t, os.RemoveAll(archiveDataDir))
	require.NoError(t, compose("up", "-d", "--wait", "rm-driver", "rm-archive"))
	t.Cleanup(func() {
		clearNetem()
		for _, c := range []string{driverName, archiveName} {
			_ = exec.Command("docker", "unpause", c).Run()
		}
	})
}

func stopStack(t *testing.T) {
	t.Helper()
	_ = exec.Command("docker", "unpause", driverName).Run()
	_ = exec.Command("docker", "unpause", archiveName).Run()
	require.NoError(t, compose("rm", "-sf", "rm-archive", "rm-driver"))
}

// restartArchive starts the archive again after it was killed (the catalog in archiveDataDir is kept). The Java
// archive refuses to start ("found existing archive for archiveId") until the driver has timed out the killed
// archive's client, so retry for longer than the client liveness timeout.
func restartArchive() error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := compose("up", "-d", "--wait", "rm-archive")
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(time.Second)
	}
}

// restartStack starts the driver and archive after a driver kill. The archive exits on its own driver timeout, so
// wait for (or force) that first.
func restartStack(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for running(archiveName) && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if running(archiveName) {
		docker(t, "kill", archiveName)
	}
	require.NoError(t, compose("up", "-d", "--wait", "rm-driver", "rm-archive"))
}

// clientErrors collects errors reported through an aeron.Context ErrorHandler.
type clientErrors struct {
	mu   sync.Mutex
	errs []error
}

func (c *clientErrors) add(err error) {
	c.mu.Lock()
	c.errs = append(c.errs, err)
	c.mu.Unlock()
}

func (c *clientErrors) list() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.errs...)
}

// images records available/unavailable image callbacks by session id.
type images struct {
	mu          sync.Mutex
	available   map[int32]int
	unavailable map[int32]int
}

func (im *images) count(unavailable bool, session int32) int {
	im.mu.Lock()
	defer im.mu.Unlock()
	if unavailable {
		return im.unavailable[session]
	}
	return im.available[session]
}

type client struct {
	*aeron.Aeron
	errs   *clientErrors
	images *images
}

func newContext(errs *clientErrors, im *images) *aeron.Context {
	return aeron.NewContext().AeronDir(aeronDir).MediaDriverTimeout(3 * time.Second).
		ErrorHandler(errs.add).
		AvailableImageHandler(func(img aeron.Image) {
			im.mu.Lock()
			im.available[img.SessionID()]++
			im.mu.Unlock()
		}).
		UnavailableImageHandler(func(img aeron.Image) {
			im.mu.Lock()
			im.unavailable[img.SessionID()]++
			im.mu.Unlock()
		})
}

// connect connects a Go client to the suite's driver; it is closed at test cleanup.
func connect(t *testing.T) *client {
	t.Helper()
	c := &client{errs: &clientErrors{}, images: &images{available: map[int32]int{}, unavailable: map[int32]int{}}}
	deadline := time.Now().Add(20 * time.Second)
	for {
		a, err := aeron.Connect(newContext(c.errs, c.images))
		if err == nil {
			c.Aeron = a
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("aeron connect: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func connectArchive(t *testing.T, a *aeron.Aeron) *archive.Archive {
	t.Helper()
	arch, err := rmrun.ConnectArchive(a, time.Now().Add(20*time.Second), rmrun.Nop)
	require.NoError(t, err)
	t.Cleanup(func() { _ = arch.Close() })
	return arch
}

// ports allocates the UDP ports for one live stream: control (publisher MDC control), live (merge live
// destination), replay (replay destination) and record (archive recording subscription).
type ports struct{ control, live, replay, record int }

func portsFor(base int) ports { return ports{base, base + 1, base + 2, base + 3} }

func (p ports) pubChannel(extra string) string {
	return fmt.Sprintf("aeron:udp?control=localhost:%d|control-mode=dynamic|term-length=1m%s", p.control, extra)
}

func (p ports) recordingChannel(session int32) string {
	return fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d|session-id=%d", p.record, p.control,
		session)
}

func (p ports) liveDestination() string {
	return fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d", p.live, p.control)
}

func (p ports) replayDestination() string {
	return fmt.Sprintf("aeron:udp?endpoint=localhost:%d", p.replay)
}

// startRecording records session on the ports' publication and returns the recording id.
func startRecording(t *testing.T, arch *archive.Archive, p ports, stream, session int32) int64 {
	t.Helper()
	_, err := arch.StartRecording(p.recordingChannel(session), stream, false, true)
	require.NoError(t, err)
	var id int64 = -1
	awaitTrue(t, "recording", 10*time.Second, func() bool {
		ds, err := arch.ListRecordingsForUri(0, 100, fmt.Sprintf("localhost:%d", p.record), stream)
		if err != nil {
			return false
		}
		for _, d := range ds {
			if d.SessionId == session {
				id = d.RecordingId
				return true
			}
		}
		return false
	})
	return id
}

func awaitTrue(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// child is a helper process (rmhelper or the Java oracle) reporting JSON events on stdout.
type child struct {
	t      *testing.T
	name   string
	cmd    *exec.Cmd
	mu     sync.Mutex
	events []rmrun.Event
	cond   *sync.Cond
	done   chan struct{}
	stderr strings.Builder
}

func startChild(t *testing.T, name string, cmd *exec.Cmd) *child {
	t.Helper()
	c := &child{t: t, name: name, cmd: cmd, done: make(chan struct{})}
	c.cond = sync.NewCond(&c.mu)
	out, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = &c.stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	go func() {
		sc := bufio.NewScanner(out)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			var e rmrun.Event
			if json.Unmarshal([]byte(line), &e) != nil {
				continue
			}
			c.mu.Lock()
			c.events = append(c.events, e)
			c.cond.Broadcast()
			c.mu.Unlock()
		}
		_ = cmd.Wait()
		close(c.done)
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	}()
	t.Cleanup(func() {
		c.kill()
		if t.Failed() {
			t.Logf("%s events: %v\nstderr: %s", name, c.all(), c.stderr.String())
		}
	})
	return c
}

// startHelper starts rmhelper mode with flags.
func startHelper(t *testing.T, mode string, flags ...string) *child {
	return startChild(t, "rmhelper-"+mode, exec.Command(helperBin, append([]string{mode, "-dir", aeronDir}, flags...)...))
}

var oracleSeq int

// startOracle runs the Java oracle (RmOracle.java) in a container named so it can be SIGKILLed.
func startOracle(t *testing.T, mode string, args ...string) *child {
	t.Helper()
	oracleSeq++
	name := fmt.Sprintf("adv-replaymerge-oracle-%d-%d", os.Getpid(), oracleSeq)
	cmdArgs := append([]string{"compose", "-p", project, "-f", composeFile, "run", "--rm", "--name", name,
		"rm-oracle", mode}, args...)
	c := startChild(t, "oracle-"+mode, exec.Command("docker", cmdArgs...))
	c.name = name
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	return c
}

func (c *child) all() []rmrun.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]rmrun.Event(nil), c.events...)
}

// await waits for an event matching pred.
func (c *child) await(what string, timeout time.Duration, pred func(rmrun.Event) bool) rmrun.Event {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	go func() {
		<-ctx.Done()
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	}()
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; ; {
		for ; i < len(c.events); i++ {
			if pred(c.events[i]) {
				return c.events[i]
			}
		}
		select {
		case <-c.done:
			c.t.Fatalf("%s exited before %s; events %v stderr %s", c.name, what, c.events, c.stderr.String())
		case <-ctx.Done():
			c.t.Fatalf("%s: timed out after %v waiting for %s; events %v", c.name, timeout, what, c.events)
		default:
		}
		c.cond.Wait()
	}
}

func (c *child) awaitEvent(event string, timeout time.Duration) rmrun.Event {
	return c.await(event, timeout, func(e rmrun.Event) bool { return e["event"] == event })
}

// result waits for the child's "result" event.
func (c *child) result(timeout time.Duration) rmrun.Event { return c.awaitEvent("result", timeout) }

func (c *child) kill() {
	if c.cmd.Process != nil {
		_ = syscall.Kill(-c.cmd.Process.Pid, syscall.SIGKILL)
	}
	if strings.HasPrefix(c.name, "adv-replaymerge-oracle-") {
		_ = exec.Command("docker", "kill", "-s", "KILL", c.name).Run()
	}
	select {
	case <-c.done:
	case <-time.After(10 * time.Second):
	}
}

// num returns a numeric event field.
func num(e rmrun.Event, key string) int64 {
	switch v := e[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	}
	return -1
}

// netem applies a tc netem spec to UDP traffic on lo to or from any of the given ports and returns a func that
// removes it. Requires passwordless sudo; skips the test otherwise.
func netem(t *testing.T, spec string, udpPorts ...int) func() {
	t.Helper()
	if exec.Command("sudo", "-n", "tc", "qdisc", "show", "dev", "lo").Run() != nil {
		t.Skip("tc netem needs passwordless sudo (or run a NET_ADMIN host-network sidecar)")
	}
	clearNetem()
	run := func(args ...string) {
		out, err := exec.Command("sudo", append([]string{"-n", "tc"}, args...)...).CombinedOutput()
		require.NoError(t, err, "tc %v: %s", args, out)
	}
	run("qdisc", "add", "dev", "lo", "root", "handle", "1:", "prio", "bands", "4",
		"priomap", "0", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1")
	run(append([]string{"qdisc", "add", "dev", "lo", "parent", "1:4", "handle", "40:", "netem"},
		strings.Fields(spec)...)...)
	for _, p := range udpPorts {
		for _, dir := range []string{"dport", "sport"} {
			run("filter", "add", "dev", "lo", "parent", "1:0", "protocol", "ip", "prio", "1", "u32",
				"match", "ip", "protocol", "17", "0xff", "match", "ip", dir, fmt.Sprint(p), "0xffff", "flowid", "1:4")
		}
	}
	return clearNetem
}

func clearNetem() { _ = exec.Command("sudo", "-n", "tc", "qdisc", "del", "dev", "lo", "root").Run() }

// resources snapshots this process's goroutines, fds and aeron mmaps for leak checks.
type resources struct{ goroutines, fds, maps int }

func snapshot(t *testing.T) resources {
	t.Helper()
	fds, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	maps, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	n := 0
	for _, line := range strings.Split(string(maps), "\n") {
		if strings.Contains(line, aeronDir+"/") {
			n++
		}
	}
	return resources{runtime.NumGoroutine(), len(fds), n}
}

// requireNoLeaks waits for resources to return to (at most) base.
func requireNoLeaks(t *testing.T, base resources) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		now := snapshot(t)
		if now.goroutines <= base.goroutines && now.fds <= base.fds && now.maps <= base.maps {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("resource leak: now %+v, base %+v\n%s", now, base, buf[:n])
		}
		time.Sleep(50 * time.Millisecond)
	}
}
