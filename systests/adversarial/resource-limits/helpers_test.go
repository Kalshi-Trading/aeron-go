// Package resourcelimits holds adversarial end-to-end tests that push the Go client and real Java Aeron 1.53.3 media
// drivers and archives to their resource limits and position edge cases: a full /dev/shm when the driver creates log
// buffers, a full archive disk, an exhausted counters buffer, MAX_POSITION_EXCEEDED and term-id wrap at the end of the
// log, back pressure from a stuck or slow consumer under min/max flow control, the publication window, a 100+
// subscriber fan-out and 1000+ publications on one client. While at those limits they SIGKILL/SIGSTOP the driver
// containers and killable Go client processes (rlhelper) and assert the Java-equivalent result codes, callbacks,
// recovery, data integrity and that nothing (goroutines, mmaps, fds, driver counters) leaks after Close.
//
// The stack is docker/adversarial-resource-limits.compose.yml (compose project adv-resource-limits); every test
// restarts the containers it needs, so tests do not depend on each other's order. Resources: AERON_DIR
// /dev/shm/aeron-adv-resource-limits (and -b, -tiny), UDP ports 27900-27999 and stream ids 9900-9999.
//
//	make docker-mirror jar   # once, avoids Docker Hub / Maven Central rate limits
//	docker compose -p adv-resource-limits -f docker/adversarial-resource-limits.compose.yml build
//	AERON_E2E=1 AERON_ADVERSARIAL=1 go test -p 1 -count=1 -v ./systests/adversarial/resource-limits/
//	docker compose -p adv-resource-limits -f docker/adversarial-resource-limits.compose.yml down
package resourcelimits

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/archive"
	"github.com/stretchr/testify/require"
)

const (
	project     = "adv-resource-limits"
	composeFile = "../../../docker/adversarial-resource-limits.compose.yml"

	dir     = "/dev/shm/aeron-adv-resource-limits"
	dirB    = "/dev/shm/aeron-adv-resource-limits-b"
	dirTiny = "/dev/shm/aeron-adv-resource-limits-tiny"
	dirShm  = "/dev/shm/aeron-adv-resource-limits-shm" // inside the rl-shm-driver container only

	driverSvc  = "rl-driver"
	driverBSvc = "rl-driver-b"
	archiveSvc = "rl-archive"
	shmSvc     = "rl-shm-driver"
	tinySvc    = "rl-tiny-driver"

	driverContainer  = "adv-rl-driver"
	driverBContainer = "adv-rl-driver-b"
	archiveContainer = "adv-rl-archive"
	shmContainer     = "adv-rl-shm-driver"

	archiveControl       = "aeron:udp?endpoint=localhost:27910"
	archiveControlStream = 10
)

var containerOf = map[string]string{
	driverSvc: driverContainer, driverBSvc: driverBContainer, archiveSvc: archiveContainer,
	shmSvc: shmContainer, tinySvc: "adv-rl-tiny-driver",
}

var (
	setupOnce  sync.Once
	setupErr   error
	helperPath string
)

// requireStack skips unless AERON_E2E=1 AERON_ADVERSARIAL=1, builds the images and rlhelper once, and then
// recreates services from scratch so the test starts from empty drivers (aeron.dir.delete.on.start).
func requireStack(t *testing.T, services ...string) {
	t.Helper()
	if os.Getenv("AERON_E2E") != "1" || os.Getenv("AERON_ADVERSARIAL") != "1" {
		t.Skip("set AERON_E2E=1 AERON_ADVERSARIAL=1 (kills/restarts the docker/adversarial-resource-limits.compose.yml containers)")
	}
	setupOnce.Do(func() {
		if setupErr = compose("build"); setupErr != nil {
			return
		}
		bin, err := os.MkdirTemp("", "rlhelper")
		if err != nil {
			setupErr = err
			return
		}
		helperPath = filepath.Join(bin, "rlhelper")
		build := exec.Command("go", "build", "-o", helperPath, "./rlhelper")
		build.Env = append(os.Environ(), "CGO_ENABLED=0")
		out, err := build.CombinedOutput()
		if err != nil {
			setupErr = fmt.Errorf("go build rlhelper: %v: %s", err, out)
		}
	})
	require.NoError(t, setupErr)
	freshStack(t, services...)
}

// freshStack stops every service of the project and recreates the given ones, waiting until they are healthy.
func freshStack(t *testing.T, services ...string) {
	t.Helper()
	require.NoError(t, compose("rm", "-s", "-f"))
	if len(services) > 0 {
		require.NoError(t, compose(append([]string{"up", "-d", "--wait"}, services...)...))
	}
}

func compose(args ...string) error {
	cmd := exec.Command("docker", append([]string{"compose", "-p", project, "-f", composeFile}, args...)...)
	out, err := cmd.CombinedOutput()
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

// signalService sends sig (KILL, STOP, CONT) to the service's java process, which is pid 1 of its container.
func signalService(t *testing.T, service, sig string) {
	t.Helper()
	docker(t, "kill", "-s", sig, containerOf[service])
}

func running(service string) bool {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", containerOf[service]).Output()
	return err == nil && strings.TrimSpace(string(out)) == "true"
}

// errs collects everything delivered to a client's ErrorHandler.
type errs struct {
	mu   sync.Mutex
	list []error
}

func (e *errs) handle(err error) {
	e.mu.Lock()
	e.list = append(e.list, err)
	e.mu.Unlock()
}

func (e *errs) all() []error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]error(nil), e.list...)
}

func (e *errs) find(pred func(error) bool) error {
	for _, err := range e.all() {
		if pred(err) {
			return err
		}
	}
	return nil
}

func newContext(aeronDir string, eh *errs) *aeron.Context {
	ctx := aeron.NewContext().AeronDir(aeronDir).MediaDriverTimeout(10 * time.Second).
		ResourceLingerTimeout(100 * time.Millisecond)
	if eh != nil {
		ctx.ErrorHandler(eh.handle)
	}
	return ctx
}

// connect connects a client to the driver in aeronDir and closes it at the end of the test.
func connect(t *testing.T, ctx *aeron.Context) *aeron.Aeron {
	t.Helper()
	var a *aeron.Aeron
	var err error
	deadline := time.Now().Add(20 * time.Second)
	for {
		a, err = aeron.Connect(ctx)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func connectArchive(t *testing.T, ctx *aeron.Context) *archive.Archive {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		options := archive.DefaultOptions()
		options.RequestChannel = archiveControl
		options.RequestStream = archiveControlStream
		options.ResponseChannel = "aeron:udp?endpoint=localhost:0"
		options.Timeout = 5 * time.Second
		arch, err := archive.NewArchive(options, ctx)
		if err == nil {
			t.Cleanup(func() { _ = arch.Close() })
			return arch
		}
		require.True(t, time.Now().Before(deadline), "connect archive: %v", err)
		time.Sleep(500 * time.Millisecond)
	}
}

// awaitTrue polls cond until it holds or timeout elapses.
func awaitTrue(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// within runs f and fails the test if it does not return in limit: no blocking call may hang.
func within[T any](t *testing.T, what string, limit time.Duration, f func() T) T {
	t.Helper()
	done := make(chan T, 1)
	go func() { done <- f() }()
	select {
	case v := <-done:
		return v
	case <-time.After(limit):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("%s did not return within %v\n%s", what, limit, buf[:n])
		panic("unreachable")
	}
}

// mappedFiles counts this process's memory mappings of files under aeronDir.
func mappedFiles(t *testing.T, aeronDir string) int {
	t.Helper()
	maps, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	n := 0
	for _, line := range strings.Split(string(maps), "\n") {
		if strings.Contains(line, aeronDir+"/") {
			n++
		}
	}
	return n
}

func openFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(entries)
}

// leakBaseline records goroutines and fds before a test's clients exist; check asserts that, once every client is
// closed, goroutines and fds return to the baseline and nothing under the given dirs is still mapped.
type leakBaseline struct {
	goroutines, fds int
}

func baseline(t *testing.T) leakBaseline {
	t.Helper()
	return leakBaseline{goroutines: runtime.NumGoroutine(), fds: openFDs(t)}
}

func (b leakBaseline) check(t *testing.T, dirs ...string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		mapped := 0
		for _, d := range dirs {
			mapped += mappedFiles(t, d)
		}
		g, f := runtime.NumGoroutine(), openFDs(t)
		if mapped == 0 && g <= b.goroutines && f <= b.fds {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("leak after close: mapped=%d goroutines=%d (baseline %d) fds=%d (baseline %d)\n%s",
				mapped, g, b.goroutines, f, b.fds, buf[:n])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Driver counter type ids (io.aeron.driver.status / AeronCounters) that belong to client resources.
const (
	counterPublisherLimit = 1
	counterSenderPosition = 2
	counterReceiverHwm    = 3
	counterSubscriberPos  = 4
	counterReceiverPos    = 5
	counterSendChannel    = 6
	counterRecvChannel    = 7
	counterSenderLimit    = 9
	counterClientHB       = 11
	counterPublisherPos   = 12
	counterSenderBpe      = 13
)

// resourceCounters counts the allocated driver counters of the given types, read through a's counters reader.
func resourceCounters(a *aeron.Aeron, types ...int32) int {
	n := 0
	a.CounterReader().Scan(func(c counters.Counter) {
		for _, ty := range types {
			if c.TypeId == ty {
				n++
			}
		}
	})
	return n
}

// clientCounters returns the publication/subscription/channel counters the driver still holds.
func clientCounters(a *aeron.Aeron) int {
	return resourceCounters(a, counterPublisherLimit, counterSenderPosition, counterReceiverHwm,
		counterSubscriberPos, counterReceiverPos, counterSendChannel, counterRecvChannel, counterSenderLimit,
		counterPublisherPos, counterSenderBpe)
}

// driverErrors returns the driver's "Errors" system counter (distinct error observations in its error log).
func driverErrors(a *aeron.Aeron) int64 {
	var v int64
	a.CounterReader().Scan(func(c counters.Counter) {
		if c.TypeId == 0 && strings.HasPrefix(c.Label, "Errors") {
			v = c.Value
		}
	})
	return v
}

// helperProc is a running rlhelper child process.
type helperProc struct {
	cmd    *exec.Cmd
	lines  chan string
	exited chan struct{}
	mu     sync.Mutex
	seen   []string
}

// startHelper runs rlhelper with args; it is SIGKILLed at the end of the test if still alive.
func startHelper(t *testing.T, args ...string) *helperProc {
	t.Helper()
	return startProcess(t, helperPath, args...)
}

// startProcess runs name with args as a killable child whose stdout is parsed as rlhelper events.
func startProcess(t *testing.T, name string, args ...string) *helperProc {
	t.Helper()
	cmd := exec.Command(name, args...)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	h := &helperProc{cmd: cmd, lines: make(chan string, 1024), exited: make(chan struct{})}
	go h.read(stdout)
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGCONT)
		_ = cmd.Process.Kill()
		<-h.exited
	})
	return h
}

func (h *helperProc) read(r io.Reader) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		h.mu.Lock()
		h.seen = append(h.seen, sc.Text())
		h.mu.Unlock()
		select {
		case h.lines <- sc.Text():
		default:
		}
	}
	_ = h.cmd.Wait()
	close(h.exited)
}

// expect waits for an output line starting with prefix and returns it.
func (h *helperProc) expect(t *testing.T, prefix string, timeout time.Duration) string {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case l := <-h.lines:
			if strings.HasPrefix(l, prefix) {
				return l
			}
			if strings.HasPrefix(l, "fatal") {
				t.Fatalf("rlhelper: %s", l)
			}
		case <-h.exited:
			for {
				select {
				case l := <-h.lines:
					if strings.HasPrefix(l, prefix) {
						return l
					}
				default:
					h.mu.Lock()
					defer h.mu.Unlock()
					t.Fatalf("rlhelper exited (%v) before %q; output:\n%s", h.cmd.ProcessState, prefix,
						strings.Join(h.seen, "\n"))
				}
			}
		case <-deadline:
			h.mu.Lock()
			defer h.mu.Unlock()
			t.Fatalf("rlhelper: no %q within %v; output:\n%s", prefix, timeout, strings.Join(h.seen, "\n"))
		}
	}
}

func (h *helperProc) signal(t *testing.T, sig syscall.Signal) {
	t.Helper()
	require.NoError(t, h.cmd.Process.Signal(sig))
	if sig == syscall.SIGKILL {
		select {
		case <-h.exited:
		case <-time.After(5 * time.Second):
			t.Fatal("rlhelper did not exit after SIGKILL")
		}
	}
}

// field extracts key=value from an rlhelper line.
func field(line, key string) string {
	for _, kv := range strings.Fields(line) {
		if strings.HasPrefix(kv, key+"=") {
			return strings.TrimPrefix(kv, key+"=")
		}
	}
	return ""
}

func payload(size int) *atomic.Buffer {
	return atomic.MakeBuffer(make([]byte, size))
}
