// Package drivercrash adversarially tests how the Go client survives Java 1.53.3 media driver crashes, stalls and
// restarts, and crashes of client processes, against real drivers in docker/adversarial-driver-crash.compose.yml.
// Drivers are SIGKILLed, SIGSTOPped/SIGCONTed and restarted (with aeron.dir.delete.on.start, so a restart creates a
// new CnC) while Go publishers and subscribers of every kind are active. Client crashes use real child processes:
// internal/crashpeer (Go) and docker/adversarial-driver-crash/JavaPeer.java (Java 1.53.3, the reference behaviour),
// which both report their error-handler, close-handler and image events as JSON lines.
//
// Resources: AERON_DIR /dev/shm/aeron-adv-driver-crash, UDP ports 27000-27099 (archive control 27010, events 27030),
// stream ids 9000-9099, compose project adv-driver-crash. Every test (re)creates the driver it needs, so tests do not
// depend on order.
//
//	make docker-mirror   # pre-pull base images (Docker Hub rate limits)
//	docker compose -f docker/adversarial-driver-crash.compose.yml --profile tools --profile archive build
//	AERON_E2E=1 AERON_ADVERSARIAL=1 go test -count=1 -p 1 -timeout 30m -v ./systests/adversarial/driver-crash/
package drivercrash

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	aeronDir      = "/dev/shm/aeron-adv-driver-crash"
	composeFile   = "../../../docker/adversarial-driver-crash.compose.yml"
	plainDriver   = "adv-driver-crash-driver"
	archDriver    = "adv-driver-crash-archiving-driver"
	javaPeerImage = "adv-driver-crash-java-peer:1.53.3"

	controlChannel = "aeron:udp?endpoint=localhost:27010"
	controlStream  = 10

	// Driver settings from the compose file.
	clientLiveness = 2 * time.Second
	imageLiveness  = 2 * time.Second
	unblockTimeout = 3 * time.Second

	// driverTimeout is the Go clients' media driver timeout (Context.MediaDriverTimeout).
	driverTimeout = 2 * time.Second
)

// udp returns a UDP endpoint inside this suite's port range 27000-27099.
func udp(port int) string { return fmt.Sprintf("localhost:%d", 27000+port) }

var crashPeerBin string

func TestMain(m *testing.M) {
	code := func() int {
		if !enabled() {
			return m.Run()
		}
		dir, err := os.MkdirTemp("", "adv-driver-crash")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(dir)
		crashPeerBin = filepath.Join(dir, "crashpeer")
		if out, err := exec.Command("go", "build", "-o", crashPeerBin, "./internal/crashpeer").CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "building crashpeer: %v\n%s", err, out)
			return 1
		}
		code := m.Run()
		_ = exec.Command("docker", "rm", "-f", plainDriver, archDriver).Run()
		return code
	}()
	os.Exit(code)
}

func enabled() bool { return e2e.Enabled() && os.Getenv("AERON_ADVERSARIAL") == "1" }

func requireAdv(t *testing.T) {
	t.Helper()
	if !enabled() {
		t.Skip("set AERON_E2E=1 AERON_ADVERSARIAL=1 (kills/stops/restarts docker/adversarial-driver-crash.compose.yml)")
	}
}

func compose(args ...string) error {
	all := append([]string{"compose", "-f", composeFile, "--profile", "archive"}, args...)
	out, err := exec.Command("docker", all...).CombinedOutput()
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

// driverKind selects one of the two drivers sharing aeronDir.
type driverKind struct{ service, container string }

var (
	plain     = driverKind{"driver", plainDriver}
	archiving = driverKind{"archiving-driver", archDriver}
)

// startDriver force-recreates kind's container (so every test starts from a fresh driver and a new CnC) after removing
// the other driver, and waits until it is healthy. A cleanup SIGCONTs it in case the test stopped it.
func startDriver(t *testing.T, kind driverKind) {
	t.Helper()
	other := plain
	if kind == plain {
		other = archiving
	}
	_ = exec.Command("docker", "rm", "-f", other.container).Run()
	_ = exec.Command("docker", "kill", "-s", "CONT", kind.container).Run()
	require.NoError(t, compose("up", "-d", "--wait", "--force-recreate", kind.service))
	awaitTrue(t, "driver heartbeat", 20*time.Second, func() bool {
		ok, _ := aeron.IsDriverActive(aeronDir, driverTimeout, nil)
		return ok
	})
	t.Cleanup(func() { _ = exec.Command("docker", "kill", "-s", "CONT", kind.container).Run() })
}

// restartDriver starts kind again after a crash (docker start keeps the container; delete.on.start makes a new CnC).
func restartDriver(t *testing.T, kind driverKind) {
	t.Helper()
	_ = exec.Command("docker", "kill", "-s", "CONT", kind.container).Run()
	_ = exec.Command("docker", "kill", kind.container).Run()
	awaitTrue(t, "driver container stopped", 15*time.Second, func() bool { return !running(kind.container) })
	require.NoError(t, compose("up", "-d", "--wait", kind.service))
	awaitTrue(t, "driver heartbeat", 20*time.Second, func() bool {
		ok, _ := aeron.IsDriverActive(aeronDir, driverTimeout, nil)
		return ok
	})
}

func signalDriver(t *testing.T, kind driverKind, sig string) {
	t.Helper()
	docker(t, "kill", "-s", sig, kind.container)
}

func running(container string) bool {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", container).Output()
	return err == nil && strings.TrimSpace(string(out)) == "true"
}

// observer records what the client reported through its context handlers.
type observer struct {
	mu          sync.Mutex
	errs        []error
	closes      atomic.Int32
	available   atomic.Int32
	unavailable atomic.Int32
}

func (o *observer) errors() []error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]error(nil), o.errs...)
}

func (o *observer) context(timeout time.Duration) *aeron.Context {
	return aeron.NewContext().AeronDir(aeronDir).MediaDriverTimeout(timeout).
		ErrorHandler(func(err error) {
			o.mu.Lock()
			o.errs = append(o.errs, err)
			o.mu.Unlock()
		}).
		CloseHandler(func() { o.closes.Add(1) }).
		AvailableImageHandler(func(aeron.Image) { o.available.Add(1) }).
		UnavailableImageHandler(func(aeron.Image) { o.unavailable.Add(1) })
}

// connectWith connects ctx, retrying while a (re)started driver comes up. Close is registered as a cleanup.
func connectWith(t *testing.T, ctx *aeron.Context) *aeron.Aeron {
	t.Helper()
	a, err := aeron.Connect(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func connect(t *testing.T) (*aeron.Aeron, *observer) {
	t.Helper()
	o := &observer{}
	return connectWith(t, o.context(driverTimeout)), o
}

// classify maps a Go client error to the simple name of the equivalent Java 1.53.3 exception (as JavaPeer reports).
func classify(err error) string {
	var dt *aeron.DriverTimeoutError
	var ct *aeron.ClientTimeoutError
	var st *aeron.ConductorServiceTimeoutError
	switch {
	case errors.As(err, &dt):
		return "DriverTimeout"
	case errors.As(err, &ct):
		return "ClientTimeout"
	case errors.As(err, &st):
		return "ConductorServiceTimeout"
	}
	return fmt.Sprintf("%T", err)
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

// within runs fn and fails if it does not return within limit (a hang), returning how long it took.
func within(t *testing.T, what string, limit time.Duration, fn func()) time.Duration {
	t.Helper()
	start := time.Now()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
		return time.Since(start)
	case <-time.After(limit):
		buf := make([]byte, 1<<20)
		t.Fatalf("%s hung for more than %v\n%s", what, limit, buf[:runtime.Stack(buf, true)])
		return 0
	}
}

// ------------------------------------------------------------------------------------------------- leak checks

type resources struct {
	goroutines, fds, maps int
}

func snapshot(t *testing.T) resources {
	t.Helper()
	return resources{runtime.NumGoroutine(), openFDs(t), mappedFiles(t)}
}

func openFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(entries)
}

// mappedFiles counts this process's mappings of files under aeronDir, including unlinked ("(deleted)") files left
// behind when a restarted driver recreated the directory.
func mappedFiles(t *testing.T) int {
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

// requireNoLeaks waits for goroutines, fds and aeronDir mappings to return to the baseline.
func requireNoLeaks(t *testing.T, base resources) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		now := snapshot(t)
		if now.goroutines <= base.goroutines && now.fds <= base.fds && now.maps == 0 {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			t.Fatalf("leak: goroutines %d (base %d), fds %d (base %d), aeron dir mappings %d\n%s", now.goroutines,
				base.goroutines, now.fds, base.fds, now.maps, buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ------------------------------------------------------------------------------------------- driver-side counters

const (
	pubLimitTypeID  = 1
	subPosTypeID    = 4
	heartbeatTypeID = 11
)

// countCounters counts allocated driver counters matching (typeID, label substring) as seen through reader.
func countCounters(reader *counters.Reader, typeID int32, labelPart string) int {
	n := 0
	reader.Scan(func(c counters.Counter) {
		if c.TypeId == typeID && strings.Contains(c.Label, labelPart) {
			n++
		}
	})
	return n
}

// systemCounter returns the value of the driver system counter with the given label.
func systemCounter(reader *counters.Reader, label string) int64 {
	v := int64(-1)
	reader.Scan(func(c counters.Counter) {
		if c.TypeId == 0 && c.Label == label {
			v = c.Value
		}
	})
	return v
}

// heartbeatCount is the number of client heartbeat counters with the given client id (0 = any client).
func heartbeatCount(reader *counters.Reader, clientID int64) int {
	n := 0
	reader.Scan(func(c counters.Counter) {
		if c.TypeId != heartbeatTypeID {
			return
		}
		if owner, err := reader.GetKeyPartInt64(c.Id, 0); err == nil && (clientID == 0 || owner == clientID) {
			n++
		}
	})
	return n
}

// ----------------------------------------------------------------------------------------------- peer processes

type event map[string]any

func (e event) name() string { s, _ := e["event"].(string); return s }
func (e event) str(k string) string {
	s, _ := e[k].(string)
	return s
}
func (e event) int(k string) int64 {
	switch v := e[k].(type) {
	case json.Number:
		n, _ := v.Int64()
		return n
	case float64:
		return int64(v)
	}
	return 0
}

// peer is a Go crashpeer child process or a JavaPeer container whose JSON events are collected.
type peer struct {
	t      *testing.T
	lang   string
	name   string // docker container name (java)
	cmd    *exec.Cmd
	mu     sync.Mutex
	events []event
	done   chan struct{}
}

// startGoPeer runs internal/crashpeer with the given flags (e.g. "-role=pub").
func startGoPeer(t *testing.T, args ...string) *peer {
	t.Helper()
	p := &peer{t: t, lang: "go"}
	p.cmd = exec.Command(crashPeerBin, append([]string{"-dir=" + aeronDir}, args...)...)
	p.start()
	return p
}

var javaPeerSeq atomic.Int32

// startJavaPeer runs JavaPeer (key=value args) in its own container so it can be killed and frozen.
func startJavaPeer(t *testing.T, args ...string) *peer {
	t.Helper()
	p := &peer{t: t, lang: "java", name: fmt.Sprintf("adv-driver-crash-java-%d-%d", os.Getpid(), javaPeerSeq.Add(1))}
	p.cmd = exec.Command("docker", append([]string{"run", "--rm", "-i", "--init", "--name", p.name, "--network",
		"host", "--ipc", "host", "-u", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), "-v", "/dev/shm:/dev/shm",
		javaPeerImage, "dir=" + aeronDir}, args...)...)
	p.start()
	return p
}

func (p *peer) start() {
	t := p.t
	t.Helper()
	stdout, err := p.cmd.StdoutPipe()
	require.NoError(t, err)
	p.cmd.Stderr = &prefixWriter{t: t, prefix: p.lang + " peer stderr: "}
	require.NoError(t, p.cmd.Start())
	p.done = make(chan struct{})
	go func() {
		defer close(p.done)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			var e event
			dec := json.NewDecoder(strings.NewReader(sc.Text()))
			dec.UseNumber()
			if dec.Decode(&e) != nil {
				continue
			}
			p.mu.Lock()
			p.events = append(p.events, e)
			p.mu.Unlock()
		}
		_ = p.cmd.Wait()
	}()
	t.Cleanup(p.kill)
}

// await returns the first event called name, failing the test after timeout.
func (p *peer) await(name string, timeout time.Duration) event {
	p.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if e := p.find(name); e != nil {
			return e
		}
		if time.Now().After(deadline) {
			p.t.Fatalf("%s peer: no %q event after %v; events: %v", p.lang, name, timeout, p.all())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (p *peer) find(name string) event {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.events {
		if e.name() == name {
			return e
		}
	}
	return nil
}

func (p *peer) count(name string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, e := range p.events {
		if e.name() == name {
			n++
		}
	}
	return n
}

func (p *peer) last(name string) event {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := len(p.events) - 1; i >= 0; i-- {
		if p.events[i].name() == name {
			return p.events[i]
		}
	}
	return nil
}

func (p *peer) all() []event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]event(nil), p.events...)
}

// signal delivers sig to the client process: the Go child directly, the Java container via docker (STOP/CONT use the
// cgroup freezer, docker pause/unpause, because the JVM runs under docker-init).
func (p *peer) signal(sig syscall.Signal) {
	p.t.Helper()
	if p.lang == "go" {
		require.NoError(p.t, p.cmd.Process.Signal(sig))
		return
	}
	switch sig {
	case syscall.SIGSTOP:
		docker(p.t, "pause", p.name)
	case syscall.SIGCONT:
		docker(p.t, "unpause", p.name)
	default:
		docker(p.t, "kill", "-s", "KILL", p.name)
	}
}

// exited waits for the process to exit.
func (p *peer) exited(timeout time.Duration) bool {
	select {
	case <-p.done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (p *peer) kill() {
	if p.lang == "java" {
		_ = exec.Command("docker", "unpause", p.name).Run()
		_ = exec.Command("docker", "kill", p.name).Run()
	} else if p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(syscall.SIGCONT)
		_ = p.cmd.Process.Kill()
	}
	p.exited(15 * time.Second)
}

type prefixWriter struct {
	t      *testing.T
	prefix string
}

func (w *prefixWriter) Write(b []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
		if line != "" {
			fmt.Fprintln(os.Stderr, w.prefix+line)
		}
	}
	return len(b), nil
}

var _ io.Writer = (*prefixWriter)(nil)
