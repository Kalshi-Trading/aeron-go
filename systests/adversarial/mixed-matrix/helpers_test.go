// Package mixedmatrix adversarially tests a mixed matrix of Go and Java publishers and consumers against a real
// Java Aeron 1.53.3 ArchivingMediaDriver while processes come and go and get SIGKILLed / SIGSTOPped on a seeded
// random schedule.
//
// Publishers: Go concurrent Publication.Offer, Go ExclusivePublication.Offer, Go TryClaim, Go OfferBlock (all in
// the killable cmd/mmpeer child process or in-process), Java Publication and Java ExclusivePublication (the
// MatrixPeer container of docker/adversarial-mixed-matrix). Consumers: Go Poll + FragmentAssembler, Go
// ControlledPoll + ControlledFragmentAssembler with random ABORT/BREAK/COMMIT, spy subscriptions,
// multi-destination subscriptions, Java FragmentAssembler subscriber and archive recording + replay. Transports:
// IPC, UDP unicast, multicast, manual and dynamic MDC. Message sizes include 0, 1, exactly MTU payload, the maximum
// message length and fragmented sizes. Every message carries a CRC32 and (writer, seq) in its reserved value; every
// consumer checks that fragment positions are contiguous from the image join position and that each writer's
// messages arrive in order exactly once per image.
//
// Resources: AERON_DIR /dev/shm/aeron-adv-mixed-matrix, UDP ports 27700-27799 (archive control 27790), stream ids
// 9700-9799, compose project adv-mixed-matrix (docker/adversarial-mixed-matrix.compose.yml).
//
// Run (from the repository root):
//
//	make docker-mirror jar
//	docker compose -f docker/adversarial-mixed-matrix.compose.yml --profile tools build
//	AERON_E2E=1 AERON_ADVERSARIAL=1 go test -p 1 -count=1 -v ./systests/adversarial/mixed-matrix/
//
// MM_SEED=<n> replays a schedule (every test logs its seed). The 20 minute soak runs only with MM_SOAK=20m (any
// duration). Network faults use `sudo -n tc netem` on lo and are skipped when that is not permitted.
package mixedmatrix

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	abuf "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/systests/adversarial/mixed-matrix/internal/mm"
	"github.com/stretchr/testify/require"
)

const (
	aeronDir       = "/dev/shm/aeron-adv-mixed-matrix"
	driverName     = "adv-mixed-matrix-driver"
	javaImage      = "aeron-go-adv-mixed-matrix-java:1.53.3"
	archiveControl = "aeron:udp?endpoint=localhost:27790"
	controlStream  = 10
	project        = "adv-mixed-matrix"
	composeFile    = "../../../docker/adversarial-mixed-matrix.compose.yml"
	peerLabel      = "aeron-go.adv-mixed-matrix"
	driverTimeout  = 3 * time.Second
)

var peerBinary string

func TestMain(m *testing.M) {
	if os.Getenv("AERON_E2E") != "1" || os.Getenv("AERON_ADVERSARIAL") != "1" {
		os.Exit(m.Run()) // every test skips
	}
	dir, err := os.MkdirTemp("", "mmpeer")
	if err != nil {
		panic(err)
	}
	peerBinary = filepath.Join(dir, "mmpeer")
	if out, err := exec.Command("go", "build", "-o", peerBinary, "./cmd/mmpeer").CombinedOutput(); err != nil {
		panic(fmt.Sprintf("building mmpeer: %v\n%s", err, out))
	}
	code := m.Run()
	removePeers()
	_ = compose("down", "--timeout", "1")
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func requireAdv(t *testing.T) {
	t.Helper()
	if os.Getenv("AERON_E2E") != "1" || os.Getenv("AERON_ADVERSARIAL") != "1" {
		t.Skip("set AERON_E2E=1 AERON_ADVERSARIAL=1 to run the adversarial mixed-matrix suite")
	}
}

// seed returns MM_SEED or a fresh seed, logged so a failing schedule can be replayed.
func seed(t *testing.T) *rand.Rand {
	s := time.Now().UnixNano()
	if v := os.Getenv("MM_SEED"); v != "" {
		s, _ = strconv.ParseInt(v, 10, 64)
	}
	t.Logf("seed MM_SEED=%d", s)
	return rand.New(rand.NewSource(s))
}

func run(name string, args ...string) error {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %v: %s", name, strings.Join(args, " "), err, out)
	}
	return nil
}

func compose(args ...string) error {
	return run("docker", append([]string{"compose", "-p", project, "-f", composeFile}, args...)...)
}

func removePeers() {
	out, _ := exec.Command("docker", "ps", "-aq", "--filter", "label="+peerLabel).Output()
	if ids := strings.Fields(string(out)); len(ids) > 0 {
		_ = run("docker", append([]string{"rm", "-f"}, ids...)...)
	}
}

// freshStack (re)creates the driver + archive with empty directories and waits for the driver to be usable.
func freshStack(t *testing.T) {
	t.Helper()
	removePeers()
	_ = run("docker", "rm", "-f", driverName)
	require.NoError(t, compose("up", "-d", "--wait", "adv-mm-driver"))
	awaitDriver(t)
	t.Cleanup(removePeers)
}

func awaitDriver(t *testing.T) {
	t.Helper()
	awaitTrue(t, "driver to accept clients", 30*time.Second, func() bool {
		a, err := aeron.Connect(newContext())
		if err != nil {
			return false
		}
		_ = a.Close()
		return true
	})
}

func driverSignal(t *testing.T, sig string) {
	t.Helper()
	require.NoError(t, run("docker", "kill", "-s", sig, driverName))
}

// restartDriver starts the (killed) driver container again; the driver deletes the old aeron dir on start.
func restartDriver(t *testing.T) {
	t.Helper()
	require.NoError(t, compose("up", "-d", "--wait", "adv-mm-driver"))
	awaitDriver(t)
}

func newContext() *aeron.Context {
	return aeron.NewContext().AeronDir(aeronDir).MediaDriverTimeout(driverTimeout)
}

// client is an in-process Go client whose ErrorHandler output is recorded.
type client struct {
	*aeron.Aeron
	mu     sync.Mutex
	errors []error
}

func (c *client) Errors() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.errors...)
}

func connect(t *testing.T, configure ...func(*aeron.Context)) *client {
	t.Helper()
	c := &client{}
	ctx := newContext().ErrorHandler(func(err error) {
		c.mu.Lock()
		c.errors = append(c.errors, err)
		c.mu.Unlock()
	})
	for _, f := range configure {
		f(ctx)
	}
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

func connectArchive(t *testing.T) *archive.Archive {
	t.Helper()
	var arch *archive.Archive
	var err error
	deadline := time.Now().Add(30 * time.Second)
	for {
		options := archive.DefaultOptions()
		options.RequestChannel = archiveControl
		options.RequestStream = controlStream
		options.ResponseChannel = "aeron:udp?endpoint=localhost:0"
		options.Timeout = 5 * time.Second
		arch, err = archive.NewArchive(options, newContext())
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = arch.Close() })
	return arch
}

func awaitTrue(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// --- child process peers -------------------------------------------------------------------------------------

var peerCounter atomic.Int64

// peer is a killable child client: a Go mmpeer process or a Java MatrixPeer container.
type peer struct {
	t      *testing.T
	name   string
	java   bool
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	events chan mm.Event
	exited chan struct{}
	mu     sync.Mutex
	log    []mm.Event
	stderr bytes.Buffer
}

// startPeer starts a child peer. lang is "go" or "java"; mode "pub" or "sub"; args are key=value pairs.
func startPeer(t *testing.T, lang, mode string, args ...string) *peer {
	t.Helper()
	p := &peer{t: t, name: fmt.Sprintf("adv-mm-%s-%s-%d", lang, mode, peerCounter.Add(1)), java: lang == "java",
		events: make(chan mm.Event, 1024), exited: make(chan struct{})}
	args = append([]string{mode}, append(args, "dir="+aeronDir,
		"driverTimeoutMs="+strconv.Itoa(int(driverTimeout/time.Millisecond)))...)
	if p.java {
		dockerArgs := []string{"run", "--rm", "-i", "--name", p.name, "--label", peerLabel, "--network", "host",
			"--ipc", "host", "-v", "/dev/shm:/dev/shm", "--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
			javaImage}
		p.cmd = exec.Command("docker", append(dockerArgs, args...)...)
	} else {
		p.cmd = exec.Command(peerBinary, args...)
	}
	var err error
	p.stdin, err = p.cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := p.cmd.StdoutPipe()
	require.NoError(t, err)
	p.cmd.Stderr = &p.stderr
	require.NoError(t, p.cmd.Start())
	go func() {
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 64*1024), 1<<20)
		for sc.Scan() {
			var ev mm.Event
			if json.Unmarshal(sc.Bytes(), &ev) != nil {
				continue
			}
			p.mu.Lock()
			p.log = append(p.log, ev)
			p.mu.Unlock()
			select {
			case p.events <- ev:
			default:
			}
		}
		_ = p.cmd.Wait()
		close(p.exited)
	}()
	t.Cleanup(func() { p.Kill() })
	return p
}

// await returns the next event with the given name, failing the test after timeout.
func (p *peer) await(name string, timeout time.Duration) mm.Event {
	p.t.Helper()
	ev, err := p.awaitErr(name, timeout)
	require.NoError(p.t, err)
	return ev
}

func (p *peer) awaitErr(name string, timeout time.Duration) (mm.Event, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	exited := false
	for {
		var ev mm.Event
		if exited {
			select {
			case ev = <-p.events:
			default:
				return nil, fmt.Errorf("%s exited waiting for %s; stderr: %s", p.name, name, p.stderrTail())
			}
		} else {
			select {
			case ev = <-p.events:
			case <-p.exited:
				exited = true // the reader closes exited only after queueing every event
				continue
			case <-timer.C:
				return nil, fmt.Errorf("%s: timed out after %v waiting for %s; stderr: %s", p.name, timeout, name,
					p.stderrTail())
			}
		}
		if ev.Str("event") == name {
			return ev, nil
		}
		if ev.Str("event") == "terminated" {
			return ev, fmt.Errorf("%s terminated waiting for %s: %s", p.name, name, ev.Str("error"))
		}
	}
}

func (p *peer) stderrTail() string {
	s := p.stderr.String()
	if len(s) > 2000 {
		s = s[len(s)-2000:]
	}
	return s
}

// events returns every event of name seen so far.
func (p *peer) eventsNamed(name string) []mm.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []mm.Event
	for _, ev := range p.log {
		if ev.Str("event") == name {
			out = append(out, ev)
		}
	}
	return out
}

// signal sends sig (KILL, STOP, CONT) to the peer process for real.
func (p *peer) signal(sig string) {
	if p.java {
		_ = run("docker", "kill", "-s", sig, p.name)
		return
	}
	switch sig {
	case "KILL":
		_ = p.cmd.Process.Kill()
	default:
		_ = run("kill", "-"+sig, strconv.Itoa(p.cmd.Process.Pid))
	}
}

// Kill SIGKILLs the peer and waits for it to exit.
func (p *peer) Kill() {
	select {
	case <-p.exited:
		return
	default:
	}
	p.signal("CONT")
	p.signal("KILL")
	select {
	case <-p.exited:
	case <-time.After(15 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.exited
	}
}

// close asks the peer to shut down gracefully and returns its final event ("closed" for publishers, the "report"
// events are collected before "done" for subscribers).
func (p *peer) close(final string) mm.Event {
	p.t.Helper()
	_, _ = io.WriteString(p.stdin, "close\n")
	ev := p.await(final, 20*time.Second)
	select {
	case <-p.exited:
	case <-time.After(20 * time.Second):
		p.t.Fatalf("%s did not exit after close", p.name)
	}
	require.Equal(p.t, 0, p.cmd.ProcessState.ExitCode(), "%s exit code; stderr: %s", p.name, p.stderrTail())
	return ev
}

// --- leak checks ---------------------------------------------------------------------------------------------

type resources struct {
	goroutines int
	fds        int
	mmaps      int
}

func snapshot() resources {
	fds, _ := os.ReadDir("/proc/self/fd")
	return resources{goroutines: runtime.NumGoroutine(), fds: len(fds), mmaps: mappedAeronFiles()}
}

func mappedAeronFiles() int {
	data, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		return -1
	}
	n := 0
	for _, line := range strings.Split(string(data), "\n") {
		if strings.Contains(line, aeronDir) {
			n++
		}
	}
	return n
}

// requireNoLeaks waits for goroutines, fds and aeron mmaps to return to the baseline taken before the test created
// any client.
func requireNoLeaks(t *testing.T, base resources) {
	t.Helper()
	var now resources
	deadline := time.Now().Add(15 * time.Second)
	for {
		now = snapshot()
		if now.goroutines <= base.goroutines && now.fds <= base.fds && now.mmaps <= base.mmaps {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("leak: baseline %+v, now %+v\n%s", base, now, buf[:n])
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// driverCounters counts the allocated driver counters by type id, observed through a fresh short-lived client.
func driverCounters(t *testing.T) map[int32]int {
	t.Helper()
	a, err := aeron.Connect(newContext())
	require.NoError(t, err)
	defer a.Close()
	own := a.ClientID()
	out := map[int32]int{}
	a.CounterReader().ForEachMetaData(func(id, typeID int32, key *abuf.Buffer, label string) {
		if typeID == counters.DriverHeartbeatTypeId && key.GetInt64(0) == own {
			return
		}
		out[typeID]++
	})
	return out
}

// requireCountersReturn waits for the driver's counters to return to the baseline (client heartbeats, publication
// and subscription positions of killed/closed clients are reclaimed after client liveness + linger).
func requireCountersReturn(t *testing.T, base map[int32]int) {
	t.Helper()
	var now map[int32]int
	deadline := time.Now().Add(30 * time.Second)
	for {
		now = driverCounters(t)
		if fmt.Sprint(now) == fmt.Sprint(base) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("driver counters did not return to baseline: base %v now %v", base, now)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// requireTerminatedWith waits for the peer to exit with a "terminated" event after reporting an error containing
// want (case-insensitive) to its error handler.
func requireTerminatedWith(t *testing.T, p *peer, want string, timeout time.Duration) {
	t.Helper()
	ev, err := p.awaitErr("never", timeout)
	require.Error(t, err)
	require.Equal(t, "terminated", ev.Str("event"), "%s: %v", p.name, err)
	var msgs []string
	for _, e := range p.eventsNamed("clientError") {
		if strings.Contains(strings.ToLower(e.Str("message")), want) {
			return
		}
		msgs = append(msgs, e.Str("message"))
	}
	t.Fatalf("%s terminated (%s) without reporting %q to its error handler; errors: %v", p.name, ev.Str("error"),
		want, msgs)
}
