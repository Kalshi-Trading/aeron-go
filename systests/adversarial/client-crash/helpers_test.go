// Package clientcrash is the adversarial "client process crash and stall" suite. It runs the Go client against the
// Java 1.53.3 ArchivingMediaDriver in docker/adversarial-client-crash.compose.yml (compose project adv-client-crash,
// AERON_DIR /dev/shm/aeron-adv-client-crash, client liveness timeout 3s) and crashes things for real: Go client
// processes (internal/crashpeer, built by the suite) and Java InteropTool peers are SIGKILLed and SIGSTOPped
// mid-traffic, the driver container is paused, and callbacks panic, block the conductor and close the client.
// Every test asserts the Java 1.53.3 recovery semantics: driver-side reclamation of the dead client's counters,
// unavailable images on the survivors, flow control that unblocks, no gaps/duplicates/corruption, clean replacement
// clients, errors through the ErrorHandler, and no leaked goroutines, mappings, fds or driver counters after Close.
//
// Resources: UDP ports 27100-27199, stream ids 9100-9199, archive control localhost:27110.
//
//	docker compose -f docker/interop/docker-compose.yml --profile tools build   # Java peers
//	docker compose -f docker/adversarial-client-crash.compose.yml up -d --build --wait
//	AERON_E2E=1 AERON_ADVERSARIAL=1 go test -count=1 -p 1 -v ./systests/adversarial/client-crash/
//	docker compose -f docker/adversarial-client-crash.compose.yml down -v
//
// Each test recreates the driver container first, so tests are independent of order and of earlier runs.
package clientcrash

import (
	"bufio"
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

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

const (
	advDir         = "/dev/shm/aeron-adv-client-crash"
	advContainer   = "adv-client-crash-driver"
	composeFile    = "../../../docker/adversarial-client-crash.compose.yml"
	controlChannel = "aeron:udp?endpoint=localhost:27110"
	interopImage   = "aeron-go-interop-java:1.53.3"
	livenessTO     = 3 * time.Second
	// reclaimTO bounds how long the driver may take to reclaim a dead client: liveness + timer interval + slack.
	reclaimTO = livenessTO + 4*time.Second

	heartbeatTypeID = 11
)

var (
	buildOnce sync.Once
	peerBin   string
	buildErr  error
)

func requireAdv(t *testing.T) {
	t.Helper()
	if !e2e.Enabled() || os.Getenv("AERON_ADVERSARIAL") != "1" {
		t.Skip("set AERON_E2E=1 AERON_ADVERSARIAL=1 (kills docker/adversarial-client-crash.compose.yml containers)")
	}
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "crashpeer")
		if err != nil {
			buildErr = err
			return
		}
		peerBin = filepath.Join(dir, "crashpeer")
		args := []string{"build", "-o", peerBin}
		if raceEnabled {
			args = append(args, "-race")
		}
		out, err := exec.Command("go", append(args, "./internal/crashpeer")...).CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("build crashpeer: %v: %s", err, out)
		}
	})
	require.NoError(t, buildErr)
	freshStack(t)
}

func compose(args ...string) error {
	out, err := exec.Command("docker", append([]string{"compose", "-f", composeFile}, args...)...).CombinedOutput()
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

// freshStack recreates the driver container (and with it the aeron dir and archive catalog), so every test starts
// from an empty driver whatever earlier tests or runs left behind.
func freshStack(t *testing.T) {
	t.Helper()
	require.NoError(t, compose("up", "-d", "--wait", "--force-recreate", "driver"))
	// The CnC file exists before the driver heartbeats; wait until a client can connect.
	a := connectRetry(t, aeron.NewContext(), 20*time.Second)
	require.NoError(t, a.Close())
}

func newContext() *aeron.Context {
	return aeron.NewContext().AeronDir(advDir).MediaDriverTimeout(5 * time.Second)
}

func connectRetry(t *testing.T, ctx *aeron.Context, within time.Duration) *aeron.Aeron {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		a, err := aeron.Connect(ctx.AeronDir(advDir).MediaDriverTimeout(5 * time.Second))
		if err == nil {
			return a
		}
		if time.Now().After(deadline) {
			require.NoError(t, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// connect returns a client closed at test cleanup.
func connect(t *testing.T, ctx *aeron.Context) *aeron.Aeron {
	t.Helper()
	a := connectRetry(t, ctx, 10*time.Second)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func connectArchive(t *testing.T) *archive.Archive {
	t.Helper()
	options := archive.DefaultOptions()
	options.RequestChannel = controlChannel
	options.RequestStream = 10
	options.ResponseChannel = "aeron:udp?endpoint=localhost:0"
	options.Timeout = 5 * time.Second
	arch, err := archive.NewArchive(options, newContext())
	require.NoError(t, err)
	t.Cleanup(func() { _ = arch.Close() })
	return arch
}

func awaitTrue(t *testing.T, what string, timeout time.Duration, cond func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for !cond() {
		if time.Since(start) > timeout {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return time.Since(start)
}

// ------------------------------------------------------------------------------------------------- driver counters

// counterLabels returns the labels of every allocated driver counter whose label contains substr.
func counterLabels(a *aeron.Aeron, substr string) []string {
	var labels []string
	a.CounterReader().Scan(func(c counters.Counter) {
		if strings.Contains(c.Label, substr) {
			labels = append(labels, c.Label)
		}
	})
	return labels
}

// clientCounters returns the labels of the counters the driver keeps for clientID: its heartbeat plus every counter
// owned by the client (publication/subscription positions, user counters).
func clientCounters(a *aeron.Aeron, clientID int64) []string {
	reader := a.CounterReader()
	var labels []string
	reader.Scan(func(c counters.Counter) {
		if c.TypeId == heartbeatTypeID {
			if id, err := reader.GetKeyPartInt64(c.Id, 0); err == nil && id == clientID {
				labels = append(labels, c.Label)
			}
			return
		}
		if reader.GetCounterOwnerID(c.Id) == clientID {
			labels = append(labels, c.Label)
		}
	})
	return labels
}

// awaitReclaimed waits for the driver to drop every counter of a dead client and every counter labelled with tag.
func awaitReclaimed(t *testing.T, a *aeron.Aeron, clientID int64, tag string) time.Duration {
	t.Helper()
	var left []string
	start := time.Now()
	for {
		left = clientCounters(a, clientID)
		if tag != "" {
			left = append(left, counterLabels(a, tag)...)
		}
		if len(left) == 0 {
			return time.Since(start)
		}
		if time.Since(start) > reclaimTO {
			t.Fatalf("timed out after %v waiting for driver to reclaim client %d / %q, still allocated: %q",
				reclaimTO, clientID, tag, left)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// --------------------------------------------------------------------------------------------------- process leaks

type leakBaseline struct {
	goroutines int
	fds        int
}

func baseline() leakBaseline {
	return leakBaseline{goroutines: runtime.NumGoroutine(), fds: fdCount()}
}

func fdCount() int {
	entries, _ := os.ReadDir("/proc/self/fd")
	return len(entries)
}

func mappedFiles(t *testing.T) int {
	t.Helper()
	maps, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	return strings.Count(string(maps), advDir+"/")
}

// requireNoLeaks checks, after every client of the test is closed, that no goroutine, fd or mapping under the aeron
// dir survives.
func (b leakBaseline) requireNoLeaks(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for (runtime.NumGoroutine() > b.goroutines || fdCount() > b.fds || mappedFiles(t) > 0) &&
		time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > b.goroutines {
		buf := make([]byte, 1<<20)
		t.Fatalf("goroutine leak: %d > %d\n%s", n, b.goroutines, buf[:runtime.Stack(buf, true)])
	}
	require.LessOrEqual(t, fdCount(), b.fds, "fd leak")
	require.Zero(t, mappedFiles(t), "mappings under %s left after close", advDir)
}

// ------------------------------------------------------------------------------------------------ child processes

type event map[string]any

func (e event) int(k string) int64 {
	if v, ok := e[k].(json.Number); ok {
		n, _ := v.Int64()
		return n
	}
	return 0
}
func (e event) str(k string) string { s, _ := e[k].(string); return s }

// peer is a crashpeer or Java InteropTool child process with its stdout JSON event stream.
type peer struct {
	t      *testing.T
	label  string
	cmd    *exec.Cmd
	docker string // container name for a Java peer
	mu     sync.Mutex
	events []event
	stderr strings.Builder
	exited chan struct{}
	err    error
}

// startPeer runs internal/crashpeer with args.
func startPeer(t *testing.T, label string, args ...string) *peer {
	t.Helper()
	return start(t, label, "", exec.Command(peerBin, append([]string{"-dir", advDir}, args...)...))
}

// startJava runs the Java 1.53.3 InteropTool (docker/interop) in a named container on the suite driver.
func startJava(t *testing.T, label, mode string, args ...string) *peer {
	t.Helper()
	name := fmt.Sprintf("adv-client-crash-java-%d-%d", os.Getpid(), time.Now().UnixNano())
	cmdArgs := append([]string{"run", "--rm", "-i", "--name", name, "--network", "host", "--ipc", "host",
		"-v", "/dev/shm:/dev/shm", "-u", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"-e", "AERON_PROPS=-Daeron.dir=" + advDir, interopImage, mode}, args...)
	return start(t, label, name, exec.Command("docker", cmdArgs...))
}

func start(t *testing.T, label, container string, cmd *exec.Cmd) *peer {
	t.Helper()
	p := &peer{t: t, label: label, cmd: cmd, docker: container, exited: make(chan struct{})}
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = &lockedBuilder{b: &p.stderr, mu: &p.mu}
	_, err = cmd.StdinPipe() // keeps the Java tool's stdin reader open
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 1<<20), 1<<20)
		for scanner.Scan() {
			dec := json.NewDecoder(strings.NewReader(scanner.Text()))
			dec.UseNumber()
			var e event
			p.mu.Lock()
			if dec.Decode(&e) == nil && e.str("event") != "" {
				p.events = append(p.events, e)
			} else {
				p.stderr.WriteString(scanner.Text() + "\n")
			}
			p.mu.Unlock()
		}
		p.err = cmd.Wait()
		close(p.exited)
	}()
	t.Cleanup(func() {
		if p.docker != "" {
			_ = exec.Command("docker", "kill", p.docker).Run()
		}
		_ = cmd.Process.Kill()
		select {
		case <-p.exited:
		case <-time.After(10 * time.Second):
		}
	})
	return p
}

type lockedBuilder struct {
	b  *strings.Builder
	mu *sync.Mutex
}

func (l *lockedBuilder) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (p *peer) output() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var sb strings.Builder
	for _, e := range p.events {
		b, _ := json.Marshal(e)
		sb.Write(b)
		sb.WriteByte('\n')
	}
	return sb.String() + p.stderr.String()
}

// last returns the latest event called name.
func (p *peer) last(name string) event {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := len(p.events) - 1; i >= 0; i-- {
		if p.events[i].str("event") == name {
			return p.events[i]
		}
	}
	return nil
}

// await waits for an event called name, failing on an error event or an early exit.
func (p *peer) await(name string, timeout time.Duration) event {
	p.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if e := p.last(name); e != nil {
			return e
		}
		if e := p.last("error"); e != nil {
			p.t.Fatalf("%s: error event: %s\n%s", p.label, e.str("message"), p.output())
		}
		select {
		case <-p.exited:
			if e := p.last(name); e != nil {
				return e
			}
			p.t.Fatalf("%s exited (%v) before %q:\n%s", p.label, p.err, name, p.output())
		default:
		}
		if time.Now().After(deadline) {
			p.t.Fatalf("%s: no %q within %v:\n%s", p.label, name, timeout, p.output())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// signal sends sig to the Go process, or to the Java process inside the container.
func (p *peer) signal(sig syscall.Signal) {
	p.t.Helper()
	if p.docker != "" {
		docker(p.t, "kill", "-s", sigName(sig), p.docker)
		return
	}
	require.NoError(p.t, p.cmd.Process.Signal(sig))
}

func sigName(sig syscall.Signal) string {
	switch sig {
	case syscall.SIGKILL:
		return "KILL"
	case syscall.SIGSTOP:
		return "STOP"
	case syscall.SIGCONT:
		return "CONT"
	}
	return fmt.Sprint(int(sig))
}

// kill SIGKILLs the peer and waits for the process to be gone.
func (p *peer) kill() {
	p.t.Helper()
	p.signal(syscall.SIGKILL)
	p.waitExit(10 * time.Second)
}

func (p *peer) waitExit(timeout time.Duration) error {
	p.t.Helper()
	select {
	case <-p.exited:
		return p.err
	case <-time.After(timeout):
		p.t.Fatalf("%s did not exit within %v:\n%s", p.label, timeout, p.output())
	}
	return nil
}

// ------------------------------------------------------------------------------------------------ live subscriber

// liveSub polls a subscription on its own goroutine, verifying the e2e sequence of every image independently and
// recording available/unavailable images.
type liveSub struct {
	t           *testing.T
	sub         *aeron.Subscription
	mu          sync.Mutex
	checkers    map[int32]*e2e.SequenceChecker
	available   map[int32]int
	unavailable map[int32]int
	stop        chan struct{}
	done        chan struct{}
}

func newLiveSub(t *testing.T, a *aeron.Aeron, channel string, stream int32, dests ...string) *liveSub {
	t.Helper()
	s := &liveSub{t: t, checkers: map[int32]*e2e.SequenceChecker{}, available: map[int32]int{},
		unavailable: map[int32]int{}, stop: make(chan struct{}), done: make(chan struct{})}
	sub, err := a.AddSubscriptionWithHandlers(channel, stream,
		func(img aeron.Image) { s.mu.Lock(); s.available[img.SessionID()]++; s.mu.Unlock() },
		func(img aeron.Image) { s.mu.Lock(); s.unavailable[img.SessionID()]++; s.mu.Unlock() })
	require.NoError(t, err)
	for _, d := range dests {
		require.NoError(t, sub.AddDestination(d))
	}
	s.sub = sub
	assembler := aeron.NewFragmentAssembler(func(buf *ab.Buffer, offset, length int32, header *logbuffer.Header) {
		msg := buf.GetBytesArray(offset, length)
		s.mu.Lock()
		c := s.checkers[header.SessionId()]
		if c == nil {
			c = e2e.NewSequenceChecker(0)
			s.checkers[header.SessionId()] = c
		}
		c.OnMessage(msg)
		s.mu.Unlock()
	}, aeron.DefaultFragmentAssemblyBufferLength)
	go func() {
		defer close(s.done)
		for {
			select {
			case <-s.stop:
				return
			default:
			}
			if sub.Poll(assembler.OnFragment, 20) == 0 {
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()
	t.Cleanup(s.close)
	return s
}

func (s *liveSub) close() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	<-s.done
}

// checker returns a copy of the session's checker state (zero value if nothing was received yet).
func (s *liveSub) checker(sessionID int32) e2e.SequenceChecker {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.checkers[sessionID]; c != nil {
		return *c
	}
	return e2e.SequenceChecker{}
}

func (s *liveSub) counts(sessionID int32) (available, unavailable int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.available[sessionID], s.unavailable[sessionID]
}

// awaitReceived waits until seq of sessionID has been received, failing on any gap, duplicate or corruption.
func (s *liveSub) awaitReceived(sessionID int32, seq int64, timeout time.Duration) {
	s.t.Helper()
	awaitTrue(s.t, fmt.Sprintf("session %d to deliver seq %d", sessionID, seq), timeout, func() bool {
		c := s.checker(sessionID)
		require.NoError(s.t, ptr(c).Err(), "session %d: %s", sessionID, c.String())
		return c.Next > seq
	})
}

func (s *liveSub) awaitUnavailable(sessionID int32, timeout time.Duration) time.Duration {
	s.t.Helper()
	return awaitTrue(s.t, fmt.Sprintf("unavailable image for session %d", sessionID), timeout, func() bool {
		_, u := s.counts(sessionID)
		return u > 0
	})
}

// awaitPublisherGone waits for the driver to drop a killed publisher's client and publication, and for its image to
// go unavailable. Java 1.53.3 never unblocks a publication whose publisher died part way through a fragmented
// message (see publisher-crash awaitUnavailableOrStuck): the driver keeps the publication and the image stays, stuck,
// until the subscription closes. For a fragmented case that state is accepted once the client heartbeat is gone;
// the caller checks the publication is freed after closing the subscription.
func awaitPublisherGone(t *testing.T, a *aeron.Aeron, s *liveSub, c pubKillCase, session int32, clientID int64) {
	t.Helper()
	if !c.fragmented() {
		reclaimed := awaitReclaimed(t, a, clientID, alias(c.pubChan))
		unavailable := s.awaitUnavailable(session, reclaimTO)
		t.Logf("session %d reclaimed after %v, unavailable after a further %v", session, reclaimed, unavailable)
		return
	}
	reclaimed := awaitTrue(t, fmt.Sprintf("driver to time out client %d", clientID), reclaimTO, func() bool {
		for _, l := range clientCounters(a, clientID) {
			if strings.HasPrefix(l, "client-heartbeat") {
				return false
			}
		}
		return true
	})
	for start := time.Now(); time.Since(start) < reclaimTO; time.Sleep(5 * time.Millisecond) {
		if _, u := s.counts(session); u > 0 {
			t.Logf("session %d reclaimed after %v, unavailable after a further %v", session, reclaimed, time.Since(start))
			return
		}
	}
	require.NotEmpty(t, counterLabels(a, alias(c.pubChan)), "image of session %d stuck after its publication was freed",
		session)
	t.Logf("session %d died mid fragmented message: client reclaimed after %v, image stuck, as in Java", session,
		reclaimed)
}
