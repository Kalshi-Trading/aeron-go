package publishercrash

import (
	"bufio"
	"encoding/json"
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
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

const (
	advDir      = "/dev/shm/aeron-adv-publisher-crash"
	advDriver   = "adv-publisher-crash-driver"
	composeFile = "../../../docker/adversarial-publisher-crash.compose.yml"
	project     = "adv-publisher-crash"
	javaImage   = "aeron-go-adv-publisher-crash-java:1.53.3"
	netemImage  = "aeron-go-adv-netem"

	// clientLiveness, imageLiveness and unblockTimeout match the driver properties in the compose file.
	clientLiveness = 2 * time.Second
	unblockTimeout = 4 * time.Second
)

var (
	crashClientBin string
	setupOnce      sync.Once
	setupErr       error
	javaSeq        int
)

func TestMain(m *testing.M) {
	code := m.Run()
	if crashClientBin != "" {
		_ = os.RemoveAll(filepath.Dir(crashClientBin))
	}
	if enabled() {
		_ = compose("down", "--remove-orphans", "-t", "1")
	}
	os.Exit(code)
}

func enabled() bool { return e2e.Enabled() && os.Getenv("AERON_ADVERSARIAL") == "1" }

// requireAdv skips unless enabled, builds the helpers once and gives the test a freshly created driver. The
// driver and every helper process the test started are gone again when the test ends.
func requireAdv(t *testing.T) {
	t.Helper()
	if !enabled() {
		t.Skip("set AERON_E2E=1 AERON_ADVERSARIAL=1 (runs docker/adversarial-publisher-crash.compose.yml)")
	}
	setupOnce.Do(func() {
		dir, err := os.MkdirTemp("", "crashclient")
		if err != nil {
			setupErr = err
			return
		}
		crashClientBin = filepath.Join(dir, "crashclient")
		if out, err := exec.Command("go", "build", "-o", crashClientBin, "./cmd/crashclient").CombinedOutput(); err != nil {
			setupErr = fmt.Errorf("go build crashclient: %v\n%s", err, out)
			return
		}
		if err := compose("--profile", "tools", "build", "-q"); err != nil {
			setupErr = err
		}
	})
	require.NoError(t, setupErr)
	restartDriver(t)
	t.Cleanup(func() {
		_ = exec.Command("docker", "kill", "-s", "CONT", advDriver).Run()
		_ = compose("rm", "-sf", "driver")
	})
}

func compose(args ...string) error {
	cmd := exec.Command("docker", append([]string{"compose", "-p", project, "-f", composeFile}, args...)...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("GID=%d", os.Getgid()), fmt.Sprintf("UID=%d", os.Getuid()))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose %v: %v\n%s", args, err, out)
	}
	return nil
}

func docker(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	require.NoError(t, err, "docker %v: %s", args, out)
}

func restartDriver(t *testing.T) {
	t.Helper()
	_ = exec.Command("docker", "kill", "-s", "CONT", advDriver).Run()
	require.NoError(t, compose("up", "-d", "--force-recreate", "--wait", "driver"))
}

func connect(t *testing.T) *aeron.Aeron {
	return connectCtx(t, aeron.NewContext())
}

func connectCtx(t *testing.T, ctx *aeron.Context) *aeron.Aeron {
	t.Helper()
	var a *aeron.Aeron
	var err error
	deadline := time.Now().Add(15 * time.Second)
	for {
		a, err = aeron.Connect(ctx.AeronDir(advDir).MediaDriverTimeout(5 * time.Second))
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
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

// --- child processes ---

type event map[string]interface{}

func (e event) int(key string) int64 {
	v, ok := e[key].(float64)
	if !ok {
		panic(fmt.Sprintf("event %v has no number %q", e, key))
	}
	return int64(v)
}

func (e event) str(key string) string { s, _ := e[key].(string); return s }

// peer is a Go or Java client process started by a test.
type peer struct {
	t      *testing.T
	name   string
	java   bool
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	mu     sync.Mutex
	events []event
	exited chan struct{}
	err    error
}

type lang string

const (
	goLang   lang = "go"
	javaLang lang = "java"
)

// startPeer runs crashclient (go) or CrashTool (java) with the given mode and key=value args.
func startPeer(t *testing.T, l lang, mode string, kv ...string) *peer {
	t.Helper()
	args := append([]string{mode, "dir=" + advDir}, kv...)
	p := &peer{t: t, java: l == javaLang, exited: make(chan struct{})}
	if p.java {
		javaSeq++
		p.name = fmt.Sprintf("adv-publisher-crash-java-%d-%d", os.Getpid(), javaSeq)
		dargs := []string{"run", "--rm", "-i", "--name", p.name, "--network", "host", "--ipc", "host",
			"-u", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), "-v", "/dev/shm:/dev/shm", javaImage}
		p.cmd = exec.Command("docker", append(dargs, args...)...)
	} else {
		p.name = "crashclient " + mode
		p.cmd = exec.Command(crashClientBin, args...)
	}
	stdout, err := p.cmd.StdoutPipe()
	require.NoError(t, err)
	p.cmd.Stderr = &prefixWriter{t: t, prefix: p.name}
	p.stdin, err = p.cmd.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, p.cmd.Start())
	go func() {
		s := bufio.NewScanner(stdout)
		s.Buffer(make([]byte, 64*1024), 1<<20)
		for s.Scan() {
			var e event
			if json.Unmarshal(s.Bytes(), &e) != nil {
				continue
			}
			p.mu.Lock()
			p.events = append(p.events, e)
			p.mu.Unlock()
		}
		p.err = p.cmd.Wait()
		close(p.exited)
	}()
	t.Cleanup(func() {
		p.kill()
		select {
		case <-p.exited:
		case <-time.After(20 * time.Second):
			t.Errorf("%s did not exit", p.name)
		}
	})
	return p
}

type prefixWriter struct {
	t      *testing.T
	prefix string
}

func (w *prefixWriter) Write(b []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
		w.t.Logf("%s: %s", w.prefix, line)
	}
	return len(b), nil
}

func (p *peer) signal(sig syscall.Signal) {
	p.t.Helper()
	if p.java {
		name := map[syscall.Signal]string{syscall.SIGKILL: "KILL", syscall.SIGSTOP: "STOP", syscall.SIGCONT: "CONT"}[sig]
		_ = exec.Command("docker", "kill", "-s", name, p.name).Run()
		return
	}
	_ = p.cmd.Process.Signal(sig)
}

// kill SIGKILLs the process (a no-op once it has exited).
func (p *peer) kill() {
	select {
	case <-p.exited:
		return
	default:
	}
	p.signal(syscall.SIGKILL)
}

// killAndWait SIGKILLs the process and waits until it is gone.
func (p *peer) killAndWait() {
	p.t.Helper()
	p.kill()
	select {
	case <-p.exited:
	case <-time.After(20 * time.Second):
		p.t.Fatalf("%s did not die", p.name)
	}
}

func (p *peer) send(cmd string) {
	_, _ = io.WriteString(p.stdin, cmd+"\n")
}

func (p *peer) snapshot() []event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]event(nil), p.events...)
}

// await returns the first event named name for which match (nil matches all) is true.
func (p *peer) await(name string, timeout time.Duration, match func(event) bool) event {
	p.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, e := range p.snapshot() {
			if e["event"] == name && (match == nil || match(e)) {
				return e
			}
		}
		for _, e := range p.snapshot() {
			if e["event"] == "fatal" && name != "fatal" && name != "terminated" {
				p.t.Fatalf("%s failed waiting for %s: %v", p.name, name, e)
			}
		}
		if time.Now().After(deadline) {
			p.t.Fatalf("%s: timed out after %v waiting for %s; events: %v", p.name, timeout, name, tail(p.snapshot()))
		}
		select {
		case <-p.exited:
			if name != "fatal" && name != "terminated" {
				for _, e := range p.snapshot() {
					if e["event"] == name && (match == nil || match(e)) {
						return e
					}
				}
				p.t.Fatalf("%s exited (%v) waiting for %s; events: %v", p.name, p.err, name, tail(p.snapshot()))
			}
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// last returns the latest event named name, or nil.
func (p *peer) last(name string) event {
	ev := p.snapshot()
	for i := len(ev) - 1; i >= 0; i-- {
		if ev[i]["event"] == name {
			return ev[i]
		}
	}
	return nil
}

func tail(ev []event) []event {
	if len(ev) > 10 {
		return ev[len(ev)-10:]
	}
	return ev
}

// report asks a subscriber peer for its per-session stats.
func (p *peer) report() (map[int32]event, int64) {
	p.t.Helper()
	before := len(p.snapshot())
	p.send("report")
	end := p.await("reportEnd", 10*time.Second, nil)
	sessions := map[int32]event{}
	var found bool
	for i, e := range p.snapshot() {
		if i < before {
			continue
		}
		if e["event"] == "session" {
			sessions[int32(e.int("sessionId"))] = e
		}
		if e["event"] == "reportEnd" {
			found = true
			sessions2 := sessions
			sessions = sessions2
		}
	}
	require.True(p.t, found)
	return sessions, end.int("images")
}

// lastPublished is the number of messages a publisher peer reported as offered (its last progress/holding/done).
func (p *peer) lastPublished() int64 {
	var next int64
	for _, e := range p.snapshot() {
		switch e["event"] {
		case "progress", "done":
			next = e.int("next")
		case "holding":
			next = e.int("seq")
		}
	}
	return next
}

// --- in-process consumers ---

type consumerKind string

const (
	pollFA        consumerKind = "Poll+FragmentAssembler"
	controlledCFA consumerKind = "ControlledPoll+ControlledFragmentAssembler"
	imageIFA      consumerKind = "Image.Poll+ImageFragmentAssembler"
	boundedIFA    consumerKind = "Image.BoundedPoll+ImageFragmentAssembler"
	boundedCtlICF consumerKind = "Image.BoundedControlledPoll+ImageControlledFragmentAssembler"
)

var allConsumers = []consumerKind{pollFA, controlledCFA, imageIFA, boundedIFA, boundedCtlICF}

type sessionStats struct {
	first, next, received, gaps, duplicates, corrupt int64
}

// consumer is an in-process subscription polled by the test goroutine. Image callbacks run on the conductor
// goroutine and are recorded under mu.
type consumer struct {
	t     *testing.T
	kind  consumerKind
	sub   *aeron.Subscription
	fa    *aeron.FragmentAssembler
	cfa   *aeron.ControlledFragmentAssembler
	mu    sync.Mutex
	avail []aeron.Image
	gone  []aeron.Image
	// gonePositions and goneEOS are the image position and IsEndOfStream observed in the unavailable callback.
	gonePositions map[int32]int64
	goneEOS       map[int32]bool
	assemblers    map[int64]interface{}
	stats         map[int32]*sessionStats
	maxLength     int32
}

func newConsumer(t *testing.T, a *aeron.Aeron, kind consumerKind, channel string, stream int32,
	dests ...string) *consumer {
	t.Helper()
	c := &consumer{t: t, kind: kind, stats: map[int32]*sessionStats{}, assemblers: map[int64]interface{}{},
		gonePositions: map[int32]int64{}, goneEOS: map[int32]bool{}}
	c.fa = aeron.NewFragmentAssembler(c.onMessage, aeron.DefaultFragmentAssemblyBufferLength)
	c.cfa = aeron.NewControlledFragmentAssembler(func(b *ab.Buffer, o, l int32, h *logbuffer.Header) term.ControlledPollAction {
		c.onMessage(b, o, l, h)
		return term.ControlledPollActionContinue
	}, aeron.DefaultFragmentAssemblyBufferLength)
	sub, err := a.AddSubscriptionWithHandlers(channel, stream, func(img aeron.Image) {
		c.mu.Lock()
		c.avail = append(c.avail, img)
		c.mu.Unlock()
	}, func(img aeron.Image) {
		// Java: the assemblers free the session buffer from the unavailable image handler.
		c.fa.OnUnavailableImage(img)
		c.cfa.OnUnavailableImage(img)
		c.mu.Lock()
		c.gone = append(c.gone, img)
		c.gonePositions[img.SessionID()] = img.Position()
		c.goneEOS[img.SessionID()] = img.IsEndOfStream()
		c.mu.Unlock()
	})
	require.NoError(t, err)
	for _, d := range dests {
		// The Java driver closes a released subscription's receive endpoint asynchronously on its receiver thread,
		// so re-binding the same destination straight after a Close can briefly fail with "Address already in use".
		deadline := time.Now().Add(5 * time.Second)
		for {
			err := sub.AddDestination(d)
			if err == nil {
				break
			}
			if !strings.Contains(err.Error(), "Address already in use") || time.Now().After(deadline) {
				require.NoError(t, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.sub = sub
	t.Cleanup(func() { _ = sub.Close() })
	return c
}

func (c *consumer) onMessage(buffer *ab.Buffer, offset, length int32, header *logbuffer.Header) {
	s := c.stats[header.SessionID()]
	if s == nil {
		s = &sessionStats{first: -1, next: -1}
		c.stats[header.SessionID()] = s
	}
	s.received++
	if length > c.maxLength {
		c.maxLength = length
	}
	seq, err := e2e.DecodeMessage(buffer.GetBytesArray(offset, length))
	switch {
	case err != nil:
		s.corrupt++
		c.t.Errorf("%s: corrupt message session=%d length=%d: %v", c.kind, header.SessionID(), length, err)
	case s.first < 0:
		s.first, s.next = seq, seq+1
	case seq == s.next:
		s.next++
	case seq > s.next:
		s.gaps++
		c.t.Errorf("%s: gap session=%d expected=%d got=%d", c.kind, header.SessionID(), s.next, seq)
		s.next = seq + 1
	default:
		s.duplicates++
		c.t.Errorf("%s: duplicate session=%d expected=%d got=%d", c.kind, header.SessionID(), s.next, seq)
	}
}

// poll does one poll of the subscription in the consumer's style.
func (c *consumer) poll() int {
	switch c.kind {
	case pollFA:
		return c.sub.Poll(c.fa.OnFragment, 16)
	case controlledCFA:
		return c.sub.ControlledPoll(c.cfa.OnFragment, 16)
	}
	c.mu.Lock()
	images := append([]aeron.Image(nil), c.avail...)
	c.mu.Unlock()
	n := 0
	for _, img := range images {
		if img.IsClosed() {
			delete(c.assemblers, img.CorrelationID())
			continue
		}
		asm := c.assemblers[img.CorrelationID()]
		if asm == nil {
			if c.kind == boundedCtlICF {
				asm = aeron.NewImageControlledFragmentAssembler(func(b *ab.Buffer, o, l int32, h *logbuffer.Header) term.ControlledPollAction {
					c.onMessage(b, o, l, h)
					return term.ControlledPollActionContinue
				}, aeron.DefaultFragmentAssemblyBufferLength)
			} else {
				asm = aeron.NewImageFragmentAssembler(c.onMessage, aeron.DefaultFragmentAssemblyBufferLength)
			}
			c.assemblers[img.CorrelationID()] = asm
		}
		// bounded polls are limited to one MTU-ish window past the current position, so a fragmented message is
		// assembled across many polls.
		limit := img.Position() + 4096
		switch c.kind {
		case imageIFA:
			n += img.Poll(asm.(*aeron.ImageFragmentAssembler).OnFragment, 16)
		case boundedIFA:
			n += img.BoundedPoll(asm.(*aeron.ImageFragmentAssembler).OnFragment, limit, 16)
		case boundedCtlICF:
			n += img.BoundedControlledPoll(asm.(*aeron.ImageControlledFragmentAssembler).OnFragment, limit, 16)
		}
	}
	return n
}

// pollUntil polls until cond holds or fails the test after timeout.
func (c *consumer) pollUntil(what string, timeout time.Duration, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			c.t.Fatalf("%s: timed out after %v waiting for %s; stats=%v", c.kind, timeout, what, c.statsString())
		}
		if c.poll() == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
}

// pollFor polls for d regardless.
func (c *consumer) pollFor(d time.Duration) {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if c.poll() == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
}

func (c *consumer) session(id int32) sessionStats {
	if s := c.stats[id]; s != nil {
		return *s
	}
	return sessionStats{first: -1, next: -1}
}

func (c *consumer) statsString() string {
	var b strings.Builder
	for id, s := range c.stats {
		fmt.Fprintf(&b, "[session %d first=%d next=%d received=%d gaps=%d dups=%d corrupt=%d] ", id, s.first, s.next,
			s.received, s.gaps, s.duplicates, s.corrupt)
	}
	return b.String()
}

func (c *consumer) isGone(sessionID int32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, img := range c.gone {
		if img.SessionID() == sessionID {
			return true
		}
	}
	return false
}

func (c *consumer) hasImage(sessionID int32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, img := range c.avail {
		if img.SessionID() == sessionID {
			return true
		}
	}
	return false
}

func (c *consumer) imageFor(sessionID int32) aeron.Image {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, img := range c.avail {
		if img.SessionID() == sessionID {
			return img
		}
	}
	return nil
}

// requireClean asserts no corruption, gaps or duplicates in any session.
func (c *consumer) requireClean() {
	c.t.Helper()
	for id, s := range c.stats {
		require.Zero(c.t, s.corrupt+s.gaps+s.duplicates, "%s session %d: %s", c.kind, id, c.statsString())
	}
}

// --- driver state and leak checks ---

// sessionCounters lists the driver counters (publisher/sender/subscriber positions etc.) of a session id on a
// stream: their labels contain " <sessionId> <streamId> ".
func sessionCounters(a *aeron.Aeron, sessionID, stream int32) []string {
	var labels []string
	needle := fmt.Sprintf(" %d %d ", sessionID, stream)
	a.CounterReader().ForEach(func(_ int64, _ int32, label string) {
		if strings.Contains(label, needle) {
			labels = append(labels, label)
		}
	})
	return labels
}

// clientCounters lists the allocated driver counters of client id (its heartbeat).
func clientCounters(a *aeron.Aeron, clientID int64) []string {
	var labels []string
	needle := fmt.Sprintf("id=%d", clientID)
	a.CounterReader().ForEach(func(_ int64, _ int32, label string) {
		if strings.HasPrefix(label, "client-heartbeat") && strings.Contains(label, needle+" ") ||
			strings.HasSuffix(label, needle) && strings.HasPrefix(label, "client-heartbeat") {
			labels = append(labels, label)
		}
	})
	return labels
}

func systemCounter(a *aeron.Aeron, label string) int64 {
	var v int64 = -1
	a.CounterReader().ForEach(func(value int64, _ int32, l string) {
		if l == label {
			v = value
		}
	})
	return v
}

// awaitDriverForgets waits until the driver has freed every counter of the session and client.
func awaitDriverForgets(t *testing.T, a *aeron.Aeron, sessionID, stream int32, clientID int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		s, c := sessionCounters(a, sessionID, stream), clientCounters(a, clientID)
		if len(s) == 0 && len(c) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("driver still has counters of dead session %d / client %d after %v: %v %v", sessionID, clientID,
				timeout, s, c)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type resources struct{ goroutines, fds int }

func snapshotResources() resources {
	return resources{goroutines: runtime.NumGoroutine(), fds: openFDs()}
}

func openFDs() int {
	entries, _ := os.ReadDir("/proc/self/fd")
	return len(entries)
}

func mappedFiles(dir string) int {
	maps, _ := os.ReadFile("/proc/self/maps")
	n := 0
	for _, line := range strings.Split(string(maps), "\n") {
		if strings.Contains(line, dir+"/") {
			n++
		}
	}
	return n
}

// checkLeaks registers a cleanup, run after the test's clients are closed (cleanups run LIFO, so call it before
// connecting), that fails the test if goroutines, fds or mappings of the Aeron dir outlive the clients.
func checkLeaks(t *testing.T) {
	t.Helper()
	base := snapshotResources()
	t.Cleanup(func() {
		deadline := time.Now().Add(10 * time.Second)
		for {
			now := snapshotResources()
			maps := mappedFiles(advDir)
			if now.goroutines <= base.goroutines && now.fds <= base.fds && maps == 0 {
				return
			}
			if time.Now().After(deadline) {
				buf := make([]byte, 1<<20)
				n := runtime.Stack(buf, true)
				t.Errorf("leak after close: goroutines %d (base %d), fds %d (base %d), mappings of %s: %d\n%s",
					now.goroutines, base.goroutines, now.fds, base.fds, advDir, maps, buf[:n])
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
}

// lastImageFor is the latest image of sessionID that became available.
func (c *consumer) lastImageFor(sessionID int32) aeron.Image {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.avail) - 1; i >= 0; i-- {
		if c.avail[i].SessionID() == sessionID {
			return c.avail[i]
		}
	}
	return nil
}

func (c *consumer) imagesFor(sessionID int32) (available, gone int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, img := range c.avail {
		if img.SessionID() == sessionID {
			available++
		}
	}
	for _, img := range c.gone {
		if img.SessionID() == sessionID {
			gone++
		}
	}
	return available, gone
}

// awaitUnavailableOrStuck waits for the image of a killed publisher to go unavailable and reports whether it was
// stuck instead. Java 1.53.3: a publisher killed part way through writing a fragmented message leaves a zeroed frame
// inside the reserved tail with nothing written after it, and LogBufferUnblocker only pads such a frame once a later
// frame is written or the term ends, so the draining publication is never unblocked and its image stays, stuck at
// that frame, until the subscription closes. For a stuck image this checks it really doesn't move, closes the
// subscription and waits for the driver to free the publication.
func (c *consumer) awaitUnavailableOrStuck(a *aeron.Aeron, session, stream int32, clientID int64, fragmented bool,
	timeout time.Duration) bool {
	t := c.t
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !c.isGone(session) && time.Now().Before(deadline) {
		if c.poll() == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
	if c.isGone(session) {
		return false
	}
	require.True(t, fragmented, "image of an unfragmented publisher never went unavailable; %s", c.statsString())
	img := c.imageFor(session)
	require.NotNil(t, img)
	pos := img.Position()
	c.pollFor(2 * time.Second)
	require.Equal(t, pos, img.Position(), "image neither went unavailable nor is stuck")
	require.False(t, c.isGone(session))
	t.Logf("publisher died mid fragmented message: image stuck at %d, as in Java; closing the subscription", pos)
	require.NoError(t, c.sub.Close())
	awaitDriverForgets(t, a, session, stream, clientID, 20*time.Second)
	return true
}
