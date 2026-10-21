package interop

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	aatomic "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/stretchr/testify/require"
)

// Environment:
//
//	AERON_E2E=1                 enable the interop tests (needs the docker Java 1.53.3 archiving driver running)
//	AERON_DIR                   aeron dir shared with the driver (default /dev/shm/aeron-compose)
//	AERON_INTEROP_IMAGE         docker image of the Java tool (default aeron-go-interop-java:1.53.3)
//	AERON_INTEROP_JAVA_CP       if set, run the Java tool with a local `java -cp <cp>` instead of docker
//	AERON_INTEROP_MCAST_IFACE   interface for multicast tests (default localhost; "off" skips multicast)
const (
	defaultAeronDir = "/dev/shm/aeron-compose"
	defaultImage    = "aeron-go-interop-java:1.53.3"
	defaultTimeout  = 30 * time.Second
)

func requireE2E(t *testing.T) {
	t.Helper()
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 (and start docker/docker-compose.yml archiving-driver) to run interop tests")
	}
}

func aeronDir() string {
	if d := os.Getenv("AERON_DIR"); d != "" {
		return d
	}
	return defaultAeronDir
}

var portSeq int32 = 24600
var streamSeq int32 = 7000

func nextPort() int     { return int(atomic.AddInt32(&portSeq, 1)) }
func nextStream() int32 { return atomic.AddInt32(&streamSeq, 1) }

func goContext() *aeron.Context {
	return aeron.NewContext().AeronDir(aeronDir()).MediaDriverTimeout(10 * time.Second)
}

func connectGo(t *testing.T) *aeron.Aeron {
	t.Helper()
	a, err := aeron.Connect(goContext())
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// ---------------------------------------------------------------------------------------------------- java process

type event map[string]any

func (e event) name() string { s, _ := e["event"].(string); return s }
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
func (e event) str(k string) string { s, _ := e[k].(string); return s }
func (e event) bool(k string) bool  { b, _ := e[k].(bool); return b }

func (e event) result() Result {
	return Result{
		Count:      e.int("count"),
		Errors:     e.int("errors"),
		FirstError: e.str("firstError"),
		SHA256:     e.str("sha256"),
		Bytes:      e.int("bytes"),
		Position:   e.int("position"),
		SessionID:  int32(e.int("sessionId")),
		TimedOut:   e.bool("timedOut"),
	}
}

type javaProc struct {
	t      *testing.T
	label  string
	name   string
	docker bool
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stderr bytes.Buffer

	mu       sync.Mutex
	events   []event
	consumed []bool
	notify   chan struct{}
	exited   chan struct{}
}

var javaSeq int32

// startJava runs `InteropTool <mode> args...` (see docker/interop) and streams its JSON events.
func startJava(t *testing.T, mode string, args ...string) *javaProc {
	t.Helper()
	p := &javaProc{
		t:      t,
		label:  fmt.Sprintf("java-%s#%d", mode, atomic.AddInt32(&javaSeq, 1)),
		notify: make(chan struct{}, 1),
		exited: make(chan struct{}),
	}
	toolArgs := append([]string{mode}, args...)
	props := "-Daeron.dir=" + aeronDir()
	if cp := os.Getenv("AERON_INTEROP_JAVA_CP"); cp != "" {
		cmdArgs := append([]string{
			"--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
			"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
			props, "-cp", cp, "io.aeron.interop.InteropTool"}, toolArgs...)
		p.cmd = exec.Command("java", cmdArgs...)
	} else {
		image := os.Getenv("AERON_INTEROP_IMAGE")
		if image == "" {
			image = defaultImage
		}
		p.docker = true
		p.name = fmt.Sprintf("aeron-interop-%d-%d", os.Getpid(), atomic.LoadInt32(&javaSeq))
		cmdArgs := append([]string{"run", "--rm", "-i", "--name", p.name,
			"--network", "host", "--ipc", "host", "-v", "/dev/shm:/dev/shm",
			"-u", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
			"-e", "AERON_PROPS=" + props, image}, toolArgs...)
		p.cmd = exec.Command("docker", cmdArgs...)
	}
	p.cmd.Stderr = &lockedWriter{w: &p.stderr, mu: &p.mu}
	stdout, err := p.cmd.StdoutPipe()
	require.NoError(t, err)
	p.stdin, err = p.cmd.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, p.cmd.Start())
	t.Logf("%s: started %s", p.label, strings.Join(toolArgs, " "))

	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 1<<20), 1<<20)
		for scanner.Scan() {
			line := scanner.Text()
			dec := json.NewDecoder(strings.NewReader(line))
			dec.UseNumber()
			var e event
			if dec.Decode(&e) != nil || e.name() == "" {
				p.mu.Lock()
				p.stderr.WriteString(line + "\n")
				p.mu.Unlock()
				continue
			}
			p.mu.Lock()
			p.events = append(p.events, e)
			p.consumed = append(p.consumed, false)
			p.mu.Unlock()
			select {
			case p.notify <- struct{}{}:
			default:
			}
		}
		_ = p.cmd.Wait()
		close(p.exited)
	}()
	t.Cleanup(p.stop)
	return p
}

type lockedWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}

func (p *javaProc) take(name string) (event, bool, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, e := range p.events {
		if p.consumed[i] {
			continue
		}
		if e.name() == "error" {
			return nil, false, e.str("message")
		}
		if e.name() == name {
			p.consumed[i] = true
			return e, true, ""
		}
	}
	return nil, false, ""
}

func (p *javaProc) stderrString() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stderr.String()
}

// waitFor returns the next not yet consumed event with the given name.
func (p *javaProc) waitFor(name string, timeout time.Duration) event {
	p.t.Helper()
	e, err := p.tryWaitFor(name, timeout)
	if err != nil {
		p.t.Fatalf("%s: %v\nstderr:\n%s", p.label, err, p.stderrString())
	}
	return e
}

func (p *javaProc) tryWaitFor(name string, timeout time.Duration) (event, error) {
	deadline := time.After(timeout)
	for {
		e, ok, errMsg := p.take(name)
		if errMsg != "" {
			return nil, fmt.Errorf("java error while waiting for %q: %s", name, errMsg)
		}
		if ok {
			return e, nil
		}
		select {
		case <-p.notify:
		case <-p.exited:
			if e, ok, _ := p.take(name); ok {
				return e, nil
			}
			return nil, fmt.Errorf("exited before %q event", name)
		case <-deadline:
			return nil, fmt.Errorf("timed out after %v waiting for %q", timeout, name)
		}
	}
}

func (p *javaProc) send(line string) {
	_, _ = io.WriteString(p.stdin, line+"\n")
}

func (p *javaProc) stop() {
	_ = p.stdin.Close()
	select {
	case <-p.exited:
	case <-time.After(10 * time.Second):
		if p.docker {
			_ = exec.Command("docker", "kill", p.name).Run()
		} else {
			_ = p.cmd.Process.Kill()
		}
		<-p.exited
	}
}

// ------------------------------------------------------------------------------------------------------- go helpers

type msgOpts struct {
	from     int64 // first sequence number to publish (receivers verify from first)
	first    int64 // first sequence number receivers expect (count messages are received from it)
	count    int64
	sizes    Sizes
	claim    bool
	reserved bool
	delay    time.Duration // per message (slow publisher / receiver)
	timeout  time.Duration
}

func (o msgOpts) timeoutOrDefault() time.Duration {
	if o.timeout == 0 {
		return defaultTimeout
	}
	return o.timeout
}

// javaArgs renders the message options as InteropTool arguments.
func (o msgOpts) javaArgs() []string {
	args := []string{fmt.Sprintf("count=%d", o.count), "sizes=" + o.sizes.String(),
		fmt.Sprintf("reserved=%t", o.reserved), fmt.Sprintf("timeoutMs=%d", o.timeoutOrDefault().Milliseconds())}
	if o.first != 0 {
		args = append(args, fmt.Sprintf("first=%d", o.first))
	}
	if o.claim {
		args = append(args, "mode=claim")
	}
	return args
}

func awaitConnected(pub aeron.Publisher, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for !pub.IsConnected() {
		if time.Now().After(deadline) {
			return fmt.Errorf("publication %s/%d not connected after %v", pub.Channel(), pub.StreamID(), timeout)
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

func awaitSubConnected(sub *aeron.Subscription, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for !sub.IsConnected() {
		if time.Now().After(deadline) {
			return fmt.Errorf("subscription %s/%d has no image after %v", sub.Channel(), sub.StreamID(), timeout)
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

// goPublish offers (or claims) messages [0, count) and returns the publication position afterwards.
func goPublish(pub aeron.Publisher, o msgOpts) (int64, error) {
	buf := aatomic.MakeBuffer(make([]byte, o.sizes.Max()))
	var claim logbuffer.Claim
	deadline := time.Now().Add(o.timeoutOrDefault())
	for i := o.from; i < o.count; i++ {
		length := o.sizes.Length(i)
		rv := ReservedValue(i)
		var supplier term.ReservedValueSupplier
		if o.reserved {
			supplier = func(*aatomic.Buffer, int32, int32) int64 { return rv }
		}
		if !o.claim {
			FillBuffer(buf, 0, i, length)
		}
		for {
			var result int64
			if o.claim {
				result = pub.TryClaim(length, &claim)
				if result > 0 {
					FillBuffer(claim.Buffer(), claim.Offset(), i, length)
					if o.reserved {
						claim.SetReservedValue(rv)
					}
					claim.Commit()
				}
			} else {
				result = pub.Offer(buf, 0, length, supplier)
			}
			if result > 0 {
				break
			}
			if result == aeron.PublicationClosed || result == aeron.MaxPositionExceeded {
				return 0, fmt.Errorf("offer of message %d failed: %d", i, result)
			}
			if time.Now().After(deadline) {
				return 0, fmt.Errorf("offer of message %d timed out: last result %d", i, result)
			}
		}
		if o.delay > 0 {
			time.Sleep(o.delay)
		}
	}
	return pub.Position(), nil
}

type pollFunc func(handler term.FragmentHandler, fragmentLimit int) int

// goReceive polls until count messages arrived (or timeout) and returns what was received.
func goReceive(poll pollFunc, o msgOpts) Result {
	v := NewVerifier(o.sizes, o.reserved)
	v.First = o.first
	handler := aeron.NewFragmentAssembler(v.OnFragment, 4096).OnFragment
	limit := 10
	if o.delay > 0 {
		limit = 1
	}
	deadline := time.Now().Add(o.timeoutOrDefault())
	for v.Count() < o.count {
		if poll(handler, limit) > 0 && o.delay > 0 {
			time.Sleep(o.delay)
		}
		if time.Now().After(deadline) {
			return v.Result(true)
		}
	}
	return v.Result(false)
}

func requireResult(t *testing.T, who string, r Result, o msgOpts, position int64) {
	t.Helper()
	require.Falsef(t, r.TimedOut, "%s timed out: received %d/%d (first error %q)", who, r.Count, o.count, r.FirstError)
	require.Equalf(t, o.count, r.Count, "%s message count", who)
	require.Zerof(t, r.Errors, "%s errors, first: %s", who, r.FirstError)
	require.Equalf(t, ExpectedDigestFrom(o.sizes, o.first, o.count), r.SHA256, "%s digest", who)
	if position >= 0 {
		require.Equalf(t, position, r.Position, "%s final position", who)
	}
}

// withParams appends URI params ("k=v|k2=v2") to a channel.
func withParams(channel, params string) string {
	if params == "" {
		return channel
	}
	if strings.Contains(channel, "?") {
		return channel + "|" + params
	}
	return channel + "?" + params
}
