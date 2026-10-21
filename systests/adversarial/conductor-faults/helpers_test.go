package conductorfaults

import (
	"bufio"
	"encoding/binary"
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
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/stretchr/testify/require"
)

const (
	aeronDir    = "/dev/shm/aeron-adv-conductor-faults"
	driverName  = "adv-conductor-faults-driver"
	project     = "adv-conductor-faults"
	composeFile = "../../../docker/adversarial-conductor-faults.compose.yml"
	interopImg  = "aeron-go-interop-java:1.53.3"

	basePort   = 27800
	baseStream = 9800
)

var portSeq, streamSeq int32

// nextPort returns a UDP port in 27800-27899.
func nextPort() int { return basePort + int(atomic.AddInt32(&portSeq, 1)-1)%100 }

// nextStream returns a stream id in 9800-9899.
func nextStream() int32 { return baseStream + (atomic.AddInt32(&streamSeq, 1)-1)%100 }

func udp(port int) string { return fmt.Sprintf("aeron:udp?endpoint=localhost:%d", port) }

func gate(t *testing.T) {
	t.Helper()
	if os.Getenv("AERON_E2E") != "1" || os.Getenv("AERON_ADVERSARIAL") != "1" {
		t.Skip("set AERON_E2E=1 AERON_ADVERSARIAL=1 to run the conductor-faults adversarial suite")
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

func docker(args ...string) error {
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %v: %v: %s", args, err, out)
	}
	return nil
}

func driverState() string {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Status}}", driverName).Output()
	if err != nil {
		return "missing"
	}
	return strings.TrimSpace(string(out))
}

// signalDriver sends sig (STOP, CONT, KILL) to the driver JVM (pid 1 of the container).
func signalDriver(t *testing.T, sig string) {
	t.Helper()
	require.NoError(t, docker("kill", "-s", sig, driverName))
}

// startDriver (re)creates the driver if it is not running and waits for a fresh CnC.
func startDriver(t *testing.T) {
	t.Helper()
	if driverState() == "paused" {
		require.NoError(t, docker("unpause", driverName))
	}
	_ = docker("kill", "-s", "CONT", driverName)
	require.NoError(t, compose("up", "-d", "--wait"))
	awaitTrue(t, 30*time.Second, "driver active", func() bool {
		active, _ := aeron.NewContext().AeronDir(aeronDir).IsDriverActive(nil)
		return active
	})
}

// restartDriver kills the driver with SIGKILL and starts a fresh one.
func restartDriver(t *testing.T) {
	t.Helper()
	_ = docker("kill", "-s", "CONT", driverName)
	_ = docker("kill", "-s", "KILL", driverName)
	awaitTrue(t, 20*time.Second, "driver exited", func() bool { return driverState() != "running" })
	startDriver(t)
}

// setup gives every test a running, unstalled driver and resumes/restarts it again at the end.
func setup(t *testing.T) {
	t.Helper()
	gate(t)
	startDriver(t)
	t.Cleanup(func() {
		_ = docker("kill", "-s", "CONT", driverName)
		if driverState() != "running" {
			_ = compose("up", "-d", "--wait")
		}
	})
}

func awaitTrue(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// errorSink collects errors from the client ErrorHandler.
type errorSink struct {
	mu   sync.Mutex
	errs []error
}

func (s *errorSink) handle(err error) {
	s.mu.Lock()
	s.errs = append(s.errs, err)
	s.mu.Unlock()
}

func (s *errorSink) all() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.errs...)
}

// find returns the first collected error matching target (errors.As).
func (s *errorSink) find(target interface{}) bool {
	for _, err := range s.all() {
		if errors.As(err, target) {
			return true
		}
	}
	return false
}

type client struct {
	*aeron.Aeron
	errs *errorSink
}

func newContext(errs *errorSink, driverTimeout time.Duration) *aeron.Context {
	return aeron.NewContext().AeronDir(aeronDir).MediaDriverTimeout(driverTimeout).ErrorHandler(errs.handle)
}

// connect connects a client and closes it at test end. Optional cfg can tweak the context.
func connect(t *testing.T, driverTimeout time.Duration, cfg ...func(*aeron.Context)) *client {
	t.Helper()
	errs := &errorSink{}
	ctx := newContext(errs, driverTimeout)
	for _, c := range cfg {
		c(ctx)
	}
	a, err := aeron.Connect(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return &client{Aeron: a, errs: errs}
}

// resources is a snapshot of process-level resources that a closed client must give back.
type resources struct {
	goroutines int
	fds        int
	maps       int
}

func snapshot(t *testing.T) resources {
	t.Helper()
	return resources{goroutines: runtime.NumGoroutine(), fds: openFDs(t), maps: mappedFiles(t)}
}

func openFDs(t *testing.T) int {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(ents)
}

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

// assertNoLeaks waits until goroutines, fds and aeron mmaps are back to the before snapshot.
func assertNoLeaks(t *testing.T, before resources) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		now := snapshot(t)
		if now.goroutines <= before.goroutines && now.fds <= before.fds && now.maps <= before.maps {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("leak: before=%+v now=%+v\n%s", before, now, buf[:n])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// driverCounters returns the allocated driver counters owned by clientID (as reported by an observer client).
func driverCounters(r *counters.Reader, clientID int64) []counters.Counter {
	var out []counters.Counter
	r.Scan(func(c counters.Counter) {
		if r.GetCounterOwnerID(c.Id) == clientID {
			out = append(out, c)
		}
	})
	return out
}

// heartbeatCounter returns the id of clientID's heartbeat counter, or -1. The driver allocates it on the first
// command it receives from the client (Java DriverConductor.getOrAddClient), keyed by the client id.
func heartbeatCounter(r *counters.Reader, clientID int64) int32 {
	return counters.FindHeartbeatCounterIDByRegistrationID(r, counters.HeartbeatTypeId, clientID)
}

// awaitNoClientCounters waits until the driver has freed every counter owned by clientID.
func awaitNoClientCounters(t *testing.T, observer *client, clientID int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		left := driverCounters(observer.CounterReader(), clientID)
		if len(left) == 0 && heartbeatCounter(observer.CounterReader(), clientID) < 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("driver still holds counters for client %d after %v: %+v", clientID, timeout, left)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// within runs f and fails if it does not return within limit.
func within(t *testing.T, limit time.Duration, what string, f func()) time.Duration {
	t.Helper()
	done := make(chan struct{})
	start := time.Now()
	go func() { defer close(done); f() }()
	select {
	case <-done:
		return time.Since(start)
	case <-time.After(limit):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("%s did not return within %v (hang)\n%s", what, limit, buf[:n])
		return 0
	}
}

// encode/decode use the docker/interop InteropTool payload: LE int64 seq + byte(seq*31+j).
func encode(msg []byte, seq int64) {
	binary.LittleEndian.PutUint64(msg, uint64(seq))
	for j := 8; j < len(msg); j++ {
		msg[j] = byte(seq*31 + int64(j))
	}
}

func decode(msg []byte) (int64, error) {
	if len(msg) < 8 {
		return 0, fmt.Errorf("short message %d", len(msg))
	}
	seq := int64(binary.LittleEndian.Uint64(msg))
	for j := 8; j < len(msg); j++ {
		if msg[j] != byte(seq*31+int64(j)) {
			return seq, fmt.Errorf("corrupt payload seq=%d byte %d", seq, j)
		}
	}
	return seq, nil
}

// proc is a child process (crashclient or a Java InteropTool container) whose stdout lines are captured.
type proc struct {
	t     *testing.T
	cmd   *exec.Cmd
	name  string // docker container name for Java peers
	mu    sync.Mutex
	lines []string
	done  chan struct{}
}

var helperOnce sync.Once
var helperPath string
var helperErr error

func buildHelper(t *testing.T) string {
	t.Helper()
	helperOnce.Do(func() {
		dir, err := os.MkdirTemp("", "crashclient")
		if err != nil {
			helperErr = err
			return
		}
		helperPath = filepath.Join(dir, "crashclient")
		args := []string{"build"}
		if raceEnabled {
			args = append(args, "-race")
		}
		out, err := exec.Command("go", append(args, "-o", helperPath, "./crashclient")...).CombinedOutput()
		if err != nil {
			helperErr = fmt.Errorf("go build crashclient: %v: %s", err, out)
		}
	})
	require.NoError(t, helperErr)
	return helperPath
}

func startProc(t *testing.T, cmd *exec.Cmd, name string) *proc {
	t.Helper()
	p := &proc{t: t, cmd: cmd, name: name, done: make(chan struct{})}
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	go func() {
		defer close(p.done)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			p.mu.Lock()
			p.lines = append(p.lines, sc.Text())
			p.mu.Unlock()
		}
		_, _ = io.Copy(io.Discard, stdout)
		_ = cmd.Wait()
	}()
	t.Cleanup(func() {
		if name != "" {
			_ = docker("kill", name)
		}
		_ = cmd.Process.Kill()
		<-p.done
	})
	return p
}

// startHelper runs the crashclient helper with the given flags.
func startHelper(t *testing.T, args ...string) *proc {
	t.Helper()
	return startProc(t, exec.Command(buildHelper(t), append([]string{"-dir", aeronDir}, args...)...), "")
}

// startJava runs `InteropTool <mode> args...` from the interop image as a host-network container.
func startJava(t *testing.T, mode string, args ...string) *proc {
	t.Helper()
	img := os.Getenv("AERON_INTEROP_IMAGE")
	if img == "" {
		img = interopImg
	}
	if docker("image", "inspect", img) != nil {
		t.Skipf("interop image %s missing: docker compose -f docker/interop/docker-compose.yml --profile tools build", img)
	}
	name := fmt.Sprintf("adv-conductor-faults-java-%d-%d", os.Getpid(), atomic.AddInt32(&portSeq, 0)+int32(time.Now().UnixNano()%100000))
	cmdArgs := append([]string{"run", "--rm", "-i", "--name", name, "--network", "host", "--ipc", "host",
		"-v", "/dev/shm:/dev/shm", "-u", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"-e", "AERON_PROPS=-Daeron.dir=" + aeronDir, img, mode}, args...)
	return startProc(t, exec.Command("docker", cmdArgs...), name)
}

// await waits for a stdout line with the given prefix and returns its fields.
func (p *proc) await(prefix string, timeout time.Duration) []string {
	p.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		p.mu.Lock()
		for _, l := range p.lines {
			if strings.HasPrefix(l, prefix) {
				p.mu.Unlock()
				return strings.Fields(l)
			}
		}
		lines := append([]string(nil), p.lines...)
		p.mu.Unlock()
		select {
		case <-p.done:
			p.t.Fatalf("process exited before %q; output: %v", prefix, lines)
		default:
		}
		if time.Now().After(deadline) {
			p.t.Fatalf("timed out waiting for %q; output: %v", prefix, lines)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (p *proc) has(prefix string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, l := range p.lines {
		if strings.HasPrefix(l, prefix) {
			return true
		}
	}
	return false
}

func (p *proc) signal(t *testing.T, sig os.Signal) {
	t.Helper()
	require.NoError(t, p.cmd.Process.Signal(sig))
}

func (p *proc) awaitExit(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(timeout):
		t.Fatalf("process did not exit within %v", timeout)
	}
}
