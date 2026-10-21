package archivecrash

import (
	"encoding/binary"
	"errors"
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
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

const (
	aeronDir    = "/dev/shm/aeron-adv-archive-crash"
	dataDir     = "/dev/shm/adv-archive-crash-data"
	composeFile = "../../../docker/adversarial-archive-crash.compose.yml"

	driverSvc  = "adv-driver"
	archiveA   = "adv-archive-a"
	archiveB   = "adv-archive-b"
	archiveSml = "adv-archive-small"

	controlA   = "aeron:udp?endpoint=localhost:27300"
	controlB   = "aeron:udp?endpoint=localhost:27310"
	controlSml = "aeron:udp?endpoint=localhost:27320"
	eventsA    = "aeron:udp?endpoint=localhost:27303|control=localhost:27301|control-mode=dynamic"

	controlStream = 10
	eventsStream  = 30

	// msgTimeout is the archive client message timeout used by the suite; every blocking archive call must
	// return within msgTimeout+slack.
	msgTimeout = 2 * time.Second
	slack      = 1500 * time.Millisecond

	interopImage = "aeron-go-interop-java:1.53.3"
)

var containers = map[string]string{
	driverSvc:  "adv-archive-crash-driver",
	archiveA:   "adv-archive-crash-archive-a",
	archiveB:   "adv-archive-crash-archive-b",
	archiveSml: "adv-archive-crash-archive-small",
}

func TestMain(m *testing.M) {
	if role := os.Getenv(childRoleEnv); role != "" {
		os.Exit(runChild(role))
	}
	if enabled() {
		// The suite owns one docker compose project; serialise concurrent runs of it instead of letting them
		// recreate each other's containers.
		lock, err := os.OpenFile(filepath.Join(os.TempDir(), "adv-archive-crash.lock"), os.O_CREATE|os.O_RDWR, 0o644)
		if err == nil && syscall.Flock(int(lock.Fd()), syscall.LOCK_EX) == nil {
			defer lock.Close()
		}
	}
	code := m.Run()
	if enabled() && os.Getenv("AERON_ADV_KEEP_STACK") != "1" {
		_ = compose("down", "-t", "0")
	}
	os.Exit(code)
}

func enabled() bool { return e2e.Enabled() && os.Getenv("AERON_ADVERSARIAL") == "1" }

// freshStack skips unless the suite is enabled, then recreates the whole stack with empty catalogs.
func freshStack(t *testing.T) {
	t.Helper()
	if !enabled() {
		t.Skip("set AERON_E2E=1 AERON_ADVERSARIAL=1 (kills/restarts docker/adversarial-archive-crash.compose.yml)")
	}
	_ = compose("kill", "-s", "KILL")
	require.NoError(t, compose("rm", "-f", "-s", "-v"))
	require.NoError(t, os.RemoveAll(dataDir))
	require.NoError(t, os.MkdirAll(dataDir+"/a", 0o755))
	require.NoError(t, os.MkdirAll(dataDir+"/b", 0o755))
	require.NoError(t, compose("up", "-d", "--wait"))
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

func running(container string) bool {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", container).Output()
	return err == nil && strings.TrimSpace(string(out)) == "true"
}

// kill sends SIGKILL to a service (unless it already died) and waits for its container to exit.
func kill(t *testing.T, svc string) {
	t.Helper()
	if running(containers[svc]) {
		docker(t, "kill", "-s", "KILL", containers[svc])
	}
	awaitTrue(t, svc+" exited", 15*time.Second, func() bool { return !running(containers[svc]) })
	if id, ok := archiveIDs[svc]; ok && running(containers[driverSvc]) {
		// Java refuses to start an archive while the driver still has counters of the previous archive with the
		// same archive id ("found existing archive for archiveId"), i.e. until the driver times out the dead
		// archive client (aeron.client.liveness.timeout).
		obs, err := aeron.Connect(newContext(nil))
		require.NoError(t, err)
		defer obs.Close()
		label := fmt.Sprintf("archiveId=%d", id)
		awaitTrue(t, svc+" counters freed by the driver", 15*time.Second, func() bool {
			n := 0
			obs.CounterReader().Scan(func(c counters.Counter) {
				if strings.Contains(c.Label, label) {
					n++
				}
			})
			return n == 0
		})
	}
}

var archiveIDs = map[string]int{archiveA: 9300, archiveB: 9301, archiveSml: 9302}

func pause(t *testing.T, svc string)  { t.Helper(); docker(t, "kill", "-s", "STOP", containers[svc]) }
func resume(t *testing.T, svc string) { t.Helper(); docker(t, "kill", "-s", "CONT", containers[svc]) }

// start (re)starts services and waits until they are healthy. Archives exit by themselves once their driver is
// gone; restarting the driver waits for that first.
func start(t *testing.T, svcs ...string) {
	t.Helper()
	for _, s := range svcs {
		if s == driverSvc {
			for _, a := range []string{archiveA, archiveB, archiveSml} {
				deadline := time.Now().Add(15 * time.Second)
				for running(containers[a]) && time.Now().Before(deadline) {
					time.Sleep(100 * time.Millisecond)
				}
				if running(containers[a]) {
					docker(t, "kill", "-s", "KILL", containers[a])
				}
			}
		}
	}
	if err := compose(append([]string{"up", "-d", "--wait"}, svcs...)...); err != nil {
		for _, s := range svcs {
			out, _ := exec.Command("docker", "logs", "--tail", "40", containers[s]).CombinedOutput()
			t.Logf("%s logs:\n%s", s, out)
		}
		t.Fatal(err)
	}
}

type errSink struct {
	mu   sync.Mutex
	errs []error
}

func (s *errSink) add(err error) { s.mu.Lock(); s.errs = append(s.errs, err); s.mu.Unlock() }

func (s *errSink) all() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.errs...)
}

func (s *errSink) any(pred func(error) bool) bool {
	for _, e := range s.all() {
		if pred(e) {
			return true
		}
	}
	return false
}

func newContext(sink *errSink) *aeron.Context {
	ctx := aeron.NewContext().AeronDir(aeronDir).MediaDriverTimeout(3 * time.Second)
	if sink != nil {
		ctx.ErrorHandler(sink.add)
	}
	return ctx
}

// connect connects an aeron client to the suite driver, retrying while it starts.
func connect(t *testing.T, sink *errSink) *aeron.Aeron {
	t.Helper()
	var a *aeron.Aeron
	var err error
	deadline := time.Now().Add(20 * time.Second)
	for {
		if a, err = aeron.Connect(newContext(sink)); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func archiveOptions(control string) *archive.Options {
	o := archive.DefaultOptions()
	o.RequestChannel = control
	o.RequestStream = controlStream
	o.ResponseChannel = "aeron:udp?endpoint=localhost:0"
	o.Timeout = msgTimeout
	return o
}

// connectArchive connects an archive client that owns its own aeron client, retrying while the archive starts.
func connectArchive(t *testing.T, control string, sink *errSink) *archive.Archive {
	t.Helper()
	var arch *archive.Archive
	var err error
	deadline := time.Now().Add(30 * time.Second)
	for {
		o := archiveOptions(control)
		if sink != nil {
			o.ErrorHandler = sink.add
		}
		if arch, err = archive.NewArchive(o, newContext(sink)); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = arch.Close() })
	return arch
}

// timed runs fn and fails if it does not return within max.
func timed[T any](t *testing.T, what string, max time.Duration, fn func() T) T {
	t.Helper()
	done := make(chan T, 1)
	t0 := time.Now()
	go func() { done <- fn() }()
	select {
	case v := <-done:
		t.Logf("%s returned after %v", what, time.Since(t0).Round(time.Millisecond))
		return v
	case <-time.After(max + 10*time.Second):
		t.Fatalf("%s hung for more than %v", what, max+10*time.Second)
	}
	var zero T
	return zero
}

// requireBounded runs fn and requires it to return within max.
func requireBounded[T any](t *testing.T, what string, max time.Duration, fn func() T) T {
	t.Helper()
	t0 := time.Now()
	v := timed(t, what, max, fn)
	require.LessOrEqual(t, time.Since(t0), max, "%s took longer than %v", what, max)
	return v
}

// requireArchiveFailure checks that err is what Java's AeronArchive raises once the archive is gone: a
// TimeoutException (no response within messageTimeout) or an ArchiveException for the disconnected response
// channel / connection.
func requireArchiveFailure(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var te *aeron.TimeoutError
	var ae *archive.ArchiveException
	if errors.As(err, &te) || errors.As(err, &ae) {
		return
	}
	msg := err.Error()
	require.True(t, strings.Contains(msg, "not connected") || strings.Contains(msg, "closed") ||
		strings.Contains(msg, "timeout") || strings.Contains(msg, "disconnected"),
		"unexpected archive failure %T: %v", err, err)
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

// resources is a snapshot of process resources used to detect leaks after Close.
type resources struct{ goroutines, mmaps, fds int }

func snapshot(t *testing.T) resources {
	t.Helper()
	maps, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	n := 0
	for _, line := range strings.Split(string(maps), "\n") {
		if strings.Contains(line, aeronDir+"/") {
			n++
		}
	}
	fds, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return resources{runtime.NumGoroutine(), n, len(fds)}
}

// requireNoLeaks waits for goroutines, aeron mmaps and fds to return to the baseline.
func requireNoLeaks(t *testing.T, base resources) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		now := snapshot(t)
		if now.goroutines <= base.goroutines+1 && now.mmaps <= base.mmaps && now.fds <= base.fds {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("leak after close: now %+v, baseline %+v\n%s", now, base, buf[:n])
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// countCounters counts the allocated driver counters of typeID whose label contains labelPart.
func countCounters(a *aeron.Aeron, typeID int32, labelPart string) int {
	n := 0
	a.CounterReader().Scan(func(c counters.Counter) {
		if c.TypeId == typeID && strings.Contains(c.Label, labelPart) {
			n++
		}
	})
	return n
}

// --- payloads --------------------------------------------------------------------------------------------------

// payload formats: Go publishers use e2e.EncodeMessage, the Java interop tool uses InteropTool.fill.
type format int

const (
	goFormat format = iota
	javaFormat
)

func javaFill(buf []byte, seq int64) {
	binary.LittleEndian.PutUint64(buf, uint64(seq))
	for j := 8; j < len(buf); j++ {
		buf[j] = byte(seq*31 + int64(j))
	}
}

func decode(f format, msg []byte, size int) (int64, error) {
	if f == goFormat {
		return e2e.DecodeMessage(msg)
	}
	if len(msg) < 8 {
		return 0, fmt.Errorf("short message %d", len(msg))
	}
	seq := int64(binary.LittleEndian.Uint64(msg))
	if len(msg) != size {
		return seq, fmt.Errorf("seq %d length %d != %d", seq, len(msg), size)
	}
	for j := 8; j < len(msg); j++ {
		if msg[j] != byte(seq*31+int64(j)) {
			return seq, fmt.Errorf("seq %d corrupt at %d", seq, j)
		}
	}
	return seq, nil
}

// verifier checks a replayed stream: consecutive sequences from `next`, byte-identical payloads and, when known,
// the stream position each message ends at.
type verifier struct {
	format  format
	size    int
	next    int64
	endPos  map[int64]int64 // seq -> publication position after the message, if known
	lastPos int64
	err     error
}

func (v *verifier) onMessage(buf *atomic.Buffer, offset, length int32, header *logbuffer.Header) {
	if v.err != nil {
		return
	}
	seq, err := decode(v.format, buf.GetBytesArray(offset, length), v.size)
	switch {
	case err != nil:
		v.err = err
	case seq != v.next:
		v.err = fmt.Errorf("expected seq %d, got %d (gap or duplicate)", v.next, seq)
	default:
		pos := header.Position()
		if want, ok := v.endPos[seq]; ok && want != pos {
			v.err = fmt.Errorf("seq %d ends at %d, published to %d", seq, pos, want)
		}
		v.lastPos = pos
		v.next++
	}
}

func (v *verifier) handler() term.FragmentHandler {
	return aeron.NewFragmentAssembler(v.onMessage, 4096).OnFragment
}

// replayVerify replays [from, to) of a recording and verifies it with v until position `to` is reached.
func replayVerify(t *testing.T, arch *archive.Archive, recordingID, from, to int64, stream int32, v *verifier) {
	t.Helper()
	sub, replaySession, err := e2e.Replay(arch, recordingID, from, to-from, "aeron:udp?endpoint=localhost:27390", stream)
	require.NoError(t, err)
	defer func() {
		_ = arch.StopReplay(replaySession)
		_ = sub.Close()
	}()
	h := v.handler()
	deadline := time.Now().Add(30 * time.Second)
	for {
		sub.Poll(h, 100)
		require.NoError(t, v.err)
		if img := sub.ImageBySessionID(int32(replaySession)); img != nil && img.Position() >= to {
			break
		}
		if v.lastPos >= to {
			break
		}
		require.True(t, time.Now().Before(deadline), "replay of %d [%d,%d) stalled at %d (next seq %d)",
			recordingID, from, to, v.lastPos, v.next)
		time.Sleep(time.Millisecond)
	}
	require.NoError(t, v.err)
}
