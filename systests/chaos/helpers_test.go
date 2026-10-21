// Package chaos holds robustness and fault-injection tests that run the Go client against the Java 1.53.3 stacks in
// docker/chaos.compose.yml: killing and restarting the media driver and archive mid-stream, packet loss from the
// driver's loss generator, slow consumers, large fragmented messages and resource churn.
//
//	docker compose -f docker/chaos.compose.yml up -d --build --wait
//	AERON_E2E=1 AERON_E2E_CHAOS=1 go test -count=1 -p 1 -v ./systests/chaos/
package chaos

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

const (
	chaosDir            = "/dev/shm/aeron-chaos"
	chaosDriver         = "aeron-chaos-driver"
	chaosArchive        = "aeron-chaos-archive"
	chaosControlChannel = "aeron:udp?endpoint=localhost:8410"
	lossyDir            = "/dev/shm/aeron-lossy"
	lossyControlChannel = "aeron:udp?endpoint=localhost:8510"
	controlStream       = 10
	composeFile         = "../../docker/chaos.compose.yml"
)

// runStreamOffset keeps recorded stream ids unique across runs: the chaos archive catalog persists.
var runStreamOffset = int32(time.Now().Unix()%10000) * 100

func stream(base int32) int32 { return base + runStreamOffset }

var stackOnce sync.Once
var stackErr error

func requireChaos(t *testing.T) {
	t.Helper()
	if !e2e.Enabled() || os.Getenv("AERON_E2E_CHAOS") != "1" {
		t.Skip("set AERON_E2E=1 AERON_E2E_CHAOS=1 (kills/restarts the docker/chaos.compose.yml containers)")
	}
	stackOnce.Do(func() { stackErr = compose("up", "-d", "--wait") })
	require.NoError(t, stackErr)
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

// restartStack starts the chaos driver (and its archive) and waits for both to be healthy. After the driver has been
// killed the archive terminates on its own driver timeout, so wait for (or force) that first: otherwise compose sees it
// still running and then fails when it exits.
func restartStack(t *testing.T) {
	t.Helper()
	if !running(chaosDriver) {
		deadline := time.Now().Add(15 * time.Second)
		for running(chaosArchive) && time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
		}
		if running(chaosArchive) {
			docker(t, "kill", chaosArchive)
		}
	}
	require.NoError(t, compose("up", "-d", "--wait", "chaos-driver", "chaos-archive"))
}

func running(container string) bool {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", container).Output()
	return err == nil && strings.TrimSpace(string(out)) == "true"
}

func newContext(dir string) *aeron.Context {
	return aeron.NewContext().AeronDir(dir).MediaDriverTimeout(3 * time.Second)
}

func connect(t *testing.T, dir string) *aeron.Aeron {
	t.Helper()
	var a *aeron.Aeron
	var err error
	// The driver may still be starting after a restart.
	deadline := time.Now().Add(20 * time.Second)
	for {
		a, err = aeron.Connect(newContext(dir))
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func connectArchive(t *testing.T, dir, control string) *archive.Archive {
	t.Helper()
	arch, err := connectArchiveErr(dir, control, 30*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = arch.Close() })
	return arch
}

func connectArchiveErr(dir, control string, within time.Duration) (*archive.Archive, error) {
	deadline := time.Now().Add(within)
	for {
		options := archive.DefaultOptions()
		options.RequestChannel = control
		options.RequestStream = controlStream
		options.ResponseChannel = "aeron:udp?endpoint=localhost:0"
		options.Timeout = 3 * time.Second
		arch, err := archive.NewArchive(options, newContext(dir))
		if err == nil || time.Now().After(deadline) {
			return arch, err
		}
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
		time.Sleep(5 * time.Millisecond)
	}
}

// mappedFiles counts this process's memory mappings of files under dir (log buffers, cnc.dat).
func mappedFiles(t *testing.T, dir string) int {
	t.Helper()
	maps, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	n := 0
	for _, line := range strings.Split(string(maps), "\n") {
		if strings.Contains(line, dir+"/") {
			n++
		}
	}
	return n
}

// awaitGoroutines waits for the goroutine count to drop to at most max, dumping stacks on failure.
func awaitGoroutines(t *testing.T, max int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for runtime.NumGoroutine() > max {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("goroutine leak: %d > %d\n%s", runtime.NumGoroutine(), max, buf[:n])
		}
		time.Sleep(20 * time.Millisecond)
	}
}
