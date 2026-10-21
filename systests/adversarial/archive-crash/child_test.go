package archivecrash

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

// The test binary re-executes itself as a separate Go client process when childRoleEnv is set, so a real client
// process can be SIGKILLed while it holds archive or driver resources.
const childRoleEnv = "ADV_ARCHIVE_CRASH_CHILD"

func childFail(format string, args ...interface{}) int {
	fmt.Printf("ERROR "+format+"\n", args...)
	return 1
}

func envInt(name string) int64 {
	v, _ := strconv.ParseInt(os.Getenv(name), 10, 64)
	return v
}

func runChild(role string) int {
	switch role {
	case "replay":
		// Holds a follow-live replay of ADV_RECORDING on ADV_STREAM until killed.
		o := archiveOptions(os.Getenv("ADV_CONTROL"))
		arch, err := archive.NewArchive(o, newContext(nil))
		if err != nil {
			return childFail("connect: %v", err)
		}
		channel := "aeron:udp?endpoint=localhost:27391|alias=adv-child-replay"
		sub, replaySession, err := e2e.Replay(arch, envInt("ADV_RECORDING"), 0, aeron.NullValue, channel,
			int32(envInt("ADV_STREAM")))
		if err != nil {
			return childFail("replay: %v", err)
		}
		fmt.Printf("READY controlSession=%d replaySession=%d clientId=%d\n", arch.ControlSessionID(), replaySession,
			arch.Aeron().ClientID())
		var received int64
		h := func(*atomic.Buffer, int32, int32, *logbuffer.Header) { received++ }
		for {
			if sub.Poll(h, 10) == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	case "publish":
		// Publishes sequenced messages on ADV_CHANNEL/ADV_STREAM with an exclusive publication until killed.
		a, err := aeron.Connect(newContext(nil))
		if err != nil {
			return childFail("connect: %v", err)
		}
		pub, err := a.AddExclusivePublication(os.Getenv("ADV_CHANNEL"), int32(envInt("ADV_STREAM")))
		if err != nil {
			return childFail("publication: %v", err)
		}
		fmt.Printf("READY sessionId=%d clientId=%d\n", pub.SessionID(), a.ClientID())
		err = e2e.Publish(pub, e2e.PublishOptions{Count: 1 << 40, Size: int(envInt("ADV_SIZE")),
			Interval: 200 * time.Microsecond, Timeout: time.Hour})
		return childFail("publish: %v", err)
	}
	return childFail("unknown role %s", role)
}

// child is a running helper process.
type child struct {
	t      *testing.T
	cmd    *exec.Cmd
	mu     sync.Mutex
	out    strings.Builder
	ready  chan map[string]int64
	exited chan struct{}
}

func startChild(t *testing.T, role string, env ...string) *child {
	t.Helper()
	exe, err := os.Executable()
	require.NoError(t, err)
	c := &child{t: t, ready: make(chan map[string]int64, 1), exited: make(chan struct{})}
	c.cmd = exec.Command(exe, "-test.run=^$")
	c.cmd.Env = append(append(os.Environ(), childRoleEnv+"="+role), env...)
	stdout, err := c.cmd.StdoutPipe()
	require.NoError(t, err)
	c.cmd.Stderr = c.cmd.Stdout
	require.NoError(t, c.cmd.Start())
	go func() {
		defer close(c.exited)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Text()
			c.mu.Lock()
			if c.out.Len() < 1<<16 {
				c.out.WriteString(line + "\n")
			}
			c.mu.Unlock()
			if strings.HasPrefix(line, "READY ") {
				kv := map[string]int64{}
				for _, f := range strings.Fields(line)[1:] {
					if k, v, ok := strings.Cut(f, "="); ok {
						kv[k], _ = strconv.ParseInt(v, 10, 64)
					}
				}
				c.ready <- kv
			}
		}
		_, _ = io.Copy(io.Discard, stdout)
		_ = c.cmd.Wait()
	}()
	t.Cleanup(c.kill)
	return c
}

func (c *child) awaitReady() map[string]int64 {
	c.t.Helper()
	select {
	case kv := <-c.ready:
		return kv
	case <-c.exited:
	case <-time.After(30 * time.Second):
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t.Fatalf("child not ready:\n%s", c.out.String())
	return nil
}

// kill SIGKILLs the child and reaps it.
func (c *child) kill() {
	_ = c.cmd.Process.Signal(syscall.SIGKILL)
	<-c.exited
}
