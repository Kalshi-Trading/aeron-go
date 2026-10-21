package networkfaults

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

	"github.com/lirm/aeron-go/interop"
	"github.com/stretchr/testify/require"
)

const interopImage = "aeron-go-interop-java:1.53.3" // docker compose -f docker/interop/docker-compose.yml --profile tools build

var javaSeq atomic.Int32

type event map[string]any

func (e event) int(k string) int64 {
	if n, ok := e[k].(json.Number); ok {
		v, _ := n.Int64()
		return v
	}
	return 0
}

func (e event) result() interop.Result {
	s := func(k string) string { v, _ := e[k].(string); return v }
	b, _ := e["timedOut"].(bool)
	return interop.Result{Count: e.int("count"), Errors: e.int("errors"), FirstError: s("firstError"),
		SHA256: s("sha256"), Bytes: e.int("bytes"), Position: e.int("position"),
		SessionID: int32(e.int("sessionId")), TimedOut: b}
}

// javaPeer is an InteropTool process (docker/interop) attached to a suite driver, streaming JSON events.
type javaPeer struct {
	t      *testing.T
	name   string
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	mu     sync.Mutex
	stderr bytes.Buffer
	events []event
	next   map[string]int
	exited chan struct{}
}

func startJava(t *testing.T, dir, mode string, args ...string) *javaPeer {
	t.Helper()
	p := &javaPeer{t: t, name: fmt.Sprintf("adv-nf-java-%d-%d", os.Getpid(), javaSeq.Add(1)),
		next: map[string]int{}, exited: make(chan struct{})}
	p.cmd = exec.Command("docker", append([]string{"run", "--rm", "-i", "--name", p.name,
		"--network", "host", "--ipc", "host", "-v", "/dev/shm:/dev/shm",
		"-u", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"-e", "AERON_PROPS=-Daeron.dir=" + dir, interopImage, mode}, args...)...)
	p.cmd.Stderr = &p.stderr
	stdout, err := p.cmd.StdoutPipe()
	require.NoError(t, err)
	p.stdin, err = p.cmd.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, p.cmd.Start())
	go func() {
		defer close(p.exited)
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			d := json.NewDecoder(strings.NewReader(sc.Text()))
			d.UseNumber()
			var e event
			if d.Decode(&e) == nil {
				p.mu.Lock()
				p.events = append(p.events, e)
				p.mu.Unlock()
			}
		}
		_ = p.cmd.Wait()
	}()
	t.Cleanup(p.kill)
	return p
}

// await returns the next not yet awaited event called name.
func (p *javaPeer) await(name string, timeout time.Duration) event {
	p.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		p.mu.Lock()
		for i := p.next[name]; i < len(p.events); i++ {
			if p.events[i]["event"] == name {
				p.next[name] = i + 1
				p.mu.Unlock()
				return p.events[i]
			}
		}
		p.mu.Unlock()
		select {
		case <-p.exited:
			p.mu.Lock()
			defer p.mu.Unlock()
			for i := p.next[name]; i < len(p.events); i++ {
				if p.events[i]["event"] == name {
					p.next[name] = i + 1
					return p.events[i]
				}
			}
			p.t.Fatalf("%s exited before %q: events %v stderr %s", p.name, name, p.events, p.stderr.String())
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.t.Fatalf("%s: no %q event after %v: events %v stderr %s", p.name, name, timeout, p.events, p.stderr.String())
		}
	}
}

func (p *javaPeer) count(name string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, e := range p.events {
		if e["event"] == name {
			n++
		}
	}
	return n
}

func (p *javaPeer) send(line string) { _, _ = io.WriteString(p.stdin, line+"\n") }

// kill SIGKILLs the Java process (its container) and waits for it to go away.
func (p *javaPeer) kill() {
	_ = exec.Command("docker", "kill", "-s", "KILL", p.name).Run()
	select {
	case <-p.exited:
	case <-time.After(20 * time.Second):
		_ = p.cmd.Process.Kill()
	}
}
