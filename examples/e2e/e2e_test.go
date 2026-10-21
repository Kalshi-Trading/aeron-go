// Package e2e runs the sample programs against a real Java 1.53.3 media driver/archive (see
// docker/README.md). Enabled with AERON_E2E=1; AERON_DIR defaults to /dev/shm/aeron-compose. The
// cluster echo test also needs docker/cluster.compose.yml running (AERON_E2E_CLUSTER=1).
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
)

var (
	buildOnce sync.Once
	binDir    string
	buildErr  error
)

func setup(t *testing.T) string {
	t.Helper()
	if os.Getenv("AERON_E2E") != "1" {
		t.Skip("set AERON_E2E=1 to run against the docker Java driver")
	}
	dir := os.Getenv("AERON_DIR")
	if dir == "" {
		dir = "/dev/shm/aeron-compose"
	}
	if _, err := os.Stat(filepath.Join(dir, "cnc.dat")); err != nil {
		t.Fatalf("no media driver at %s: %v", dir, err)
	}
	buildOnce.Do(func() {
		binDir, buildErr = os.MkdirTemp("", "aeron-go-examples")
		if buildErr != nil {
			return
		}
		cmd := exec.Command("go", "build", "-o", binDir+"/", "github.com/lirm/aeron-go/examples/...",
			"github.com/lirm/aeron-go/archive/examples/basic_recording_publisher",
			"github.com/lirm/aeron-go/archive/examples/basic_replayed_subscriber",
			"github.com/lirm/aeron-go/archive/examples/replay_throughput")
		out, err := cmd.CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return dir
}

type proc struct {
	cmd *exec.Cmd
	out *bytes.Buffer
	mu  *sync.Mutex
}

type lockedWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (p *proc) output() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.out.String()
}

func start(t *testing.T, ctx context.Context, name string, args ...string) *proc {
	t.Helper()
	p := &proc{out: &bytes.Buffer{}, mu: &sync.Mutex{}}
	p.cmd = exec.CommandContext(ctx, filepath.Join(binDir, name), args...)
	p.cmd.Stdout = lockedWriter{p.mu, p.out}
	p.cmd.Stderr = lockedWriter{p.mu, p.out}
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return p
}

func (p *proc) wait(t *testing.T) string {
	t.Helper()
	if err := p.cmd.Wait(); err != nil {
		t.Fatalf("%s failed: %v\n%s", p.cmd.Path, err, p.output())
	}
	return p.output()
}

func run(t *testing.T, timeout time.Duration, name string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return start(t, ctx, name, args...).wait(t)
}

func waitFor(t *testing.T, p *proc, text string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(p.output(), text) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %q in\n%s", text, p.output())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func mustContain(t *testing.T, out string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in output:\n%s", want, out)
		}
	}
}

// pair starts a subscriber, waits for it to subscribe, runs the publisher and returns both outputs.
func pair(t *testing.T, sub []string, pub []string) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s := start(t, ctx, sub[0], sub[1:]...)
	waitFor(t, s, "Subscribing to")
	time.Sleep(200 * time.Millisecond)
	pubOut := run(t, time.Minute, pub[0], pub[1:]...)
	return s.wait(t), pubOut
}

func TestBasicPublisherSubscriber(t *testing.T) {
	dir := setup(t)
	for _, publisher := range []string{"basic_publisher", "exclusive_publisher"} {
		t.Run(publisher, func(t *testing.T) {
			chan_ := "aeron:udp?endpoint=localhost:20131"
			subOut, pubOut := pair(t,
				[]string{"basic_subscriber", "-p", dir, "-chan", chan_, "-exit", "3", "-idle", "sleep"},
				[]string{publisher, "-p", dir, "-chan", chan_, "-m", "3"})
			mustContain(t, pubOut, "Offering 0/3 - yay!", "Done sending.")
			mustContain(t, subOut, "<<Hello World! 0>>", "<<Hello World! 2>>", "received 3 messages")
		})
	}
}

func TestSimplePublisherSubscriber(t *testing.T) {
	dir := setup(t)
	subOut, pubOut := pair(t,
		[]string{"simple_subscriber", "-p", dir, "-exit", "1"},
		[]string{"simple_publisher", "-p", dir})
	mustContain(t, pubOut, "yay!")
	mustContain(t, subOut, "Message to stream 10", "<<Hello World! >>")
}

func TestFragmentation(t *testing.T) {
	dir := setup(t)
	chan_ := "aeron:udp?endpoint=localhost:20132"
	subOut, pubOut := pair(t,
		[]string{"multiple_subscribers_with_fragment_assembly", "-p", dir, "-chan", chan_, "-exit", "4", "-idle", "sleep"},
		[]string{"multiple_publishers_with_fragmentation", "-p", dir, "-chan", chan_, "-m", "2"})
	mustContain(t, pubOut, "stream id 1001 = 2 and stream id 1002 = 2")
	mustContain(t, subOut, "received 4 messages", "message to stream 1001", "message to stream 1002")
	if strings.Contains(subOut, "not assembled properly") {
		t.Fatalf("fragment reassembly failed:\n%s", subOut)
	}
}

func TestStreamingPublisherRateSubscriber(t *testing.T) {
	dir := setup(t)
	chan_ := "aeron:udp?endpoint=localhost:20133"
	subOut, pubOut := pair(t,
		[]string{"rate_subscriber", "-p", dir, "-chan", chan_, "-exit", "100000"},
		[]string{"streaming_publisher", "-p", dir, "-chan", chan_, "-m", "100000", "-len", "64"})
	mustContain(t, pubOut, "Done streaming. sent=100000")
	mustContain(t, subOut, "received 100000 messages, 6400000 bytes")
}

func TestExclusiveThroughput(t *testing.T) {
	dir := setup(t)
	out := run(t, time.Minute, "exclusive_throughput", "-p", dir, "-chan", "aeron:ipc", "-m", "200000")
	mustContain(t, out, "Done streaming. received=200000")
}

func TestPingPong(t *testing.T) {
	dir := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pong := start(t, ctx, "pong", "-p", dir, "-exit", "1100")
	waitFor(t, pong, "Publishing Pong")
	out := run(t, time.Minute, "ping", "-p", dir, "-m", "1000", "-wi", "1", "-wm", "100")
	mustContain(t, out, "Histogram of RTT latencies", "Total count    =         1000")
	mustContain(t, pong.wait(t), "echoed 1100 messages")
}

func TestStatTools(t *testing.T) {
	dir := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	chan_ := "aeron:udp?endpoint=localhost:20134"
	sub := start(t, ctx, "basic_subscriber", "-p", dir, "-chan", chan_, "-sid", "4242", "-idle", "sleep", "-exit", "2")
	waitFor(t, sub, "Subscribing to")
	pub := start(t, ctx, "basic_publisher", "-p", dir, "-chan", chan_, "-sid", "4242", "-m", "2")
	waitFor(t, sub, "<<Hello World! 0>>")

	stat := run(t, 10*time.Second, "aeronstat", "-p", dir, "watch=false", "stream=4242")
	mustContain(t, stat, "Aeron Stat (CnC v0.2.0)", "sub-pos:", "pub-lmt:", "4242 "+chan_)
	if regexp.MustCompile(`(?m)^\s*\d+: .* - Bytes sent$`).MatchString(stat) {
		t.Fatalf("stream filter did not exclude system counters:\n%s", stat)
	}
	all := run(t, 10*time.Second, "aeronstat", "-p", dir, "watch=false", "type=0")
	mustContain(t, all, "- Bytes sent", "- Errors")

	backlog := run(t, 10*time.Second, "backlogstat", "-p", dir)
	mustContain(t, backlog, "streamId=4242 channel="+chan_, "┌─for publisher", "sender", "┌─receiver", "─subscriber")

	sub.wait(t)
	pub.wait(t)

	// Provoke a distinct driver error so errorstat has real data to read.
	a, err := aeron.Connect(aeron.NewContext().AeronDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.AddPublication("aeron:udp?endpoint=no-such-host.invalid:1", 1); err == nil {
		t.Fatal("expected bad endpoint to be rejected")
	}
	a.Close()
	errs := run(t, 10*time.Second, "errorstat", "-p", dir)
	mustContain(t, errs, "observations from", "distinct errors observed.")
	if strings.Contains(errs, "\n0 distinct errors") {
		t.Fatalf("expected errors:\n%s", errs)
	}

	loss := run(t, 10*time.Second, "lossstat", "-p", dir)
	mustContain(t, loss, "#OBSERVATION_COUNT,TOTAL_BYTES_LOST", "loss entries")
}

func TestArchiveRecordAndReplay(t *testing.T) {
	dir := setup(t)
	chan_ := "aeron:udp?endpoint=localhost:20135"
	rec := run(t, time.Minute, "basic_recording_publisher", "-prefix", dir, "-samplechannel", chan_,
		"-samplestream", "5005", "-messages", "3", "-interval", "100ms")
	mustContain(t, rec, "Recording started: recordingId = ", "Offering 2/3 - yay!", "Done sending.")
	rep := run(t, time.Minute, "basic_replayed_subscriber", "-prefix", dir, "-samplechannel", chan_,
		"-samplestream", "5005", "-exit", "3")
	mustContain(t, rep, "<<Hello World! 0>>", "<<Hello World! 2>>", "Replayed 3 messages")
}

func TestArchiveReplayThroughput(t *testing.T) {
	dir := setup(t)
	out := run(t, 2*time.Minute, "replay_throughput", "-prefix", dir, "-messages", "200000", "-samplestream", "5006")
	mustContain(t, out, "Recorded ", "(200000 messages in")
}

func TestClusterEchoClient(t *testing.T) {
	dir := setup(t)
	if os.Getenv("AERON_E2E_CLUSTER") != "1" {
		t.Skip("set AERON_E2E_CLUSTER=1 with docker/cluster.compose.yml echo-cluster running")
	}
	out := run(t, time.Minute, "cluster_echo_client", "-p", dir, "-m", "5")
	mustContain(t, out, "Connected: sessionId=", "<<Hello Cluster! 4>>", "Done: 5 messages echoed")
}
