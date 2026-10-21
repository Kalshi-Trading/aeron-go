package networkfaults

import (
	"bufio"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// peerProc is a running nfpeer child process.
type peerProc struct {
	t     *testing.T
	cmd   *exec.Cmd
	mu    sync.Mutex
	lines []string
	read  int
}

func startPeer(t *testing.T, args ...string) *peerProc {
	t.Helper()
	p := &peerProc{t: t, cmd: exec.Command(peerBinary(t), args...)}
	out, err := p.cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, p.cmd.Start())
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			p.mu.Lock()
			p.lines = append(p.lines, sc.Text())
			p.mu.Unlock()
		}
	}()
	t.Cleanup(func() { p.kill9() })
	return p
}

// await returns the fields of the next line (after the previously awaited one) starting with prefix.
func (p *peerProc) await(prefix string, timeout time.Duration) []string {
	p.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		for ; p.read < len(p.lines); p.read++ {
			if f := strings.Fields(p.lines[p.read]); len(f) > 0 && f[0] == prefix {
				p.read++
				p.mu.Unlock()
				return f
			}
		}
		p.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	p.t.Fatalf("nfpeer: no %q line after %v: %v", prefix, timeout, p.output())
	return nil
}

func (p *peerProc) output() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.lines...)
}

func (p *peerProc) errors() []string {
	var errs []string
	for _, l := range p.output() {
		if strings.HasPrefix(l, "error") {
			errs = append(errs, l)
		}
	}
	return errs
}

func (p *peerProc) kill9() {
	if p.cmd.ProcessState == nil {
		_ = p.cmd.Process.Signal(syscall.SIGKILL)
		_ = p.cmd.Wait()
	}
}

// TestPublisherProcessKilledUnderLoss: a Go publisher process on driver a is SIGKILLed while 10% of its packets are
// lost. Java: the driver times the client out (client liveness 5s) and closes its publication; the subscriber keeps
// a gap free stream up to the end, then gets the unavailable image event. A restarted publisher process gets a new
// session and a new image that delivers from its first message.
func TestPublisherProcessKilledUnderLoss(t *testing.T) {
	for _, exclusive := range []bool{false, true} {
		exclusive := exclusive
		t.Run(fmt.Sprintf("exclusive=%v", exclusive), func(t *testing.T) {
			requireStack(t)
			b := connect(t, dirB)
			p, stream := port(), streamID()
			ch := udp("localhost", p, "")
			sub, err := b.AddSubscription(ch, stream)
			require.NoError(t, err)
			netem(t, toBoth("loss 10%", p))
			args := []string{"-dir", dirA, "-mode", "pub", "-channel", ch, "-stream", fmt.Sprint(stream),
				fmt.Sprintf("-exclusive=%v", exclusive)}
			pub1 := startPeer(t, args...)
			session1 := pub1.await("connected", 15*time.Second)[1]
			img1 := awaitImage(t, b.available, "available", 10*time.Second)
			require.Equal(t, session1, fmt.Sprint(img1.SessionID()))
			r1 := newImageReader(img1)
			r1.pollUntil(t, "traffic", 15*time.Second, func() bool { return r1.received > 3000 })

			pub1.kill9()
			killed := time.Now()
			r1.pollUntil(t, "unavailable image after publisher crash", 20*time.Second, func() bool {
				select {
				case img := <-b.unavailable:
					require.Equal(t, img1.CorrelationID(), img.CorrelationID())
					return true
				default:
					return false
				}
			})
			t.Logf("image gone %v after kill -9, delivered %d..%d", time.Since(killed), r1.first, r1.next-1)
			require.Zero(t, r1.first)

			pub2 := startPeer(t, args...)
			session2 := pub2.await("connected", 15*time.Second)[1]
			require.NotEqual(t, session1, session2)
			img2 := awaitImage(t, b.available, "available after restart", 10*time.Second)
			require.Equal(t, session2, fmt.Sprint(img2.SessionID()))
			r2 := newImageReader(img2)
			r2.pollUntil(t, "traffic from restarted publisher", 15*time.Second, func() bool { return r2.received > 2000 })
			require.Zero(t, r2.first)
			require.Equal(t, 1, sub.ImageCount())
			require.Empty(t, pub2.errors())
			require.Empty(t, b.errors.all())
		})
	}
}

// TestSubscriberProcessKilledUnderLoss: a Go subscriber process on driver b is SIGKILLed while the path loses 10%.
// Java: driver b times the client out (5s), removes the subscription and its image, status messages stop and the
// publication goes not connected (offers return NOT_CONNECTED rather than staying back pressured). A new subscriber
// process reconnects the publication and receives a gap free stream from its join position.
func TestSubscriberProcessKilledUnderLoss(t *testing.T) {
	requireStack(t)
	a := connect(t, dirA)
	p, stream := port(), streamID()
	ch := udp("localhost", p, "")
	args := []string{"-dir", dirB, "-mode", "sub", "-channel", ch, "-stream", fmt.Sprint(stream)}
	sub1 := startPeer(t, args...)
	sub1.await("ready", 15*time.Second)
	pub, err := a.AddPublication(ch, stream)
	require.NoError(t, err)
	awaitTrue(t, "publication connected", 15*time.Second, pub.IsConnected)
	netem(t, toBoth("loss 10%", p))
	pb := newPublisher(pub, modeOffer, mixedSize)
	done := pb.publishAsync(1<<40, 200*time.Microsecond, 2*time.Minute)
	sub1.await("seq", 15*time.Second)
	sub1.await("seq", 15*time.Second)

	sub1.kill9()
	killed := time.Now()
	awaitTrue(t, "publication not connected after subscriber crash", 20*time.Second, func() bool { return !pub.IsConnected() })
	awaitTrue(t, "offer NOT_CONNECTED", 5*time.Second, func() bool { return pb.notConn.Load() > 0 })
	t.Logf("publication not connected %v after kill -9", time.Since(killed))

	sub2 := startPeer(t, args...)
	sub2.await("image", 20*time.Second)
	awaitTrue(t, "publication reconnected", 15*time.Second, pub.IsConnected)
	pb.resetStall()
	for i := 0; i < 4; i++ {
		sub2.await("seq", 15*time.Second)
	}
	require.Less(t, time.Duration(pb.maxStall.Load()), 2*time.Second)
	require.Empty(t, sub2.errors())
	pb.stop.Store(true)
	require.NoError(t, <-done)
	require.Empty(t, a.errors.all())
}

// TestReceiverDriverFrozen: SIGSTOP the receiving driver for 4s (longer than image liveness and the publication
// connection timeout, shorter than the client's 10s driver timeout) while the path loses 5%. On SIGCONT the
// subscriber must recover: either the same image continues gap free or it is replaced by a new image, and the
// publisher resumes. The Go client attached to the frozen driver must not report a driver timeout.
func TestReceiverDriverFrozen(t *testing.T) {
	requireStack(t)
	p := port()
	topo := topology{pub: udp("localhost", p, ""), sub: udp("localhost", p, ""), ports: []int{p}}
	a, b, pub, _, _ := goPair(t, topo, false)
	img1 := awaitImage(t, b.available, "available", 10*time.Second)
	r1 := newImageReader(img1)
	netem(t, toBoth("loss 5%", p))
	pb := newPublisher(pub, modeOffer, smallSize)
	done := pb.publishAsync(1<<40, 200*time.Microsecond, 2*time.Minute)
	r1.pollUntil(t, "traffic", 10*time.Second, func() bool { return r1.received > 1000 })

	docker(t, "kill", "-s", "STOP", driverB)
	time.Sleep(4 * time.Second)
	docker(t, "kill", "-s", "CONT", driverB)
	pb.resetStall()
	reader, baseline := r1, r1.received
	deadline := time.Now().Add(20 * time.Second)
	for (reader == r1 && reader.received < baseline+2000) || (reader != r1 && reader.received < 2000) {
		require.NoError(t, reader.err)
		select {
		case img := <-b.available:
			require.True(t, img1.IsClosed())
			require.GreaterOrEqual(t, img.JoinPosition(), img1.Position())
			reader = newImageReader(img)
			t.Logf("new image after freeze, join %d", img.JoinPosition())
		default:
		}
		if reader.poll() == 0 {
			require.True(t, time.Now().Before(deadline), "no recovery after SIGCONT: received %d", reader.received)
			time.Sleep(100 * time.Microsecond)
		}
	}
	require.NoError(t, reader.err)
	require.Less(t, time.Duration(pb.maxStall.Load()), 5*time.Second)
	pb.stop.Store(true)
	require.NoError(t, <-done)
	require.Empty(t, a.errors.all())
	require.Empty(t, b.errors.all())
}
