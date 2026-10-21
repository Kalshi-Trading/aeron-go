package drivercrash

import (
	"strconv"
	"syscall"
	"testing"
	"time"

	aatomic "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

// TestClientProcessSIGKILL SIGKILLs Go and Java client processes (publishers, subscribers, and a publisher killed
// between TryClaim and Commit) next to a surviving in-process Go client on the same streams, and checks the driver
// recovers like Java expects: the dead client's heartbeat counter and publications go after the client liveness
// timeout ("Client liveness timeouts" increments), the survivor gets an unavailable-image callback for a dead
// publisher, a dead subscriber does not stall the survivor's publication, a claim that is never committed is
// unblocked after the publication unblock timeout ("Unblocked Publications" increments) so the survivor's messages on
// the shared log keep flowing intact, the survivor never sees an error, and a replacement process resumes.
func TestClientProcessSIGKILL(t *testing.T) {
	requireAdv(t)
	type scenario struct {
		name, lang, role string
		udp              bool
	}
	var scenarios []scenario
	for _, lang := range []string{"go", "java"} {
		scenarios = append(scenarios,
			scenario{name: lang + "-publisher-udp", lang: lang, role: "pub", udp: true},
			scenario{name: lang + "-subscriber-ipc", lang: lang, role: "sub"},
		)
	}
	scenarios = append(scenarios, scenario{name: "go-publisher-killed-mid-claim", lang: "go", role: "claimhang"})

	for i, sc := range scenarios {
		sc, i := sc, i
		t.Run(sc.name, func(t *testing.T) {
			startDriver(t, plain)
			a, o := connect(t)
			reader := a.CounterReader()
			stream := 9060 + i
			channel := "aeron:ipc"
			if sc.udp {
				channel = "aeron:udp?endpoint=" + udp(60+i)
			}
			args := map[string]string{"role": sc.role, "channel": channel, "stream": itoa(stream)}
			if sc.role == "pub" {
				// Its own log (session), so its image can go away while the survivor's publication stays.
				args["exclusive"] = "true"
			}
			start := func() *peer {
				if sc.lang == "go" {
					return startGoPeer(t, goArgs(args)...)
				}
				return startJavaPeer(t, peerArgs("java", stream, driverTimeout, args)...)
			}

			// The survivor publishes and subscribes on the same channel and stream as the peer.
			sub, err := a.AddSubscription(channel, int32(stream))
			require.NoError(t, err)
			pub, err := a.AddPublication(channel, int32(stream))
			require.NoError(t, err)

			// The survivor publishes its own sequence before, during and after the fault (filtered by session); with an
			// uncommitted claim ahead of it in the shared log it is blocked until the driver unblocks the claim.
			checker := e2e.NewSequenceChecker(0)
			handler := checker.Handler()
			own := func(b *aatomic.Buffer, off, length int32, h *logbuffer.Header) {
				if h.SessionID() == pub.SessionID() {
					handler(b, off, length, h)
				}
			}
			msg := make([]byte, 64)
			buf := aatomic.MakeBuffer(msg)
			seq := int64(0)
			pump := func() {
				e2e.EncodeMessage(msg, seq)
				if pub.Offer(buf, 0, 64, nil) > 0 {
					seq++
				}
				sub.Poll(own, 64)
			}
			p := start()
			ready := p.await("ready", 30*time.Second)
			peerClient := ready.int("clientId")
			awaitTrue(t, "peer heartbeat counter", 5*time.Second, func() bool { return heartbeatCount(reader, peerClient) == 1 })
			if sc.role == "claimhang" {
				p.await("claimed", 10*time.Second)
			} else {
				awaitTrue(t, "peer traffic", 10*time.Second, func() bool {
					pump()
					e := p.last("progress")
					return e != nil && (e.int("sent") > 100 || e.int("received") > 100)
				})
			}
			timeouts := systemCounter(reader, "Client liveness timeouts")
			unblocked := systemCounter(reader, "Unblocked Publications")
			unavailable := o.unavailable.Load()

			p.signal(syscall.SIGKILL)
			require.True(t, p.exited(10*time.Second), "peer did not die")
			killed := time.Now()

			deadline := time.Now().Add(clientLiveness + unblockTimeout + 3*slack)
			from := checker.Next
			for time.Now().Before(deadline) && (checker.Next < from+2000 || heartbeatCount(reader, peerClient) != 0) {
				pump()
				time.Sleep(50 * time.Microsecond)
			}
			require.NoError(t, checker.Err(), checker.String())
			require.GreaterOrEqual(t, checker.Next, from+2000, "survivor traffic stalled after the peer died: %s", checker)
			require.Zero(t, heartbeatCount(reader, peerClient), "dead client's heartbeat counter not removed")
			t.Logf("dead client removed within %v", time.Since(killed))
			awaitTrue(t, "client liveness timeout counted", 5*time.Second, func() bool {
				return systemCounter(reader, "Client liveness timeouts") > timeouts
			})
			switch sc.role {
			case "pub":
				awaitTrue(t, "unavailable image for the dead publisher", imageLiveness+slack, func() bool {
					return o.unavailable.Load() > unavailable
				})
			case "claimhang":
				require.Greater(t, systemCounter(reader, "Unblocked Publications"), unblocked)
			}
			require.Empty(t, o.errors(), "the surviving client must not see errors")
			require.False(t, a.IsClosed())

			// A replacement process on the same stream works.
			if sc.role != "claimhang" {
				p2 := start()
				p2.await("ready", 30*time.Second)
				awaitTrue(t, "replacement peer traffic", 10*time.Second, func() bool {
					pump()
					e := p2.last("progress")
					return e != nil && (e.int("sent") > 100 || e.int("received") > 100)
				})
				p2.kill()
			}
			require.NoError(t, pub.Close())
			require.NoError(t, sub.Close())
		})
	}
}

// TestPeersDriverSIGKILLAndInterop runs a Go and a Java client process next to an in-process client and SIGKILLs
// the driver: all three must report one DriverTimeoutException-equivalent and close, and a Go and a Java process
// started on the restarted driver must exchange messages with each other over UDP.
func TestPeersDriverSIGKILLAndInterop(t *testing.T) {
	requireAdv(t)
	startDriver(t, plain)
	a, o := connect(t)
	goPeer := startGoPeer(t, goArgs(map[string]string{"role": "pubsub", "channel": "aeron:ipc", "stream": "9070"})...)
	javaPeer := startJavaPeer(t, peerArgs("java", 9071, driverTimeout, nil)...)
	goPeer.await("ready", 30*time.Second)
	javaPeer.await("ready", 30*time.Second)

	signalDriver(t, plain, "KILL")
	awaitTrue(t, "in-process client detects driver death", driverTimeout+slack, a.IsClosed)
	for _, p := range []*peer{goPeer, javaPeer} {
		p.await("terminated", driverTimeout+slack)
		require.Equal(t, outcome{errors: "DriverTimeout", closed: 1, terminated: true, offerAfter: "-4"}, p.outcome(),
			"%s peer", p.lang)
		require.True(t, p.exited(10*time.Second), "%s peer did not exit after Close", p.lang)
		p.await("exit", time.Second)
	}
	require.Len(t, o.errors(), 1)
	require.Equal(t, "DriverTimeout", classify(o.errors()[0]))

	restartDriver(t, plain)
	ch := "aeron:udp?endpoint=" + udp(72)
	goSub := startGoPeer(t, goArgs(map[string]string{"role": "sub", "channel": ch, "stream": "9072"})...)
	javaPub := startJavaPeer(t, peerArgs("java", 9072, driverTimeout, map[string]string{"role": "pub", "channel": ch})...)
	goSub.await("ready", 30*time.Second)
	javaPub.await("ready", 30*time.Second)
	awaitTrue(t, "Go subscriber receives from Java publisher after restart", 15*time.Second, func() bool {
		e := goSub.last("progress")
		return e != nil && e.int("received") > 500
	})
	require.Equal(t, int64(1), int64(goSub.count("available")))
}

func goArgs(opts map[string]string) []string {
	args := []string{"-driverTimeout=" + driverTimeout.String()}
	for k, v := range opts {
		args = append(args, "-"+k+"="+v)
	}
	return args
}

func itoa(i int) string { return strconv.Itoa(i) }
