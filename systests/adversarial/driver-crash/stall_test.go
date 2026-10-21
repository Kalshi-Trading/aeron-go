package drivercrash

import (
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// peerArgs starts the same client in both languages: a publishing and subscribing IPC client on stream, with the
// given driver timeout.
func peerArgs(lang string, stream int, timeout time.Duration, extra map[string]string) []string {
	opts := map[string]string{"role": "pubsub", "channel": "aeron:ipc", "stream": fmt.Sprint(stream), "linger": "2s"}
	for k, v := range extra {
		opts[k] = v
	}
	var args []string
	if lang == "go" {
		args = append(args, "-driverTimeout="+timeout.String())
		for k, v := range opts {
			args = append(args, "-"+k+"="+v)
		}
		return args
	}
	args = append(args, fmt.Sprintf("driverTimeoutMs=%d", timeout.Milliseconds()))
	for k, v := range opts {
		switch k {
		case "linger":
			d, _ := time.ParseDuration(v)
			args = append(args, fmt.Sprintf("lingerMs=%d", d.Milliseconds()))
		case "interval":
			d, _ := time.ParseDuration(v)
			args = append(args, fmt.Sprintf("intervalUs=%d", d.Microseconds()))
		default:
			args = append(args, k+"="+v)
		}
	}
	return args
}

// outcome is what a client process reported about a fault: its error-handler calls (by Java exception name), whether
// its close handler ran, whether its client terminated, and what an Offer returned afterwards.
type outcome struct {
	errors     string
	closed     int
	terminated bool
	offerAfter string
}

func (p *peer) outcome() outcome {
	var types []string
	for _, e := range p.all() {
		// Java's AgentRunner also hands the AgentTerminationException it uses to stop the conductor thread to the
		// error handler after the real error; it is an artifact of Java's threading model, not a client error.
		if (e.name() == "error" || e.name() == "exception") && e.str("type") != "AgentTerminationException" {
			types = append(types, e.str("type"))
		}
	}
	o := outcome{errors: strings.Join(types, ","), closed: p.count("closed")}
	if e := p.find("terminated"); e != nil {
		o.terminated = true
		o.offerAfter = e.str("offerAfter")
	}
	return o
}

// TestStallVersusJava SIGSTOPs the driver or the client process for different lengths relative to the client
// liveness timeout (driver side, 2s) and the driver timeout (client side), SIGCONTs it, and requires the Go client to
// react exactly like the Java 1.53.3 client running the same scenario on the same driver at the same time:
//
//   - driver stopped longer than the driver timeout: both clients raise DriverTimeoutException and close;
//   - driver stopped longer than the client liveness timeout but shorter than the driver timeout: the clients keep
//     their heartbeat fresh, so the resumed driver keeps them and traffic resumes with no error;
//   - client stopped longer than the client liveness timeout: the driver times the client out, but once resumed the
//     client's own service-interval check (inter-service timeout = client liveness timeout) runs before it reads the
//     driver's ON_CLIENT_TIMEOUT, so Java raises ConductorServiceTimeoutException, not ClientTimeoutException, and
//     closes (Offer then returns CLOSED). ClientTimeoutException itself is covered by TestClientTimeoutFromDriver.
//
// After each scenario a fresh Go client on the same driver must be able to publish and receive.
func TestStallVersusJava(t *testing.T) {
	requireAdv(t)
	scenarios := []struct {
		name       string
		timeout    time.Duration
		stop       func(t *testing.T, peers []*peer)
		cont       func(t *testing.T, peers []*peer)
		pause      time.Duration
		want       outcome
		driverDown bool
	}{
		{
			name: "driver-stopped-past-driver-timeout", timeout: 2 * time.Second, pause: 5 * time.Second,
			stop: func(t *testing.T, _ []*peer) { signalDriver(t, plain, "STOP") },
			cont: func(t *testing.T, _ []*peer) { signalDriver(t, plain, "CONT") },
			want: outcome{errors: "DriverTimeout", closed: 1, terminated: true, offerAfter: "-4"},
		},
		{
			name: "driver-stopped-past-client-liveness-only", timeout: 10 * time.Second, pause: 4 * time.Second,
			stop: func(t *testing.T, _ []*peer) { signalDriver(t, plain, "STOP") },
			cont: func(t *testing.T, _ []*peer) { signalDriver(t, plain, "CONT") },
			want: outcome{},
		},
		{
			name: "client-stopped-past-client-liveness", timeout: 10 * time.Second, pause: 4 * time.Second,
			stop: func(t *testing.T, peers []*peer) {
				for _, p := range peers {
					p.signal(syscall.SIGSTOP)
				}
			},
			cont: func(t *testing.T, peers []*peer) {
				for _, p := range peers {
					p.signal(syscall.SIGCONT)
				}
			},
			want: outcome{errors: "ConductorServiceTimeout", closed: 1, terminated: true, offerAfter: "-4"},
		},
	}
	for i, sc := range scenarios {
		sc, i := sc, i
		t.Run(sc.name, func(t *testing.T) {
			startDriver(t, plain)
			goPeer := startGoPeer(t, peerArgs("go", 9040+i*2, sc.timeout, nil)...)
			javaPeer := startJavaPeer(t, peerArgs("java", 9041+i*2, sc.timeout, nil)...)
			peers := []*peer{goPeer, javaPeer}
			for _, p := range peers {
				p.await("ready", 30*time.Second)
			}
			awaitTrue(t, "peers exchanging messages", 10*time.Second, func() bool {
				for _, p := range peers {
					if e := p.last("progress"); e == nil || e.int("received") < 100 {
						return false
					}
				}
				return true
			})

			sc.stop(t, peers)
			time.Sleep(sc.pause)
			sc.cont(t, peers)
			// Long enough for a resumed driver to time out stale clients (liveness 2s + timer interval) and for the
			// clients to see it.
			time.Sleep(clientLiveness + 2*time.Second)

			for _, p := range peers {
				if sc.want.terminated {
					p.await("terminated", 10*time.Second)
				}
			}
			for _, p := range peers {
				t.Logf("%s client: %+v", p.lang, p.outcome())
			}
			for _, p := range peers {
				got := p.outcome()
				require.Equal(t, sc.want, got, "%s client outcome", p.lang)
				if !sc.want.terminated {
					before := p.last("progress").int("received")
					awaitTrue(t, p.lang+" traffic resumes", 10*time.Second, func() bool {
						return p.last("progress").int("received") > before+100
					})
				}
			}
			require.Equal(t, javaPeer.outcome(), goPeer.outcome(), "Go must react like Java")

			a, _ := connect(t)
			f := startFlow(t, a, specs(3+i)[0])
			f.awaitProgress(t, 500, 10*time.Second)
			f.stop()
			f.requireIntact(t)
		})
	}
}
