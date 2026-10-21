package clientcrash

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/interop"
	"github.com/stretchr/testify/require"
)

// verifyingSub polls a subscription, verifying each image with the interop payload Verifier (Java InteropTool
// messages), and records unavailable images.
type verifyingSub struct {
	mu          sync.Mutex
	verifiers   map[int32]*interop.Verifier
	unavailable map[int32]bool
	stop, done  chan struct{}
}

func newVerifyingSub(t *testing.T, a *aeron.Aeron, channel string, stream int32, sizes interop.Sizes) *verifyingSub {
	v := &verifyingSub{verifiers: map[int32]*interop.Verifier{}, unavailable: map[int32]bool{},
		stop: make(chan struct{}), done: make(chan struct{})}
	sub, err := a.AddSubscriptionWithHandlers(channel, stream, nil, func(img aeron.Image) {
		v.mu.Lock()
		v.unavailable[img.SessionID()] = true
		v.mu.Unlock()
	})
	require.NoError(t, err)
	assembler := aeron.NewFragmentAssembler(func(buf *ab.Buffer, offset, length int32, h *logbuffer.Header) {
		v.mu.Lock()
		ver := v.verifiers[h.SessionId()]
		if ver == nil {
			ver = interop.NewVerifier(sizes, false)
			v.verifiers[h.SessionId()] = ver
		}
		ver.OnFragment(buf, offset, length, h)
		v.mu.Unlock()
	}, aeron.DefaultFragmentAssemblyBufferLength)
	go func() {
		defer close(v.done)
		for {
			select {
			case <-v.stop:
				return
			default:
			}
			if sub.Poll(assembler.OnFragment, 20) == 0 {
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()
	t.Cleanup(v.close)
	return v
}

func (v *verifyingSub) close() {
	select {
	case <-v.stop:
	default:
		close(v.stop)
	}
	<-v.done
}

func (v *verifyingSub) result(session int32) (interop.Result, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	ver := v.verifiers[session]
	if ver == nil {
		return interop.Result{}, v.unavailable[session]
	}
	return ver.Result(false), v.unavailable[session]
}

// TestKillJavaPublisher SIGKILLs a Java 1.53.3 InteropTool publisher (concurrent offer, exclusive tryClaim,
// fragmented messages) mid-stream. The Go subscriber must recover exactly as for a dead Go publisher: every
// received message verifies (no gap/duplicate/corruption), the image goes unavailable, the driver drops the Java
// client's publication counters and heartbeat, and a replacement Java publisher is received from its first message.
func TestKillJavaPublisher(t *testing.T) {
	for _, c := range []struct {
		name, channel, subChan, sizes string
		stream                        int32
		extra                         []string
	}{
		{"udp-offer-fragmented", "aeron:udp?endpoint=localhost:27150|alias=advk-java-frag",
			"aeron:udp?endpoint=localhost:27150", "64,3000,20000", 9150, nil},
		{"ipc-exclusive-claim", "aeron:ipc?alias=advk-java-claim", "aeron:ipc", "64,500", 9151,
			[]string{"exclusive=true", "mode=claim"}},
		{"mdc-dynamic", "aeron:udp?control=localhost:27151|control-mode=dynamic|alias=advk-java-mdc",
			"aeron:udp?endpoint=localhost:27152|control=localhost:27151|control-mode=dynamic", "64", 9152, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			requireAdv(t)
			leaks := baseline()
			sizes, err := interop.ParseSizes(c.sizes)
			require.NoError(t, err)
			a := connect(t, newContext())
			v := newVerifyingSub(t, a, c.subChan, c.stream, sizes)
			heartbeats := len(counterLabels(a, "client-heartbeat"))
			startPub := func(label string) (*peer, int32) {
				p := startJava(t, label, "pub", append([]string{"channel=" + c.channel,
					fmt.Sprintf("stream=%d", c.stream), "sizes=" + c.sizes, "count=1000000000", "delayUs=100",
					"timeoutMs=600000"}, c.extra...)...)
				session := int32(p.await("ready", 60*time.Second).int("sessionId"))
				p.await("connected", 30*time.Second)
				awaitTrue(t, "Java messages", 20*time.Second, func() bool {
					r, _ := v.result(session)
					require.Zero(t, r.Errors, r.FirstError)
					return r.Count > 1000
				})
				return p, session
			}
			p, session := startPub("java-publisher")
			p.kill()
			awaitTrue(t, "driver to reclaim the Java client", reclaimTO, func() bool {
				return len(counterLabels(a, alias(c.channel))) == 0 &&
					len(counterLabels(a, "client-heartbeat")) == heartbeats
			})
			awaitTrue(t, "unavailable Java image", reclaimTO, func() bool {
				_, unavailable := v.result(session)
				return unavailable
			})
			r, _ := v.result(session)
			require.Zero(t, r.Errors, r.FirstError)
			t.Logf("received %d verified messages before the image went unavailable", r.Count)

			replacement, s2 := startPub("java-replacement")
			require.NotEqual(t, session, s2)
			replacement.kill()
			v.close()
			require.NoError(t, a.Close())
			leaks.requireNoLeaks(t)
		})
	}
}
