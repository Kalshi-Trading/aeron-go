package networkfaults

import (
	"fmt"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	ab "github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/interop"
	"github.com/stretchr/testify/require"
)

// topology is a publication channel (+ manual destinations) and subscription channel (+ destinations) whose UDP
// ports are listed in ports so a netem rule can degrade every path between publisher and subscriber.
type topology struct {
	pub, sub           string
	pubDests, subDests []string
	ports              []int
}

func topologies() []struct {
	name string
	make func() topology
} {
	return []struct {
		name string
		make func() topology
	}{
		{"unicast", func() topology {
			p := port()
			return topology{pub: udp("localhost", p, ""), sub: udp("localhost", p, ""), ports: []int{p}}
		}},
		{"multicast", func() topology {
			p := port()
			ch := fmt.Sprintf("aeron:udp?endpoint=224.20.30.39:%d|interface=localhost", p)
			return topology{pub: ch, sub: ch, ports: []int{p}}
		}},
		{"mdc-dynamic", func() topology {
			c, d := port(), port()
			return topology{pub: fmt.Sprintf("aeron:udp?control=localhost:%d|control-mode=dynamic", c),
				sub: fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d", d, c), ports: []int{c, d}}
		}},
		{"mdc-manual", func() topology {
			d := port()
			return topology{pub: "aeron:udp?control-mode=manual", pubDests: []string{udp("localhost", d, "")},
				sub: udp("localhost", d, ""), ports: []int{d}}
		}},
		{"multi-destination-sub", func() topology {
			d1, d2 := port(), port()
			return topology{pub: "aeron:udp?control-mode=manual",
				pubDests: []string{udp("localhost", d1, ""), udp("localhost", d2, "")},
				sub:      "aeron:udp?control-mode=manual",
				subDests: []string{udp("localhost", d1, ""), udp("localhost", d2, "")}, ports: []int{d1, d2}}
		}},
	}
}

func javaDestArgs(dests []string) []string {
	var args []string
	for _, d := range dests {
		args = append(args, "dest="+d)
	}
	return args
}

var crossSizes = interop.Sizes{64, 1500, 40, 9000, 300}

const crossCount = 3000

// TestJavaPublisherToGoSubscriberUnderLoss: a Java 1.53.3 publisher on driver a, a Go subscriber on driver b, 10%
// loss, reordering and duplication on every path, over each topology. Go must receive every message exactly once,
// in order, with the Java payload digest.
func TestJavaPublisherToGoSubscriberUnderLoss(t *testing.T) {
	for _, tp := range topologies() {
		tp := tp
		t.Run(tp.name, func(t *testing.T) {
			requireStack(t)
			b := connect(t, dirB)
			topo, stream := tp.make(), streamID()
			sub, err := b.AddSubscription(topo.sub, stream)
			require.NoError(t, err)
			for _, d := range topo.subDests {
				require.NoError(t, sub.AddDestination(d))
			}
			netem(t, toBoth("loss 10% delay 1ms 1ms reorder 10% duplicate 5%", topo.ports...))
			args := append([]string{"channel=" + topo.pub, fmt.Sprintf("stream=%d", stream),
				fmt.Sprintf("count=%d", crossCount), "sizes=" + crossSizes.String(), "reserved=true", "timeoutMs=90000"},
				javaDestArgs(topo.pubDests)...)
			java := startJava(t, dirA, "pub", args...)
			java.await("connected", 30*time.Second)

			v := interop.NewVerifier(crossSizes, true)
			fa := aeron.NewFragmentAssembler(v.OnFragment, aeron.DefaultFragmentAssemblyBufferLength)
			deadline := time.Now().Add(90 * time.Second)
			for v.Count() < crossCount && time.Now().Before(deadline) {
				if sub.Poll(fa.OnFragment, 64) == 0 {
					time.Sleep(100 * time.Microsecond)
				}
			}
			r := v.Result(v.Count() < crossCount)
			t.Logf("%s: %+v naks=%d", tp.name, r, systemCounter(b, "NAKs sent"))
			require.False(t, r.TimedOut, "%+v\n%s", r, netemStats(t))
			require.Zero(t, r.Errors, r.FirstError)
			require.Equal(t, interop.ExpectedDigest(crossSizes, crossCount), r.SHA256)
			done := java.await("done", 90*time.Second)
			require.Equal(t, done.int("position"), r.Position)
			require.Empty(t, b.errors.all())
			java.send("exit")
		})
	}
}

// TestGoPublisherToJavaSubscriberUnderLoss: the mirror image, a Go publisher on driver a and a Java subscriber on
// driver b, verified by the Java side.
func TestGoPublisherToJavaSubscriberUnderLoss(t *testing.T) {
	for _, tp := range topologies() {
		tp := tp
		for _, exclusive := range []bool{false, true} {
			exclusive := exclusive
			t.Run(fmt.Sprintf("%s/exclusive=%v", tp.name, exclusive), func(t *testing.T) {
				requireStack(t)
				a := connect(t, dirA)
				topo, stream := tp.make(), streamID()
				args := append([]string{"channel=" + topo.sub, fmt.Sprintf("stream=%d", stream),
					fmt.Sprintf("count=%d", crossCount), "sizes=" + crossSizes.String(), "reserved=true",
					"timeoutMs=90000"}, javaDestArgs(topo.subDests)...)
				java := startJava(t, dirB, "sub", args...)
				java.await("ready", 30*time.Second)
				var pub aeron.Publisher
				var err error
				if exclusive {
					pub, err = a.AddExclusivePublication(topo.pub, stream)
				} else {
					pub, err = a.AddPublication(topo.pub, stream)
				}
				require.NoError(t, err)
				for _, d := range topo.pubDests {
					require.NoError(t, pub.AddDestination(d))
				}
				awaitTrue(t, "publication connected", 30*time.Second, pub.IsConnected)
				netem(t, toBoth("loss 10% delay 1ms 1ms reorder 10% duplicate 5%", topo.ports...))

				buf := ab.MakeBuffer(make([]byte, crossSizes.Max()))
				deadline := time.Now().Add(90 * time.Second)
				for seq := int64(0); seq < crossCount; seq++ {
					n := crossSizes.Length(seq)
					interop.FillBuffer(buf, 0, seq, n)
					rv := interop.ReservedValue(seq)
					for {
						r := pub.Offer(buf, 0, n, func(*ab.Buffer, int32, int32) int64 { return rv })
						if r > 0 {
							break
						}
						require.Contains(t, []int64{aeron.BackPressured, aeron.AdminAction, aeron.NotConnected}, r)
						require.True(t, time.Now().Before(deadline), "offer seq %d stuck: %d", seq, r)
						time.Sleep(50 * time.Microsecond)
					}
				}
				r := java.await("result", 100*time.Second).result()
				t.Logf("%s: %+v", tp.name, r)
				require.False(t, r.TimedOut, "%+v\n%s", r, netemStats(t))
				require.Zero(t, r.Errors, r.FirstError)
				require.Equal(t, int64(crossCount), r.Count)
				require.Equal(t, interop.ExpectedDigest(crossSizes, crossCount), r.SHA256)
				require.Equal(t, pub.Position(), r.Position)
				require.Empty(t, a.errors.all())
			})
		}
	}
}
