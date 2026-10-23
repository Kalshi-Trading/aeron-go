package interop

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/stretchr/testify/require"
)

// channels describes one transport configuration of the matrix.
type channels struct {
	pub      string
	pubDests []string // MDC manual destinations added to the publication
	sub      string
	subDests []string // MDS destinations added to the subscription
}

type transport struct {
	name  string
	build func() channels
}

func udp(port int) string { return fmt.Sprintf("aeron:udp?endpoint=localhost:%d", port) }

func transports() []transport {
	ts := []transport{
		{"ipc", func() channels { return channels{pub: "aeron:ipc", sub: "aeron:ipc"} }},
		{"udp-unicast", func() channels { p := nextPort(); return channels{pub: udp(p), sub: udp(p)} }},
		{"mdc-dynamic", func() channels {
			ctrl, data := nextPort(), nextPort()
			return channels{
				pub: fmt.Sprintf("aeron:udp?control=localhost:%d|control-mode=dynamic", ctrl),
				sub: fmt.Sprintf("aeron:udp?endpoint=localhost:%d|control=localhost:%d", data, ctrl),
			}
		}},
		{"mdc-manual", func() channels {
			p := nextPort()
			return channels{pub: "aeron:udp?control-mode=manual", pubDests: []string{udp(p)}, sub: udp(p)}
		}},
		{"mds", func() channels {
			p1, p2 := nextPort(), nextPort()
			return channels{
				pub: "aeron:udp?control-mode=manual", pubDests: []string{udp(p1), udp(p2)},
				sub: "aeron:udp?control-mode=manual", subDests: []string{udp(p1), udp(p2)},
			}
		}},
		{"spy", func() channels {
			p := nextPort()
			ch := udp(p) + "|ssc=true"
			return channels{pub: ch, sub: "aeron-spy:" + ch}
		}},
	}
	iface := os.Getenv("AERON_INTEROP_MCAST_IFACE")
	if iface == "" {
		iface = "localhost"
	}
	if iface != "off" {
		ts = append(ts, transport{"udp-multicast", func() channels {
			ch := fmt.Sprintf("aeron:udp?endpoint=224.20.30.39:%d|interface=%s", nextPort(), iface)
			return channels{pub: ch, sub: ch}
		}})
	}
	return ts
}

type variant struct {
	name      string
	o         msgOpts
	exclusive bool
}

func variants() []variant {
	return []variant{
		{name: "offer", o: msgOpts{count: 10_000, sizes: Sizes{64}}},
		{name: "offer-reserved", o: msgOpts{count: 5_000, sizes: Sizes{8, 200, 1376, 3000}, reserved: true}},
		{name: "tryclaim-reserved", o: msgOpts{count: 5_000, sizes: Sizes{8, 100, 1376}, claim: true, reserved: true}},
		{name: "fragmented-1MB", o: msgOpts{count: 24, sizes: Sizes{1 << 20, 100, 300_001}, reserved: true}},
		{name: "exclusive", o: msgOpts{count: 5_000, sizes: Sizes{64, 5000}}, exclusive: true},
		{name: "exclusive-tryclaim", o: msgOpts{count: 5_000, sizes: Sizes{32, 1000}, claim: true, reserved: true}, exclusive: true},
	}
}

func destArgs(dests []string) []string {
	var args []string
	for _, d := range dests {
		args = append(args, "dest="+d)
	}
	return args
}

func TestPubSubMatrix(t *testing.T) {
	requireE2E(t)
	for _, tr := range transports() {
		for _, v := range variants() {
			tr, v := tr, v
			t.Run(fmt.Sprintf("java-to-go/%s/%s", tr.name, v.name), func(t *testing.T) { javaToGo(t, tr.build(), v) })
			t.Run(fmt.Sprintf("go-to-java/%s/%s", tr.name, v.name), func(t *testing.T) { goToJava(t, tr.build(), v) })
		}
	}
}

// javaToGo: Java publication -> Go subscription.
func javaToGo(t *testing.T, ch channels, v variant) {
	a := connectGo(t)
	stream := nextStream()
	sub, err := a.AddSubscription(ch.sub, stream)
	require.NoError(t, err)
	for _, d := range ch.subDests {
		require.NoError(t, sub.AddDestination(d))
	}
	args := append([]string{"channel=" + ch.pub, fmt.Sprintf("stream=%d", stream),
		fmt.Sprintf("exclusive=%t", v.exclusive)}, destArgs(ch.pubDests)...)
	j := startJava(t, "pub", append(args, v.o.javaArgs()...)...)
	j.waitFor("connected", 20*time.Second)

	start := time.Now()
	r := goReceive(sub.Poll, v.o)
	done := j.waitFor("done", v.o.timeoutOrDefault())
	j.send("exit")
	t.Logf("go received %d msgs / %d bytes in %v", r.Count, r.Bytes, time.Since(start))
	requireResult(t, "go subscriber", r, v.o, done.int("position"))
}

// goToJava: Go publication -> Java subscription.
func goToJava(t *testing.T, ch channels, v variant) {
	a := connectGo(t)
	stream := nextStream()
	var pub aeron.Publisher
	var err error
	if v.exclusive {
		pub, err = a.AddExclusivePublication(ch.pub, stream)
	} else {
		pub, err = a.AddPublication(ch.pub, stream)
	}
	require.NoError(t, err)
	defer pub.Close()
	for _, d := range ch.pubDests {
		require.NoError(t, pub.AddDestination(d))
	}
	args := append([]string{"channel=" + ch.sub, fmt.Sprintf("stream=%d", stream)}, destArgs(ch.subDests)...)
	j := startJava(t, "sub", append(args, v.o.javaArgs()...)...)
	j.waitFor("ready", 20*time.Second)
	j.waitFor("image", 20*time.Second)
	require.NoError(t, awaitConnected(pub, 10*time.Second))

	start := time.Now()
	position, err := goPublish(pub, v.o)
	require.NoError(t, err)
	r := j.waitFor("result", v.o.timeoutOrDefault()).result()
	t.Logf("java received %d msgs / %d bytes in %v", r.Count, r.Bytes, time.Since(start))
	requireResult(t, "java subscriber", r, v.o, position)
}
