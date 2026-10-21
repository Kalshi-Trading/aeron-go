package interop

import (
	"fmt"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/stretchr/testify/require"
)

// channelParamCase is a publication channel URI configuration exercised in both directions.
type channelParamCase struct {
	name      string
	pubParams string // appended to the publication channel
	subParams string // appended to the subscription channel
	exclusive bool
	o         msgOpts
	// joinPosition is the expected image join position (initial position cases), or -1 to skip the check.
	joinPosition int64
}

const (
	wrapInitTermID = int32(2147483640) // term ids wrap past math.MaxInt32 during the run
	wrapTermID     = int32(2147483646)
	wrapTermOffset = int32(65504)
	smallTerm      = int64(65536)
)

func channelParamCases() []channelParamCase {
	small := Sizes{8, 1376, 3000, 8000} // max message = term-length/8 = 8192
	return []channelParamCase{
		{name: "small-term-rotation", pubParams: "term-length=64k|mtu=1408",
			o: msgOpts{count: 3_000, sizes: small, reserved: true}, joinPosition: -1},
		{name: "initial-position-termid-wrap", exclusive: true,
			pubParams: fmt.Sprintf("term-length=64k|mtu=1408|init-term-id=%d|term-id=%d|term-offset=%d",
				wrapInitTermID, wrapTermID, wrapTermOffset),
			o:            msgOpts{count: 3_000, sizes: small, reserved: true},
			joinPosition: int64(wrapTermID-wrapInitTermID)*smallTerm + int64(wrapTermOffset)},
		{name: "session-id", pubParams: "session-id=424242", subParams: "session-id=424242",
			o: msgOpts{count: 2_000, sizes: Sizes{64, 2000}, reserved: true}, joinPosition: -1},
		{name: "mtu-8k", pubParams: "mtu=8192",
			o: msgOpts{count: 200, sizes: Sizes{8, 8160, 8161, 100_000}, reserved: true}, joinPosition: -1},
	}
}

// Go and Java agree on the wire for URI-configured publications: term rotation on small terms, an explicit
// initial position whose term ids overflow int32, a fixed session-id with a session-filtered subscription, and
// a large MTU.
func TestPubSubChannelParams(t *testing.T) {
	requireE2E(t)
	bases := map[string]func() string{
		"ipc": func() string { return "aeron:ipc" },
		"udp": func() string { return udp(nextPort()) },
	}
	for baseName, base := range bases {
		for _, c := range channelParamCases() {
			base, c := base, c
			t.Run(fmt.Sprintf("java-to-go/%s/%s", baseName, c.name), func(t *testing.T) {
				b := base()
				paramsJavaToGo(t, withParams(b, c.pubParams), withParams(b, c.subParams), c)
			})
			t.Run(fmt.Sprintf("go-to-java/%s/%s", baseName, c.name), func(t *testing.T) {
				b := base()
				paramsGoToJava(t, withParams(b, c.pubParams), withParams(b, c.subParams), c)
			})
		}
	}
}

func paramsJavaToGo(t *testing.T, pubCh, subCh string, c channelParamCase) {
	a := connectGo(t)
	stream := nextStream()
	sub, err := a.AddSubscription(subCh, stream)
	require.NoError(t, err)
	defer sub.Close()
	j := startJava(t, "pub", append([]string{"channel=" + pubCh, fmt.Sprintf("stream=%d", stream),
		fmt.Sprintf("exclusive=%t", c.exclusive)}, c.o.javaArgs()...)...)
	j.waitFor("connected", 20*time.Second)
	r := goReceive(sub.Poll, c.o)
	done := j.waitFor("done", c.o.timeoutOrDefault())
	j.send("exit")
	requireResult(t, "go subscriber", r, c.o, done.int("position"))
	if c.joinPosition >= 0 {
		require.Equal(t, c.joinPosition, sub.ImageBySessionID(r.SessionID).JoinPosition())
		require.Greater(t, done.int("position"), c.joinPosition)
	}
}

func paramsGoToJava(t *testing.T, pubCh, subCh string, c channelParamCase) {
	a := connectGo(t)
	stream := nextStream()
	var pub aeron.Publisher
	var err error
	if c.exclusive {
		pub, err = a.AddExclusivePublication(pubCh, stream)
	} else {
		pub, err = a.AddPublication(pubCh, stream)
	}
	require.NoError(t, err)
	defer pub.Close()
	j := startJava(t, "sub", append([]string{"channel=" + subCh, fmt.Sprintf("stream=%d", stream)},
		c.o.javaArgs()...)...)
	j.waitFor("ready", 20*time.Second)
	img := j.waitFor("image", 20*time.Second)
	require.NoError(t, awaitConnected(pub, 10*time.Second))
	if c.joinPosition >= 0 {
		require.Equal(t, c.joinPosition, img.int("joinPosition"))
		require.Equal(t, c.joinPosition, pub.Position())
	}
	position, err := goPublish(pub, c.o)
	require.NoError(t, err)
	r := j.waitFor("result", c.o.timeoutOrDefault()).result()
	requireResult(t, "java subscriber", r, c.o, position)
}
