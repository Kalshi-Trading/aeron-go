package interop

import (
	"fmt"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/stretchr/testify/require"
)

const fcGroupTag = 4242

type fcCase struct {
	name string
	fc   string // fc= value on the publication
	gtag bool   // receivers carry gtag=fcGroupTag
	// groupMinSize: publication must not be connected until this many receivers are present (0 = not checked)
	groupMinSize int
}

func fcCases() []fcCase {
	return []fcCase{
		{name: "min", fc: "min"},
		{name: "min-group2", fc: "min,g:/2", groupMinSize: 2},
		{name: "max", fc: "max"},
		{name: "tagged", fc: fmt.Sprintf("tagged,g:%d", fcGroupTag), gtag: true},
		{name: "tagged-group2", fc: fmt.Sprintf("tagged,g:%d/2", fcGroupTag), gtag: true, groupMinSize: 2},
	}
}

func (c fcCase) channels(ctrl int) (pub, sub string) {
	pub = fmt.Sprintf("aeron:udp?control=localhost:%d|control-mode=dynamic|fc=%s", ctrl, c.fc)
	sub = fmt.Sprintf("aeron:udp?endpoint=localhost:0|control=localhost:%d|control-mode=dynamic", ctrl)
	if c.gtag {
		sub += fmt.Sprintf("|gtag=%d", fcGroupTag)
	}
	return pub, sub
}

var fcMsgs = msgOpts{count: 3000, sizes: Sizes{100, 1500, 40, 9000}, reserved: true}

// Go MDC publication with min/max/tagged flow control, one Go and one Java receiver.
func TestFlowControlGoPubMixedReceivers(t *testing.T) {
	requireE2E(t)
	for _, c := range fcCases() {
		t.Run(c.name, func(t *testing.T) {
			a := connectGo(t)
			stream := nextStream()
			pubCh, subCh := c.channels(nextPort())
			pub, err := a.AddPublication(pubCh, stream)
			require.NoError(t, err)
			defer pub.Close()

			sub, err := a.AddSubscription(subCh, stream)
			require.NoError(t, err)
			defer sub.Close()
			require.NoError(t, awaitSubConnected(sub, 10*time.Second))
			if c.groupMinSize > 1 {
				time.Sleep(300 * time.Millisecond)
				require.False(t, pub.IsConnected(), "connected with 1 receiver below group min size %d", c.groupMinSize)
			}

			j := startJava(t, "sub", append([]string{"channel=" + subCh, fmt.Sprintf("stream=%d", stream)},
				fcMsgs.javaArgs()...)...)
			j.waitFor("image", 20*time.Second)
			require.NoError(t, awaitConnected(pub, 10*time.Second))

			goRes := make(chan Result, 1)
			go func() { goRes <- goReceive(sub.Poll, fcMsgs) }()
			position, err := goPublish(pub, fcMsgs)
			require.NoError(t, err)
			requireResult(t, "go receiver", <-goRes, fcMsgs, position)
			requireResult(t, "java receiver", j.waitFor("result", defaultTimeout).result(), fcMsgs, position)
		})
	}
}

// Java MDC publication with min/max/tagged flow control, one Go and one Java receiver.
func TestFlowControlJavaPubMixedReceivers(t *testing.T) {
	requireE2E(t)
	for _, c := range fcCases() {
		t.Run(c.name, func(t *testing.T) {
			a := connectGo(t)
			stream := nextStream()
			pubCh, subCh := c.channels(nextPort())
			jp := startJava(t, "pub", append([]string{"channel=" + pubCh, fmt.Sprintf("stream=%d", stream),
				"waitGo=true"}, fcMsgs.javaArgs()...)...)
			jp.waitFor("ready", 20*time.Second)

			sub, err := a.AddSubscription(subCh, stream)
			require.NoError(t, err)
			defer sub.Close()
			require.NoError(t, awaitSubConnected(sub, 10*time.Second))
			if c.groupMinSize > 1 {
				_, err := jp.tryWaitFor("connected", 300*time.Millisecond)
				require.Error(t, err, "java pub connected with 1 receiver below group min size %d", c.groupMinSize)
			}

			js := startJava(t, "sub", append([]string{"channel=" + subCh, fmt.Sprintf("stream=%d", stream)},
				fcMsgs.javaArgs()...)...)
			js.waitFor("image", 20*time.Second)
			jp.waitFor("connected", 20*time.Second)

			goRes := make(chan Result, 1)
			go func() { goRes <- goReceive(sub.Poll, fcMsgs) }()
			jp.send("go")
			position := jp.waitFor("done", defaultTimeout).int("position")
			requireResult(t, "go receiver", <-goRes, fcMsgs, position)
			requireResult(t, "java receiver", js.waitFor("result", defaultTimeout).result(), fcMsgs, position)
			jp.send("exit")
		})
	}
}

var _ = aeron.NullValue
