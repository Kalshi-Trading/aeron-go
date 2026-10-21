package interop

import (
	"fmt"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/stretchr/testify/require"
)

// responseTransport is a request channel (subscribed by the server, published to by the client with
// response-correlation-id) and a response channel (control-mode=response, subscribed by the client and published
// to by the server with response-correlation-id=<request image correlation id>).
type responseTransport struct {
	name     string
	request  func() string
	response func() string
}

func responseTransports() []responseTransport {
	return []responseTransport{
		{"udp", func() string { return udp(nextPort()) },
			func() string { return fmt.Sprintf("aeron:udp?control-mode=response|control=localhost:%d", nextPort()) }},
		{"udp-fragmented", func() string { return udp(nextPort()) + "|mtu=1408|term-length=65536" },
			func() string {
				return fmt.Sprintf("aeron:udp?control-mode=response|control=localhost:%d|mtu=1408|term-length=65536",
					nextPort())
			}},
		{"ipc", func() string { return "aeron:ipc" }, func() string { return "aeron:ipc?control-mode=response" }},
	}
}

var responseMsgs = msgOpts{count: 2000, sizes: Sizes{8, 200, 1376, 3000}, reserved: true}

// Go response client (control-mode=response subscription + request publication with response-correlation-id)
// against a Java response server.
func TestResponseChannelGoClientJavaServer(t *testing.T) {
	requireE2E(t)
	for _, tr := range responseTransports() {
		for _, exclusive := range []bool{false, true} {
			tr, exclusive := tr, exclusive
			t.Run(fmt.Sprintf("%s/exclusive=%t", tr.name, exclusive), func(t *testing.T) {
				a := connectGo(t)
				reqCh, respCh := tr.request(), tr.response()
				reqStream, respStream := nextStream(), nextStream()
				j := startJava(t, "respserver", append([]string{"channel=" + reqCh, fmt.Sprintf("stream=%d", reqStream),
					"respChannel=" + respCh, fmt.Sprintf("respStream=%d", respStream),
					fmt.Sprintf("exclusive=%t", exclusive)}, responseMsgs.javaArgs()...)...)
				j.waitFor("ready", 20*time.Second)

				sub, err := a.AddSubscription(respCh, respStream)
				require.NoError(t, err)
				defer sub.Close()
				pubCh := withParams(reqCh, fmt.Sprintf("response-correlation-id=%d", sub.RegistrationID()))
				var pub aeron.Publisher
				if exclusive {
					pub, err = a.AddExclusivePublication(pubCh, reqStream)
				} else {
					pub, err = a.AddPublication(pubCh, reqStream)
				}
				require.NoError(t, err)
				defer pub.Close()

				req := j.waitFor("request", 20*time.Second)
				require.Equal(t, int64(pub.SessionID()), req.int("sessionId"), "request image session id")
				require.NoError(t, awaitConnected(pub, 10*time.Second))
				require.NoError(t, awaitSubConnected(sub, 10*time.Second))
				img := sub.ImageBySessionID(int32(req.int("responseSessionId")))
				require.NotNil(t, img, "response image for session %d", req.int("responseSessionId"))

				position, err := goPublish(pub, responseMsgs)
				require.NoError(t, err)
				requireResult(t, "java server (requests)", j.waitFor("result", defaultTimeout).result(), responseMsgs,
					position)
				r := goReceive(sub.Poll, responseMsgs)
				done := j.waitFor("done", defaultTimeout)
				requireResult(t, "go client (responses)", r, responseMsgs, done.int("position"))
				require.Equal(t, int32(req.int("responseSessionId")), r.SessionID)
				j.send("exit")
			})
		}
	}
}

// Java response client against a Go response server, which publishes responses on
// <response channel>|response-correlation-id=<request image CorrelationID()>.
func TestResponseChannelJavaClientGoServer(t *testing.T) {
	requireE2E(t)
	for _, tr := range responseTransports() {
		for _, exclusive := range []bool{false, true} {
			tr, exclusive := tr, exclusive
			t.Run(fmt.Sprintf("%s/exclusive=%t", tr.name, exclusive), func(t *testing.T) {
				a := connectGo(t)
				reqCh, respCh := tr.request(), tr.response()
				reqStream, respStream := nextStream(), nextStream()
				sub, err := a.AddSubscription(reqCh, reqStream)
				require.NoError(t, err)
				defer sub.Close()

				j := startJava(t, "respclient", append([]string{"channel=" + reqCh, fmt.Sprintf("stream=%d", reqStream),
					"respChannel=" + respCh, fmt.Sprintf("respStream=%d", respStream),
					fmt.Sprintf("exclusive=%t", exclusive)}, responseMsgs.javaArgs()...)...)
				ready := j.waitFor("ready", 20*time.Second)
				require.NoError(t, awaitSubConnected(sub, 20*time.Second))
				img := sub.ImageBySessionID(int32(ready.int("sessionId")))
				require.NotNil(t, img, "request image for session %d", ready.int("sessionId"))

				pubCh := withParams(respCh, fmt.Sprintf("response-correlation-id=%d", img.CorrelationID()))
				var pub aeron.Publisher
				if exclusive {
					pub, err = a.AddExclusivePublication(pubCh, respStream)
				} else {
					pub, err = a.AddPublication(pubCh, respStream)
				}
				require.NoError(t, err)
				defer pub.Close()
				require.NoError(t, awaitConnected(pub, 10*time.Second))

				r := goReceive(img.Poll, responseMsgs)
				done := j.waitFor("done", defaultTimeout)
				requireResult(t, "go server (requests)", r, responseMsgs, done.int("position"))
				position, err := goPublish(pub, responseMsgs)
				require.NoError(t, err)
				jr := j.waitFor("result", defaultTimeout).result()
				requireResult(t, "java client (responses)", jr, responseMsgs, position)
				require.Equal(t, pub.SessionID(), jr.SessionID)
			})
		}
	}
}
