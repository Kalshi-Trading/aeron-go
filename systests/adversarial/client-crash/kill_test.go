package clientcrash

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// pubKillCase is a crashpeer publisher configuration that is SIGKILLed mid-traffic.
type pubKillCase struct {
	name    string
	pubChan string // channel of the killed publisher (carries the alias tag)
	subChan string // channel of the surviving subscriber
	stream  int32
	kind    string
	size    int
	pubDest []string // MDC destinations added by the publisher
	subDest []string // MDS destinations added by the surviving subscriber
}

func pubKillCases() []pubKillCase {
	return []pubKillCase{
		{name: "ipc-concurrent", pubChan: "aeron:ipc?alias=advk-ipc-conc", subChan: "aeron:ipc",
			stream: 9101, kind: "concurrent", size: 64},
		{name: "ipc-offerblock", pubChan: "aeron:ipc?alias=advk-ipc-block", subChan: "aeron:ipc",
			stream: 9102, kind: "offerblock", size: 200},
		{name: "udp-exclusive", pubChan: "aeron:udp?endpoint=localhost:27101|alias=advk-udp-excl",
			subChan: "aeron:udp?endpoint=localhost:27101", stream: 9103, kind: "exclusive", size: 64},
		{name: "udp-tryclaim", pubChan: "aeron:udp?endpoint=localhost:27102|alias=advk-udp-claim",
			subChan: "aeron:udp?endpoint=localhost:27102", stream: 9104, kind: "tryclaim", size: 512},
		{name: "udp-fragmented", pubChan: "aeron:udp?endpoint=localhost:27103|alias=advk-udp-frag",
			subChan: "aeron:udp?endpoint=localhost:27103", stream: 9105, kind: "concurrent", size: 100_000},
		{name: "multicast", pubChan: "aeron:udp?endpoint=224.0.1.1:27104|interface=localhost|alias=advk-mcast",
			subChan: "aeron:udp?endpoint=224.0.1.1:27104|interface=localhost", stream: 9106, kind: "exclusive",
			size: 64},
		{name: "mdc-manual", pubChan: "aeron:udp?control-mode=manual|alias=advk-mdc-man",
			subChan: "aeron:udp?endpoint=localhost:27105", stream: 9107, kind: "concurrent", size: 64,
			pubDest: []string{"aeron:udp?endpoint=localhost:27105"}},
		{name: "mdc-dynamic", pubChan: "aeron:udp?control=localhost:27106|control-mode=dynamic|alias=advk-mdc-dyn",
			subChan: "aeron:udp?endpoint=localhost:27107|control=localhost:27106|control-mode=dynamic",
			stream:  9108, kind: "exclusive", size: 64},
		{name: "mds-subscription", pubChan: "aeron:udp?endpoint=localhost:27108|alias=advk-mds",
			subChan: "aeron:udp?control-mode=manual", stream: 9109, kind: "tryclaim", size: 64,
			subDest: []string{"aeron:udp?endpoint=localhost:27108"}},
	}
}

func (c pubKillCase) args() []string {
	args := []string{"-mode", "pub", "-channel", c.pubChan, "-stream", fmt.Sprint(c.stream), "-kind", c.kind,
		"-size", fmt.Sprint(c.size), "-interval", "200us"}
	for _, d := range c.pubDest {
		args = append(args, "-dest", d)
	}
	return args
}

// TestKillPublisherProcess SIGKILLs a Go publisher process of every publication type mid-traffic. As in Java, the
// driver must time the dead client out and drop its publication and counters, the surviving subscriber must get
// every message the publisher committed (no gap, duplicate or corruption) and then an unavailable image, and a
// replacement process on the same channel must be picked up as a new image starting at its first message.
func TestKillPublisherProcess(t *testing.T) {
	for _, c := range pubKillCases() {
		t.Run(c.name, func(t *testing.T) {
			requireAdv(t)
			leaks := baseline()
			a := connect(t, newContext())
			s := newLiveSub(t, a, c.subChan, c.stream, c.subDest...)

			p := startPeer(t, "publisher", c.args()...)
			ready := p.await("ready", 10*time.Second)
			session, clientID := int32(ready.int("sessionId")), ready.int("clientId")
			p.await("connected", 20*time.Second)
			s.awaitReceived(session, 200, 20*time.Second)
			require.NotEmpty(t, clientCounters(a, clientID), "driver counters of the live publisher client")

			p.kill()
			sent := p.last("sent").int("seq")
			reclaimed := awaitReclaimed(t, a, clientID, alias(c.pubChan))
			s.awaitReceived(session, sent, 5*time.Second)
			unavailable := s.awaitUnavailable(session, reclaimTO)
			t.Logf("reclaimed after %v, unavailable after a further %v, %s", reclaimed, unavailable,
				ptr(s.checker(session)))

			replacement := startPeer(t, "replacement", c.args()...)
			r := replacement.await("ready", 10*time.Second)
			require.NotEqual(t, session, int32(r.int("sessionId")))
			replacement.await("connected", 20*time.Second)
			s.awaitReceived(int32(r.int("sessionId")), 200, 20*time.Second)
			require.NoError(t, ptr(s.checker(session)).Err())
			replacement.kill()
			awaitReclaimed(t, a, r.int("clientId"), alias(c.pubChan))

			s.close()
			require.NoError(t, a.Close())
			leaks.requireNoLeaks(t)
		})
	}
}

// TestKillMixedSpyClient SIGKILLs a Go process that both publishes and spies on its own UDP publication. The driver
// must reclaim the publication, the spy subscription and the client heartbeat; the surviving remote subscriber must
// see every committed message and an unavailable image; a replacement mixed client must resume.
func TestKillMixedSpyClient(t *testing.T) {
	requireAdv(t)
	leaks := baseline()
	const channel = "aeron:udp?endpoint=localhost:27109|alias=advk-mixed"
	args := []string{"-mode", "mixed", "-spy", "-channel", channel, "-stream", "9110", "-kind", "concurrent",
		"-interval", "200us"}
	a := connect(t, newContext())
	s := newLiveSub(t, a, "aeron:udp?endpoint=localhost:27109", 9110)

	p := startPeer(t, "mixed", args...)
	ready := p.await("ready", 10*time.Second)
	session, clientID := int32(ready.int("sessionId")), ready.int("clientId")
	p.await("connected", 20*time.Second)
	s.awaitReceived(session, 200, 20*time.Second)
	labels := clientCounters(a, clientID)
	require.True(t, containsAny(labels, "sub-pos"), "spy subscription position missing: %v", labels)
	require.True(t, containsAny(labels, "pub-lmt"), "publisher limit missing: %v", labels)

	p.kill()
	sent := p.last("sent").int("seq")
	awaitReclaimed(t, a, clientID, "advk-mixed")
	s.awaitReceived(session, sent, 5*time.Second)
	s.awaitUnavailable(session, reclaimTO)

	r := startPeer(t, "replacement", args...)
	rr := r.await("ready", 10*time.Second)
	r.await("connected", 20*time.Second)
	s.awaitReceived(int32(rr.int("sessionId")), 200, 20*time.Second)
	r.kill()
	awaitReclaimed(t, a, rr.int("clientId"), "advk-mixed")

	s.close()
	require.NoError(t, a.Close())
	leaks.requireNoLeaks(t)
}

func alias(channel string) string {
	_, rest, ok := strings.Cut(channel, "alias=")
	if !ok {
		return ""
	}
	tag, _, _ := strings.Cut(rest, "|")
	return tag
}

func containsAny(labels []string, substr string) bool {
	for _, l := range labels {
		if strings.Contains(l, substr) {
			return true
		}
	}
	return false
}

func ptr[T any](v T) *T { return &v }
