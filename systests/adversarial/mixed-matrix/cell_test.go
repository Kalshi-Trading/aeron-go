package mixedmatrix

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/systests/adversarial/mixed-matrix/internal/mm"
	"github.com/stretchr/testify/require"
)

// pubType is a publisher implementation: lang "go" (mmpeer child) or "java" (MatrixPeer container).
type pubType struct{ lang, kind string }

func (p pubType) String() string { return p.lang + "-" + p.kind }

var pubTypes = []pubType{
	{"go", mm.PubConcurrent}, {"go", mm.PubExclusive}, {"go", mm.PubTryClaim}, {"go", mm.PubOfferBlock},
	{"java", mm.PubConcurrent}, {"java", mm.PubExclusive},
}

func (p pubType) concurrent() bool { return p.kind == mm.PubConcurrent || p.kind == mm.PubTryClaim }

// Consumer types: in-process Go Poll / ControlledPoll and the Java MatrixPeer subscriber.
const (
	consGoPoll       = "go-poll"
	consGoControlled = "go-controlled"
	consJava         = "java-sub"
)

var consumerTypes = []string{consGoPoll, consGoControlled, consJava}

// subEndpoint is a subscription channel plus destinations (multi-destination subscription).
type subEndpoint struct {
	channel string
	dests   []string
}

// transport builds the publication and subscription channels of a cell from two ports.
type transport struct {
	name string
	// anchorOnly: the first endpoint binds its own sockets (multi-destination subscription), so every other
	// consumer uses the remaining endpoints
	anchorOnly bool
	// pub channel and destinations (manual MDC); subs are the subscription endpoints, anchor first.
	build func(port0, port1 int) (pub string, pubDests []string, subs []subEndpoint)
}

const termParams = "term-length=1m"

var transports = []transport{
	{"ipc", false, func(int, int) (string, []string, []subEndpoint) {
		return "aeron:ipc?" + termParams, nil, []subEndpoint{{channel: "aeron:ipc"}}
	}},
	{"udp", false, func(p, _ int) (string, []string, []subEndpoint) {
		ch := fmt.Sprintf("aeron:udp?endpoint=localhost:%d|%s", p, termParams)
		return ch, nil, []subEndpoint{{channel: fmt.Sprintf("aeron:udp?endpoint=localhost:%d", p)}}
	}},
	{"multicast", false, func(p, _ int) (string, []string, []subEndpoint) {
		ch := fmt.Sprintf("aeron:udp?endpoint=224.20.30.97:%d|interface=localhost", p)
		return ch + "|" + termParams, nil, []subEndpoint{{channel: ch}}
	}},
	{"mdc-manual", false, func(p0, p1 int) (string, []string, []subEndpoint) {
		d0, d1 := fmt.Sprintf("aeron:udp?endpoint=localhost:%d", p0), fmt.Sprintf("aeron:udp?endpoint=localhost:%d", p1)
		return "aeron:udp?control-mode=manual|" + termParams, []string{d0, d1},
			[]subEndpoint{{channel: d0}, {channel: d1}}
	}},
	{"mdc-dynamic", false, func(p, _ int) (string, []string, []subEndpoint) {
		sub := fmt.Sprintf("aeron:udp?control=localhost:%d|endpoint=localhost:0|control-mode=dynamic", p)
		return fmt.Sprintf("aeron:udp?control=localhost:%d|control-mode=dynamic|%s", p, termParams), nil,
			[]subEndpoint{{channel: sub}}
	}},
	{"mds", true, func(p0, p1 int) (string, []string, []subEndpoint) {
		// a manual MDC publisher sends to the anchor's multi-destination subscription (one live destination plus
		// an idle one) and to a plain unicast endpoint for the other consumers; MDS destinations bind their own
		// sockets, exactly as in Java, so they cannot share an endpoint with another subscription
		d0, d1 := fmt.Sprintf("aeron:udp?endpoint=localhost:%d", p0), fmt.Sprintf("aeron:udp?endpoint=localhost:%d", p1)
		return "aeron:udp?control-mode=manual|" + termParams, []string{d0, d1}, []subEndpoint{{
			channel: "aeron:udp?control-mode=manual",
			dests:   []string{d0, fmt.Sprintf("aeron:udp?endpoint=localhost:%d", p0+50)},
		}, {channel: d1}}
	}},
	{"spy", false, func(p, _ int) (string, []string, []subEndpoint) {
		ch := fmt.Sprintf("aeron:udp?endpoint=localhost:%d|%s", p, termParams)
		// spies never cause a unicast publication to connect; a normal subscriber is kept on the endpoint so
		// that the publisher connects and the spies (anchor first) see the same session
		return ch, nil, []subEndpoint{{channel: "aeron-spy:" + ch}, {channel: fmt.Sprintf("aeron:udp?endpoint=localhost:%d", p)}}
	}},
}

// sizes: 0, 1, a full MTU payload (1408 - 32), fragmented, and the maximum message length of a 1m term (128k).
var cellSizes = []int{64, 0, 1, 1376, 64, 5000, 1377, 64, 128 * 1024, 2}

// cell is one matrix cell run under churn.
type cell struct {
	pub       pubType
	consumer  string
	transport transport
	stream    int32
	port0     int
	port1     int
	duration  time.Duration
	// mixed: 30% of started publishers are a random other type on the same channel
	mixed bool
}

type pubRec struct {
	p         *peer
	typ       pubType
	writer    uint8
	sessionID int32
}

type closedWriter struct {
	writer        uint8
	next          uint32
	sessionID     int32
	firstPosition int64
}

// runCell runs one cell under seeded random churn and verifies every consumer. The stack must be up.
func runCell(t *testing.T, rnd *rand.Rand, c cell) {
	pubCh, pubDests, endpoints := c.transport.build(c.port0, c.port1)
	streamArg := "stream=" + strconv.Itoa(int(c.stream))
	cl := connect(t)

	var subsMu sync.Mutex
	var inproc []*mm.Sub
	var retired []*mm.Sub
	stopPoll := make(chan struct{})
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			select {
			case <-stopPoll:
				return
			default:
			}
			subsMu.Lock()
			work := 0
			for _, s := range inproc {
				work += s.Poll()
			}
			subsMu.Unlock()
			if work == 0 {
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()
	defer func() {
		select {
		case <-stopPoll:
		default:
			close(stopPoll)
		}
		<-pollDone
	}()

	// concurrent publishers share one session: a SIGKILLed one can leave a claimed-but-uncommitted frame that the
	// driver later unblocks as padding (Java does the same), so positions may jump mid-term
	allowGaps := true
	addInproc := func(kind string, ep subEndpoint) *mm.Sub {
		k := mm.SubPoll
		if kind == consGoControlled {
			k = mm.SubControlled
		}
		s, err := mm.AddSub(cl.Aeron, k, ep.channel, c.stream, rand.New(rand.NewSource(rnd.Int63())), allowGaps,
			ep.dests...)
		require.NoError(t, err)
		subsMu.Lock()
		inproc = append(inproc, s)
		subsMu.Unlock()
		return s
	}
	removeInproc := func(s *mm.Sub) {
		subsMu.Lock()
		for i, x := range inproc {
			if x == s {
				inproc = append(inproc[:i], inproc[i+1:]...)
			}
		}
		retired = append(retired, s)
		subsMu.Unlock()
		require.NoError(t, s.Close())
	}
	startJavaSub := func(ep subEndpoint) *peer {
		args := []string{"channel=" + ep.channel, streamArg}
		for _, d := range ep.dests {
			args = append(args, "dest="+d)
		}
		p := startPeer(t, "java", "sub", args...)
		p.await("ready", 30*time.Second)
		return p
	}

	shared := endpoints
	if c.transport.anchorOnly {
		shared = endpoints[1:]
	}
	pickShared := func() subEndpoint { return shared[rnd.Intn(len(shared))] }
	// anchor consumer lives for the whole cell; extra endpoints (other MDC destination, spy's subscriber) too
	anchorKind := c.consumer
	if anchorKind == consJava {
		anchorKind = consGoPoll
	}
	anchor := addInproc(anchorKind, endpoints[0])
	var javaSubs []*peer
	if c.consumer == consJava {
		javaSubs = append(javaSubs, startJavaSub(pickShared()))
	}
	for _, ep := range endpoints[1:] {
		addInproc(consGoPoll, ep)
	}

	var pubs []*pubRec
	var closed []closedWriter
	nextWriter := 1
	startPub := func(typ pubType) {
		if nextWriter > 250 {
			return
		}
		w := uint8(nextWriter)
		nextWriter++
		args := []string{"channel=" + pubCh, streamArg, "kind=" + typ.kind, "writer=" + strconv.Itoa(int(w)),
			"sizes=" + mm.FormatSizes(cellSizes), "rateUs=" + strconv.Itoa(300+rnd.Intn(700)),
			"exclusive=" + strconv.FormatBool(typ.kind == mm.PubExclusive)}
		for _, d := range pubDests {
			args = append(args, "dest="+d)
		}
		p := startPeer(t, typ.lang, "pub", args...)
		ev := p.await("ready", 30*time.Second)
		pubs = append(pubs, &pubRec{p: p, typ: typ, writer: w, sessionID: int32(ev.Int("sessionId"))})
	}
	closePub := func(i int) {
		r := pubs[i]
		pubs = append(pubs[:i], pubs[i+1:]...)
		ev := r.p.close("closed")
		closed = append(closed, closedWriter{writer: r.writer, next: uint32(ev.Int("next")),
			sessionID: int32(ev.Int("sessionId")), firstPosition: ev.Int("firstPosition")})
	}
	pickType := func() pubType {
		if c.mixed && rnd.Intn(10) < 3 {
			return pubTypes[rnd.Intn(len(pubTypes))]
		}
		return c.pub
	}

	startPub(c.pub)
	var lateSubs []*mm.Sub
	var lateJava []*peer
	var lateGo []*peer
	deadline := time.Now().Add(c.duration)
	var log []string
	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(200+rnd.Intn(800)) * time.Millisecond)
		switch action := rnd.Intn(10); {
		case action == 0 && len(pubs) < 3:
			log = append(log, "start-pub")
			startPub(pickType())
		case action == 1 && len(pubs) > 0:
			log = append(log, "close-pub")
			closePub(rnd.Intn(len(pubs)))
			if len(pubs) == 0 {
				startPub(pickType())
			}
		case action == 2 && len(pubs) > 0:
			i := rnd.Intn(len(pubs))
			log = append(log, "kill-pub "+pubs[i].typ.String())
			pubs[i].p.Kill()
			pubs = append(pubs[:i], pubs[i+1:]...)
			startPub(pickType())
		case action == 3 && len(pubs) > 0:
			i := rnd.Intn(len(pubs))
			log = append(log, "stop-pub")
			pubs[i].p.signal("STOP")
			time.Sleep(time.Duration(100+rnd.Intn(700)) * time.Millisecond)
			pubs[i].p.signal("CONT")
		case action == 4 && len(lateSubs) < 2:
			log = append(log, "late-go-sub")
			kind := consGoPoll
			if rnd.Intn(2) == 0 {
				kind = consGoControlled
			}
			lateSubs = append(lateSubs, addInproc(kind, pickShared()))
		case action == 5 && len(lateSubs) > 0:
			log = append(log, "remove-go-sub")
			i := rnd.Intn(len(lateSubs))
			removeInproc(lateSubs[i])
			lateSubs = append(lateSubs[:i], lateSubs[i+1:]...)
		case action == 6 && len(lateJava)+len(lateGo) < 2:
			if rnd.Intn(2) == 0 {
				log = append(log, "late-java-sub")
				lateJava = append(lateJava, startJavaSub(pickShared()))
			} else {
				log = append(log, "late-go-child-sub")
				ep := pickShared()
				args := []string{"channel=" + ep.channel, streamArg, "kind=" + mm.SubControlled}
				for _, d := range ep.dests {
					args = append(args, "dest="+d)
				}
				p := startPeer(t, "go", "sub", args...)
				p.await("ready", 20*time.Second)
				lateGo = append(lateGo, p)
			}
		case action == 7 && len(lateJava)+len(lateGo) > 0:
			// kill -9 a child subscriber: the publisher must carry on (its image goes away from the driver)
			log = append(log, "kill-sub")
			if len(lateJava) > 0 && (len(lateGo) == 0 || rnd.Intn(2) == 0) {
				lateJava[0].Kill()
				lateJava = lateJava[1:]
			} else {
				lateGo[0].Kill()
				lateGo = lateGo[1:]
			}
		case action == 8:
			// a short-lived in-process client reconnects and consumes from the live stream
			log = append(log, "reconnect-client")
			c2 := connect(t)
			ep := pickShared()
			s, err := mm.AddSub(c2.Aeron, mm.SubPoll, ep.channel, c.stream, rnd, allowGaps, ep.dests...)
			require.NoError(t, err)
			end := time.Now().Add(time.Duration(200+rnd.Intn(500)) * time.Millisecond)
			for time.Now().Before(end) {
				if s.Poll() == 0 {
					time.Sleep(100 * time.Microsecond)
				}
			}
			requireCleanTracker(t, "reconnect client", s.Tracker)
			require.NoError(t, c2.Close())
			require.Empty(t, c2.Errors())
		}
	}
	t.Logf("churn schedule: %s", strings.Join(log, ", "))

	for len(pubs) > 0 {
		closePub(0)
	}
	// the anchor joined before every publisher, so it must see every gracefully closed writer from seq 0 to
	// next-1 exactly once, unless its image joined after the writer's first message (concurrent publisher
	// joining a live shared session is always fine; a new session is joined at its initial position)
	for _, w := range closed {
		awaitTrue(t, fmt.Sprintf("anchor to receive writer %d up to %d", w.writer, w.next), 20*time.Second, func() bool {
			for _, img := range anchor.Tracker.Images() {
				if ws := img.Writers[w.writer]; img.SessionID == w.sessionID && ws != nil && ws.Next == w.next {
					return true
				}
			}
			return w.next == 0
		})
		for _, img := range anchor.Tracker.Images() {
			ws := img.Writers[w.writer]
			if img.SessionID != w.sessionID || ws == nil || img.JoinPosition > w.firstPosition {
				continue
			}
			require.Equal(t, uint32(0), ws.First, "writer %d first seq on anchor image %d", w.writer, img.CorrelationID)
			require.Equal(t, int64(w.next), ws.Count, "writer %d exactly once on anchor image %d", w.writer,
				img.CorrelationID)
		}
	}
	for _, p := range append(lateJava, javaSubs...) {
		requireCleanReports(t, p)
	}
	for _, p := range lateGo {
		requireCleanReports(t, p)
	}
	close(stopPoll)
	<-pollDone
	subsMu.Lock()
	all := append(append([]*mm.Sub(nil), inproc...), retired...)
	subsMu.Unlock()
	for _, s := range all {
		requireCleanTracker(t, "in-process "+s.Kind, s.Tracker)
	}
	for _, img := range anchor.Tracker.Images() {
		require.True(t, img.Available, "available handler for image %d", img.CorrelationID)
	}
	require.Greater(t, anchor.Tracker.Messages(), int64(0), "anchor received nothing")
	for _, s := range inproc {
		require.NoError(t, s.Close())
	}
	// every image of a closed publisher must go unavailable on the in-process consumers
	awaitTrue(t, "unavailable handlers after all publishers closed", 20*time.Second, func() bool {
		for _, s := range all {
			for _, img := range s.Tracker.Images() {
				if !img.Unavailable {
					return false
				}
			}
		}
		return true
	})
	require.Empty(t, cl.Errors(), "client ErrorHandler")
	require.NoError(t, cl.Close())
}

func requireCleanTracker(t *testing.T, what string, tr *mm.Tracker) {
	t.Helper()
	for _, img := range tr.Images() {
		require.Empty(t, img.Errors, "%s image %d (session %d, join %d)", what, img.CorrelationID, img.SessionID,
			img.JoinPosition)
	}
}

func requireCleanReports(t *testing.T, p *peer) {
	t.Helper()
	p.close("done")
	for _, r := range p.eventsNamed("report") {
		require.Equal(t, int64(0), r.Int("errors"), "%s image %d: %s", p.name, r.Int("correlationId"),
			r.Str("firstError"))
	}
}
