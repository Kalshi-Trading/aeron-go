package mixedmatrix

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/lirm/aeron-go/e2e"
	"github.com/lirm/aeron-go/systests/adversarial/mixed-matrix/internal/mm"
	"github.com/stretchr/testify/require"
)

// TestArchiveRecordReplayUnderChurn records a UDP channel while Go (exclusive, OfferBlock, TryClaim) and Java
// (Publication, ExclusivePublication) publishers come and go, are closed or SIGKILLed, and then replays every
// recording into a Go consumer. Each recording must replay contiguously from its start position with valid CRCs and
// in-order, exactly-once writers; a gracefully closed writer must be complete in the recording of its session, as
// with Java.
func TestArchiveRecordReplayUnderChurn(t *testing.T) {
	requireAdv(t)
	rnd := seed(t)
	base := snapshot()
	freshStack(t)
	const stream = int32(9780)
	ch := "aeron:udp?endpoint=localhost:27730|" + termParams
	streamArg := "stream=" + strconv.Itoa(int(stream))
	arch := connectArchive(t)
	_, err := arch.StartRecording(ch, stream, true, false)
	require.NoError(t, err)
	cl := connect(t)
	live, err := mm.AddSub(cl.Aeron, mm.SubPoll, "aeron:udp?endpoint=localhost:27730", stream, rnd, true)
	require.NoError(t, err)
	pl := startPoller(t, live)

	types := []pubType{{"go", mm.PubExclusive}, {"go", mm.PubOfferBlock}, {"go", mm.PubTryClaim},
		{"java", mm.PubConcurrent}, {"java", mm.PubExclusive}}
	type rec struct {
		p      *peer
		writer uint8
	}
	var pubs []rec
	var closed []closedWriter
	writer := uint8(1)
	start := func() {
		typ := types[rnd.Intn(len(types))]
		p := startPeer(t, typ.lang, "pub", "channel="+ch, streamArg, "kind="+typ.kind, "writer="+strconv.Itoa(int(writer)),
			"exclusive="+strconv.FormatBool(typ.kind == mm.PubExclusive), "sizes="+mm.FormatSizes(cellSizes),
			"rateUs="+strconv.Itoa(300+rnd.Intn(500)))
		p.await("ready", 30*time.Second)
		pubs = append(pubs, rec{p, writer})
		writer++
	}
	start()
	start()
	for end := time.Now().Add(15 * time.Second); time.Now().Before(end); {
		time.Sleep(time.Duration(300+rnd.Intn(900)) * time.Millisecond)
		switch rnd.Intn(3) {
		case 0:
			if len(pubs) < 3 {
				start()
			}
		case 1:
			if len(pubs) > 0 {
				i := rnd.Intn(len(pubs))
				ev := pubs[i].p.close("closed")
				closed = append(closed, closedWriter{writer: pubs[i].writer, next: uint32(ev.Int("next")),
					sessionID: int32(ev.Int("sessionId")), firstPosition: ev.Int("firstPosition")})
				pubs = append(pubs[:i], pubs[i+1:]...)
			}
		case 2:
			if len(pubs) > 0 {
				i := rnd.Intn(len(pubs))
				pubs[i].p.Kill()
				pubs = append(pubs[:i], pubs[i+1:]...)
				start()
			}
		}
	}
	for _, r := range pubs {
		ev := r.p.close("closed")
		closed = append(closed, closedWriter{writer: r.writer, next: uint32(ev.Int("next")),
			sessionID: int32(ev.Int("sessionId")), firstPosition: ev.Int("firstPosition")})
	}
	// let the archive catch up with the live stream, then stop
	for _, w := range closed {
		awaitTrue(t, fmt.Sprintf("live writer %d", w.writer), 20*time.Second, func() bool {
			_, next := writerCount(live.Tracker, w.writer)
			return next == w.next || w.next == 0
		})
	}
	time.Sleep(time.Second)
	require.NoError(t, arch.StopRecording(ch, stream))
	pl.Stop()
	requireCleanTracker(t, "live", live.Tracker)

	recordings, err := arch.ListRecordingsForUri(0, 100, "localhost:27730", stream)
	require.NoError(t, err)
	require.NotEmpty(t, recordings)
	replayed := map[int32]*mm.Tracker{}
	for i, r := range recordings {
		stop, err := e2e.WaitRecordingStopped(arch, r.RecordingId, 20*time.Second)
		require.NoError(t, err)
		if stop == r.StartPosition {
			continue
		}
		replayCh := "aeron:udp?endpoint=localhost:27732"
		replayStream := int32(9781 + i%10)
		sessionID, err := arch.StartReplay(r.RecordingId, r.StartPosition, stop-r.StartPosition, replayCh, replayStream)
		require.NoError(t, err)
		subCh, err := e2e.WithSessionID(replayCh, int32(sessionID))
		require.NoError(t, err)
		sub, err := mm.AddSub(cl.Aeron, []string{mm.SubPoll, mm.SubControlled}[i%2], subCh, replayStream,
			rand.New(rand.NewSource(rnd.Int63())), true)
		require.NoError(t, err)
		for deadline := time.Now().Add(30 * time.Second); ; {
			sub.Poll()
			done := false
			for _, img := range sub.Tracker.Images() {
				done = done || img.LastEnd >= stop
			}
			if done {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("replay of recording %+v (stop %d) incomplete: %+v", r, stop, sub.Tracker.Images())
			}
			time.Sleep(100 * time.Microsecond)
		}
		for _, img := range sub.Tracker.Images() {
			require.Equal(t, r.StartPosition, img.JoinPosition, "replay join position of recording %d", r.RecordingId)
		}
		requireCleanTracker(t, fmt.Sprintf("replay of recording %d", r.RecordingId), sub.Tracker)
		replayed[r.SessionId] = sub.Tracker
		require.NoError(t, sub.Close())
	}
	for _, w := range closed {
		if w.next == 0 {
			continue
		}
		tr := replayed[w.sessionID]
		require.NotNil(t, tr, "no recording of session %d (writer %d)", w.sessionID, w.writer)
		n, next := writerCount(tr, w.writer)
		require.Equal(t, int64(w.next), n, "writer %d messages in the recording", w.writer)
		require.Equal(t, w.next, next)
	}
	require.Empty(t, cl.Errors())
	require.NoError(t, live.Close())
	require.NoError(t, cl.Close())
	require.NoError(t, arch.Close())
	requireNoLeaks(t, base)
}
