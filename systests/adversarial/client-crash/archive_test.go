package clientcrash

import (
	"testing"
	"time"

	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/lirm/aeron-go/e2e"
	"github.com/stretchr/testify/require"
)

const (
	archChannel   = "aeron:udp?endpoint=localhost:27160|alias=advk-arch"
	archStream    = 9160
	replayChannel = "aeron:udp?endpoint=localhost:27161"
	replayStream  = 9161
)

func recordingFor(t *testing.T, list func() ([]*codecs.RecordingDescriptor, error), session int32) *codecs.RecordingDescriptor {
	t.Helper()
	recs, err := list()
	require.NoError(t, err)
	for _, r := range recs {
		if r.SessionId == session {
			return r
		}
	}
	return nil
}

// TestKillArchiveClient SIGKILLs a Go archive client that started a recording and is publishing into it. As in Java,
// the archive keeps the recording subscription (it belongs to the archive, not the dead control session), the
// driver reclaims the client's publication and control-session resources, the recording stops at a stable
// position, the recorded stream replays complete and in order up to that position, and a replacement publisher on
// the same channel is recorded as a new recording.
func TestKillArchiveClient(t *testing.T) {
	requireAdv(t)
	leaks := baseline()
	arch := connectArchive(t)
	a := arch.Aeron()
	list := func() ([]*codecs.RecordingDescriptor, error) {
		return arch.ListRecordingsForUri(0, 100, "advk-arch", archStream)
	}

	p := startPeer(t, "archive-client", "-mode", "archive", "-control", controlChannel, "-channel", archChannel,
		"-stream", "9160", "-interval", "200us")
	ready := p.await("ready", 30*time.Second)
	session := int32(ready.int("sessionId"))
	clientID := ready.int("clientId")
	subID := ready.int("recordingSubscriptionId")
	p.await("connected", 20*time.Second)

	var rec *codecs.RecordingDescriptor
	awaitTrue(t, "recording to advance", 20*time.Second, func() bool {
		if rec = recordingFor(t, list, session); rec == nil {
			return false
		}
		pos, err := arch.GetRecordingPosition(rec.RecordingId)
		return err == nil && pos > rec.StartPosition+64*1024
	})
	p.kill()
	awaitReclaimed(t, a, clientID, "")
	awaitTrue(t, "recording to stop", reclaimTO, func() bool {
		r := recordingFor(t, list, session)
		return r != nil && r.StopPosition >= 0
	})
	rec = recordingFor(t, list, session)
	subs, err := arch.ListRecordingSubscriptions(0, 10, true, archStream, "advk-arch")
	require.NoError(t, err)
	require.Len(t, subs, 1, "the archive's recording subscription outlives the client that requested it")

	sub, err := a.AddSubscription(replayChannel, replayStream)
	require.NoError(t, err)
	replaySession, err := arch.StartReplay(rec.RecordingId, rec.StartPosition, rec.StopPosition-rec.StartPosition,
		replayChannel, replayStream)
	require.NoError(t, err)
	checker := e2e.NewSequenceChecker(0)
	awaitTrue(t, "replay to reach the stop position", 30*time.Second, func() bool {
		sub.Poll(checker.Handler(), 100)
		img := sub.ImageBySessionID(int32(replaySession))
		return img != nil && img.Position() >= rec.StopPosition
	})
	require.NoError(t, checker.Err(), checker.String())
	require.Positive(t, checker.Received)
	require.NoError(t, sub.Close())

	replacement := startPeer(t, "replacement", "-mode", "pub", "-kind", "exclusive", "-channel", archChannel,
		"-stream", "9160", "-interval", "200us")
	s2 := int32(replacement.await("ready", 30*time.Second).int("sessionId"))
	replacement.await("connected", 20*time.Second)
	awaitTrue(t, "replacement to be recorded", 20*time.Second, func() bool {
		r := recordingFor(t, list, s2)
		if r == nil {
			return false
		}
		pos, err := arch.GetRecordingPosition(r.RecordingId)
		return err == nil && pos > r.StartPosition
	})
	replacement.kill()
	require.NoError(t, arch.StopRecordingBySubscriptionId(subID))
	require.NoError(t, arch.Close())
	leaks.requireNoLeaks(t)
}
