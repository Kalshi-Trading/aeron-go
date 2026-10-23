package archive

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"

	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/require"
)

// TestE2EErrorText checks the Java archive's error text and control response reach the caller unchanged, for the
// errors applications tell apart by message (substring match on the lower-cased Error()) or by RelevantId.
// Passes against the 1.53.3 docker archive and against a 1.43.0 ArchivingMediaDriver.
func TestE2EErrorText(t *testing.T) {
	requireE2E(t)
	a := e2eArchive1(t)
	channel := fmt.Sprintf("aeron:ipc?term-length=%d", e2eTermLength)

	expect := func(t *testing.T, err error, text string, code ArchiveErrorCode) {
		t.Helper()
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), text)
		var ae *ArchiveException
		require.True(t, errors.As(err, &ae), "%T: %v", err, err)
		require.Equal(t, code, ae.ErrorCode, "%v", err)
		resp := a.Control.Results.ControlResponse
		require.NotNil(t, resp)
		require.Equal(t, codecs.ControlResponseCode.ERROR, resp.Code)
		require.Equal(t, int64(code), resp.RelevantId)
	}

	t.Run("recording exists", func(t *testing.T) {
		stream := e2eStream()
		subID, err := a.StartRecording(channel, stream, true, false)
		require.NoError(t, err)
		defer func() { require.NoError(t, a.StopRecordingBySubscriptionId(subID)) }()
		_, err = a.StartRecording(channel, stream, true, false)
		expect(t, err, "recording exists", ErrorCodeActiveSubscription)
	})

	t.Run("no recording found", func(t *testing.T) {
		err := a.StopRecording(channel, e2eStream())
		expect(t, err, "no recording found", ErrorCodeUnknownSubscription)
	})

	t.Run("unknown recording", func(t *testing.T) {
		err := a.PurgeRecording(math.MaxInt32)
		expect(t, err, "unknown recording", ErrorCodeUnknownRecording)
	})

	t.Run("not aligned to a data header", func(t *testing.T) {
		stream := e2eStream()
		subID, err := a.StartRecording(channel, stream, true, false)
		require.NoError(t, err)
		defer func() { require.NoError(t, a.StopRecordingBySubscriptionId(subID)) }()
		pub, err := a.AddExclusivePublication(channel, stream)
		require.NoError(t, err)
		defer pub.Close()
		waitFor(t, "publication connected", pub.IsConnected)
		buf := atomic.MakeBuffer(make([]byte, e2ePayload))
		for i := 0; i < 4; i++ {
			waitFor(t, "offer", func() bool { return pub.Offer(buf, 0, e2ePayload, nil) > 0 })
		}
		recordingID := int64(RecordingIdNullValue)
		waitFor(t, "recording to catch up", func() bool {
			id, err := a.FindLastMatchingRecording(0, pub.SessionID(), stream, "aeron:ipc")
			require.NoError(t, err)
			if recordingID = id; id == RecordingIdNullValue {
				return false
			}
			p, err := a.GetRecordingPosition(id)
			require.NoError(t, err)
			return p == pub.Position()
		})
		// Frame aligned but in the middle of a frame, so there is no data header at the position.
		_, err = a.StartReplay(recordingID, e2eCatalogFrameLength/2, math.MaxInt64, "aeron:ipc", e2eStream())
		// Up to 1.43 the archive reports a GENERIC "not aligned to a data header"; 1.53 reports INVALID_POSITION.
		if strings.Contains(strings.ToLower(fmt.Sprint(err)), "not aligned to a data header") {
			expect(t, err, "not aligned to a data header", ErrorCodeGeneric)
		} else {
			expect(t, err, "does not point to a valid frame", ErrorCodeInvalidPosition)
		}
	})
}
