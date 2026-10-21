package archive

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newDisconnectedControl returns a Control whose only image is closed, so the subscription is not connected.
func newDisconnectedControl(t *testing.T) *Control {
	control, _ := newTestControl(t)
	image := aeron.NewMockImage(t)
	image.On("IsClosed").Return(true).Maybe()
	image.On("ControlledPoll", mock.Anything, mock.Anything).Return(0).Maybe()
	control.Subscription = newTestSub(image)
	control.archive.SessionID = testSession
	control.archive.Options.Timeout = 5 * time.Second
	return control
}

func TestControlReview_DisconnectIsDetectedBeforeTimeout(t *testing.T) {
	t.Run("PollForResponse", func(t *testing.T) {
		control := newDisconnectedControl(t)
		start := time.Now()
		_, err := control.PollForResponse(1, testSession)
		assert.Less(t, time.Since(start), time.Second)
		var ex *ArchiveException
		require.True(t, errors.As(err, &ex), "%v", err)
		assert.Contains(t, ex.Message, "response channel from archive is not connected")
		assert.Equal(t, ArchiveStateDisconnected, control.archive.State())
	})
	t.Run("PollForDescriptors", func(t *testing.T) {
		control := newDisconnectedControl(t)
		start := time.Now()
		err := control.PollForDescriptors(1, testSession, 10)
		assert.Less(t, time.Since(start), time.Second)
		var ex *ArchiveException
		require.True(t, errors.As(err, &ex), "%v", err)
		assert.Equal(t, ArchiveStateDisconnected, control.archive.State())
	})
	t.Run("ListRecordings descriptor poller", func(t *testing.T) {
		control := newDisconnectedControl(t)
		control.archive.Control = control
		p := &descriptorPoller{archive: control.archive, correlationID: 1, remaining: 10,
			recordingConsumer: func(*codecs.RecordingDescriptor) {}, unknownTerminalCode: codecs.ControlResponseCode.RECORDING_UNKNOWN}
		start := time.Now()
		_, err := p.pollAll()
		assert.Less(t, time.Since(start), time.Second)
		var ex *ArchiveException
		require.True(t, errors.As(err, &ex), "%v", err)
		assert.Equal(t, ArchiveStateDisconnected, control.archive.State())
	})
	t.Run("PollForErrorResponse", func(t *testing.T) {
		control := newDisconnectedControl(t)
		_, err := control.PollForErrorResponse()
		assert.ErrorIs(t, err, ErrNotConnected)
		assert.Equal(t, ArchiveStateDisconnected, control.archive.State())
	})
}

func TestControlReview_TimeoutsAreTimeoutErrors(t *testing.T) {
	control := newFrameControl(t, nil, &ArchiveListeners{})
	err := control.PollForDescriptors(7, testSession, 1)
	var te *aeron.TimeoutError
	require.True(t, errors.As(err, &te), "%v", err)
	assert.Contains(t, err.Error(), "awaiting recording descriptors - correlationId=7")

	control = newFrameControl(t, nil, &ArchiveListeners{})
	control.archive.Control = control
	p := &descriptorPoller{archive: control.archive, correlationID: 8, remaining: 1,
		subscriptionConsumer: func(*codecs.RecordingSubscriptionDescriptor) {}, unknownTerminalCode: codecs.ControlResponseCode.SUBSCRIPTION_UNKNOWN}
	_, err = p.pollAll()
	require.True(t, errors.As(err, &te), "%v", err)
	assert.Contains(t, err.Error(), "awaiting subscription descriptors - correlationId=8")
}

func TestControlReview_ChallengeForRequestFailsFast(t *testing.T) {
	frames := whole(t,
		&codecs.Challenge{ControlSessionId: otherSession, CorrelationId: 1, EncodedChallenge: []byte("x")},
		&codecs.Challenge{ControlSessionId: testSession, CorrelationId: 2, EncodedChallenge: []byte("x")},
		&codecs.Challenge{ControlSessionId: testSession, CorrelationId: 1, EncodedChallenge: []byte("x")})
	control := newFrameControl(t, frames, &ArchiveListeners{})
	control.archive.Options.Timeout = 5 * time.Second
	start := time.Now()
	_, err := control.PollForResponse(1, testSession)
	assert.Less(t, time.Since(start), time.Second)
	var ex *ArchiveException
	require.True(t, errors.As(err, &ex), "%v", err)
	assert.Contains(t, ex.Message, "unexpected response code")
}

func TestControlReview_ListDescriptorsErrorIsArchiveException(t *testing.T) {
	var listenerErrs []error
	frames := whole(t,
		response(testSession, 2, int64(ErrorCodeActiveRecording), codecs.ControlResponseCode.ERROR, "other"),
		response(testSession, 1, int64(ErrorCodeActiveListing), codecs.ControlResponseCode.ERROR, "busy"))
	control := newFrameControl(t, frames, &ArchiveListeners{ErrorListener: func(err error) { listenerErrs = append(listenerErrs, err) }})
	control.archive.Control = control
	p := &descriptorPoller{archive: control.archive, correlationID: 1, remaining: 10,
		recordingConsumer: func(*codecs.RecordingDescriptor) {}, unknownTerminalCode: codecs.ControlResponseCode.RECORDING_UNKNOWN}
	_, err := p.pollAll()
	code, ok := ArchiveErrorCodeOf(err)
	require.True(t, ok, "%v", err)
	assert.Equal(t, ErrorCodeActiveListing, code)
	require.Len(t, listenerErrs, 1)
	code, ok = ArchiveErrorCodeOf(listenerErrs[0])
	require.True(t, ok)
	assert.Equal(t, ErrorCodeActiveRecording, code)
}

func TestControlReview_ListDescriptorsRejectsWrongSchema(t *testing.T) {
	var listenerErrs []error
	frames := append([]testFrame{wrongSchema(t)}, whole(t, descriptor(testSession, 1, 5))...)
	control := newFrameControl(t, frames, &ArchiveListeners{ErrorListener: func(err error) { listenerErrs = append(listenerErrs, err) }})
	control.archive.Control = control
	var got []int64
	p := &descriptorPoller{archive: control.archive, correlationID: 1, remaining: 1,
		recordingConsumer:   func(d *codecs.RecordingDescriptor) { got = append(got, d.RecordingId) },
		unknownTerminalCode: codecs.ControlResponseCode.RECORDING_UNKNOWN}
	n, err := p.pollAll()
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
	assert.Equal(t, []int64{5}, got)
	require.Len(t, listenerErrs, 1)
	assert.Contains(t, listenerErrs[0].Error(), "expected schemaId")
}

func TestControlReview_RecordingEventsNotEnabled(t *testing.T) {
	a := &Archive{Events: &RecordingEventsAdapter{}, Listeners: &ArchiveListeners{}, Options: DefaultOptions(), mtx: &sync.Mutex{}}
	a.Events.archive = a
	assert.NotPanics(t, func() { assert.Zero(t, a.RecordingEventsPoll()) })
	_, err := a.IsRecordingEventsConnected()
	assert.Error(t, err)
	assert.NotPanics(t, a.DisableRecordingEvents)
	n, err := NewRecordingEventsAdapter(nil, nil, 0).Poll()
	assert.Zero(t, n)
	assert.NoError(t, err)
}
