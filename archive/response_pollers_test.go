package archive

import (
	"errors"
	"fmt"
	"testing"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	frameBegin        = 0x80
	frameEnd          = 0x40
	frameUnfragmented = frameBegin | frameEnd
	testSession       = int64(42)
	otherSession      = int64(43)
)

type testFrame struct {
	buf   *atomic.Buffer
	flags uint8
}

func newTestHeaderFlags(flags uint8) *logbuffer.Header {
	buffer := atomic.MakeBuffer(make([]byte, logbuffer.DataFrameHeader.Length))
	buffer.PutUInt8(logbuffer.DataFrameHeader.FlagsFieldOffset, flags)
	return new(logbuffer.Header).Wrap(buffer.Ptr(), buffer.Capacity())
}

func whole(t *testing.T, msgs ...encodable) []testFrame {
	var frames []testFrame
	for _, m := range msgs {
		frames = append(frames, testFrame{encode(t, m), frameUnfragmented})
	}
	return frames
}

// split encodes msg and returns it as parts fragments.
func split(t *testing.T, msg encodable, parts int) []testFrame {
	b := encode(t, msg)
	raw := b.GetBytesArray(0, b.Capacity())
	size := (len(raw) + parts - 1) / parts
	var frames []testFrame
	for i := 0; i < len(raw); i += size {
		end := i + size
		if end > len(raw) {
			end = len(raw)
		}
		var flags uint8
		if i == 0 {
			flags |= frameBegin
		}
		if end == len(raw) {
			flags |= frameEnd
		}
		chunk := append([]byte(nil), raw[i:end]...)
		frames = append(frames, testFrame{atomic.MakeBuffer(chunk), flags})
	}
	return frames
}

// newFrameSub returns a subscription whose single image replays frames honouring controlled poll actions.
func newFrameSub(t *testing.T, frames []testFrame) *aeron.Subscription {
	image := aeron.NewMockImage(t)
	image.On("IsClosed").Return(false).Maybe()
	controlled := func(handler term.ControlledFragmentHandler, limit int) int {
		count := 0
		for count < limit && len(frames) > 0 {
			f := frames[0]
			action := handler(f.buf, 0, f.buf.Capacity(), newTestHeaderFlags(f.flags))
			if action == term.ControlledPollActionAbort {
				break
			}
			frames = frames[1:]
			count++
			if action == term.ControlledPollActionBreak {
				break
			}
		}
		return count
	}
	image.On("ControlledPoll", mock.Anything, mock.Anything).Return(controlled).Maybe()
	image.On("Poll", mock.Anything, mock.Anything).Return(func(handler term.FragmentHandler, limit int) int {
		return controlled(func(b *atomic.Buffer, o, l int32, h *logbuffer.Header) term.ControlledPollAction {
			handler(b, o, l, h)
			return term.ControlledPollActionContinue
		}, limit)
	}).Maybe()
	return newTestSub(image)
}

func response(session, correlation, relevant int64, code codecs.ControlResponseCodeEnum, msg string) *codecs.ControlResponse {
	return &codecs.ControlResponse{ControlSessionId: session, CorrelationId: correlation, RelevantId: relevant,
		Code: code, ErrorMessage: []byte(msg)}
}

func signal(session, correlation, recordingId int64, s codecs.RecordingSignalEnum) *codecs.RecordingSignalEvent {
	return &codecs.RecordingSignalEvent{ControlSessionId: session, CorrelationId: correlation, RecordingId: recordingId,
		SubscriptionId: 7, Position: 128, Signal: s}
}

func descriptor(session, correlation, recordingId int64) *codecs.RecordingDescriptor {
	return &codecs.RecordingDescriptor{ControlSessionId: session, CorrelationId: correlation, RecordingId: recordingId,
		StreamId: 1001, StrippedChannel: []byte("aeron:ipc"), OriginalChannel: []byte("aeron:ipc?alias=x"),
		SourceIdentity: []byte("aeron:ipc")}
}

// unknownTemplate is a valid archive-schema message the response pollers do not handle.
func unknownTemplate() encodable {
	return &codecs.RecordingStarted{RecordingId: 99, Channel: []byte("x"), SourceIdentity: []byte("y")}
}

func wrongSchema(t *testing.T) testFrame {
	b := encode(t, response(testSession, 1, 0, codecs.ControlResponseCode.OK, ""))
	b.PutUInt16(4, 999)
	return testFrame{b, frameUnfragmented}
}

type signalRecorder struct{ signals []codecs.RecordingSignalEnum }

func (r *signalRecorder) OnSignal(_, _, _, _, _ int64, s codecs.RecordingSignalEnum) {
	r.signals = append(r.signals, s)
}

func TestArchiveException(t *testing.T) {
	assert.Equal(t, "UNKNOWN_RECORDING", ErrorCodeUnknownRecording.String())
	assert.Equal(t, "INVALID_POSITION", ErrorCodeInvalidPosition.String())
	assert.Equal(t, "unknown error code: 99", ArchiveErrorCode(99).String())
	assert.EqualValues(t, 15, ErrorCodeEmptyRecording)

	err := fmt.Errorf("wrapped: %w", newArchiveResponseException("nope", int64(ErrorCodeMaxReplays), 5))
	code, ok := ArchiveErrorCodeOf(err)
	assert.True(t, ok)
	assert.Equal(t, ErrorCodeMaxReplays, code)
	assert.True(t, errors.Is(err, &ArchiveException{ErrorCode: ErrorCodeMaxReplays}))
	assert.False(t, errors.Is(err, &ArchiveException{ErrorCode: ErrorCodeGeneric}))
	var ex *ArchiveException
	require.True(t, errors.As(err, &ex))
	assert.EqualValues(t, 5, ex.CorrelationId)
	_, ok = ArchiveErrorCodeOf(errors.New("x"))
	assert.False(t, ok)
}

func TestControlResponsePoller(t *testing.T) {
	t.Run("one response per poll and skips unknown templates", func(t *testing.T) {
		frames := whole(t, unknownTemplate(),
			response(testSession, 1, 10, codecs.ControlResponseCode.OK, ""),
			response(testSession, 2, 20, codecs.ControlResponseCode.ERROR, "bad"))
		p := NewControlResponsePoller(newFrameSub(t, frames), 0)

		n, err := p.Poll()
		require.NoError(t, err)
		assert.Equal(t, 2, n)
		assert.True(t, p.IsPollComplete())
		assert.True(t, p.IsControlResponse())
		assert.EqualValues(t, 1, p.CorrelationId())
		assert.EqualValues(t, 10, p.RelevantId())
		assert.Equal(t, codecs.ControlResponseCode.OK, p.Code())

		_, err = p.Poll()
		require.NoError(t, err)
		assert.EqualValues(t, 2, p.CorrelationId())
		assert.Equal(t, codecs.ControlResponseCode.ERROR, p.Code())
		assert.Equal(t, "bad", p.ErrorMessage())

		n, _ = p.Poll()
		assert.Zero(t, n)
		assert.False(t, p.IsPollComplete())
		assert.EqualValues(t, aeron.NullValue, p.CorrelationId())
		assert.EqualValues(t, aeron.NullValue, p.TemplateId())
	})

	t.Run("reassembles fragmented responses", func(t *testing.T) {
		long := string(make([]byte, 300))
		p := NewControlResponsePoller(newFrameSub(t, split(t, response(testSession, 3, 0, codecs.ControlResponseCode.ERROR, long), 4)), 10)
		n, err := p.Poll()
		require.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.True(t, p.IsPollComplete())
		assert.Len(t, p.ErrorMessage(), 300)
	})

	t.Run("challenge and signal", func(t *testing.T) {
		frames := whole(t,
			&codecs.Challenge{ControlSessionId: testSession, CorrelationId: 4, EncodedChallenge: []byte("c")},
			signal(testSession, 5, 6, codecs.RecordingSignal.STOP))
		p := NewControlResponsePoller(newFrameSub(t, frames), 10)
		_, _ = p.Poll()
		assert.True(t, p.WasChallenged())
		assert.Equal(t, []byte("c"), p.EncodedChallenge())
		_, _ = p.Poll()
		assert.True(t, p.IsRecordingSignal())
		assert.Equal(t, codecs.RecordingSignal.STOP, p.RecordingSignal())
		assert.EqualValues(t, 6, p.RecordingId())
		assert.EqualValues(t, 7, p.SubscriptionId())
		assert.EqualValues(t, 128, p.Position())
	})

	t.Run("schema mismatch is an ArchiveException", func(t *testing.T) {
		p := NewControlResponsePoller(newFrameSub(t, []testFrame{wrongSchema(t)}), 10)
		_, err := p.Poll()
		var ex *ArchiveException
		require.True(t, errors.As(err, &ex))
		assert.Contains(t, ex.Message, "expected schemaId=101, actual=999")
	})
}

func TestRecordingDescriptorPoller(t *testing.T) {
	t.Run("filters by session and correlation and stops at count", func(t *testing.T) {
		frames := whole(t,
			descriptor(otherSession, 1, 100),
			descriptor(testSession, 2, 101),
			signal(testSession, 9, 1, codecs.RecordingSignal.START),
			signal(otherSession, 9, 1, codecs.RecordingSignal.STOP),
			unknownTemplate(),
			descriptor(testSession, 1, 102),
			descriptor(testSession, 1, 103),
			descriptor(testSession, 1, 104))
		frames = append(frames, split(t, descriptor(testSession, 1, 105), 3)...)
		signals := &signalRecorder{}
		p := NewRecordingDescriptorPoller(newFrameSub(t, frames), nil, signals, testSession, 3)
		var ids []int64
		p.Reset(1, 3, RecordingDescriptorConsumerFunc(func(_, _, recordingId, _, _, _, _ int64, _, _, _, _, _, streamId int32, stripped, original, _ string) {
			ids = append(ids, recordingId)
			assert.EqualValues(t, 1001, streamId)
			assert.Equal(t, "aeron:ipc", stripped)
			assert.Equal(t, "aeron:ipc?alias=x", original)
		}))
		for i := 0; i < 10 && !p.IsDispatchComplete(); i++ {
			_, err := p.Poll()
			require.NoError(t, err)
		}
		assert.Equal(t, []int64{102, 103, 104}, ids)
		assert.True(t, p.IsDispatchComplete())
		assert.Equal(t, []codecs.RecordingSignalEnum{codecs.RecordingSignal.START}, signals.signals)

		// The next request picks up the fragmented descriptor that was left in the stream
		ids = nil
		p.Reset(1, 1, RecordingDescriptorConsumerFunc(func(_, _, recordingId, _, _, _, _ int64, _, _, _, _, _, _ int32, _, _, _ string) {
			ids = append(ids, recordingId)
		}))
		_, err := p.Poll()
		require.NoError(t, err)
		assert.Equal(t, []int64{105}, ids)
	})

	t.Run("completes on RECORDING_UNKNOWN for the request only", func(t *testing.T) {
		frames := whole(t,
			response(testSession, 2, 0, codecs.ControlResponseCode.RECORDING_UNKNOWN, ""),
			response(otherSession, 1, 0, codecs.ControlResponseCode.RECORDING_UNKNOWN, ""),
			descriptor(testSession, 1, 100),
			response(testSession, 1, 0, codecs.ControlResponseCode.RECORDING_UNKNOWN, ""))
		p := NewRecordingDescriptorPoller(newFrameSub(t, frames), nil, nil, testSession, 10)
		count := 0
		p.Reset(1, 5, RecordingDescriptorConsumerFunc(func(_, _, _, _, _, _, _ int64, _, _, _, _, _, _ int32, _, _, _ string) { count++ }))
		_, err := p.Poll()
		require.NoError(t, err)
		assert.True(t, p.IsDispatchComplete())
		assert.Equal(t, 1, count)
		assert.EqualValues(t, 4, p.RemainingRecordCount())
	})

	t.Run("errors", func(t *testing.T) {
		frames := whole(t,
			response(otherSession, 1, 5, codecs.ControlResponseCode.ERROR, "other session"),
			response(testSession, 2, int64(ErrorCodeActiveListing), codecs.ControlResponseCode.ERROR, "other request"),
			response(testSession, 1, int64(ErrorCodeUnknownRecording), codecs.ControlResponseCode.ERROR, "ours"))
		var handled []error
		p := NewRecordingDescriptorPoller(newFrameSub(t, frames), func(err error) { handled = append(handled, err) }, nil, testSession, 10)
		p.Reset(1, 5, nil)
		_, err := p.Poll()
		code, ok := ArchiveErrorCodeOf(err)
		require.True(t, ok)
		assert.Equal(t, ErrorCodeUnknownRecording, code)
		assert.Contains(t, err.Error(), "ours")
		require.Len(t, handled, 1)
		code, _ = ArchiveErrorCodeOf(handled[0])
		assert.Equal(t, ErrorCodeActiveListing, code)
	})
}

func TestRecordingSubscriptionDescriptorPoller(t *testing.T) {
	frames := whole(t,
		&codecs.RecordingSubscriptionDescriptor{ControlSessionId: otherSession, CorrelationId: 1, SubscriptionId: 1, StrippedChannel: []byte("a")},
		&codecs.RecordingSubscriptionDescriptor{ControlSessionId: testSession, CorrelationId: 1, SubscriptionId: 2, StreamId: 3, StrippedChannel: []byte("b")},
		response(testSession, 1, 0, codecs.ControlResponseCode.RECORDING_UNKNOWN, ""),
		response(testSession, 1, 0, codecs.ControlResponseCode.SUBSCRIPTION_UNKNOWN, ""))
	p := NewRecordingSubscriptionDescriptorPoller(newFrameSub(t, frames), nil, nil, testSession, 10)
	var subs []int64
	p.Reset(1, 5, RecordingSubscriptionDescriptorConsumerFunc(func(_, _, subscriptionId int64, streamId int32, channel string) {
		subs = append(subs, subscriptionId)
		assert.Equal(t, "b", channel)
		assert.EqualValues(t, 3, streamId)
	}))
	n, err := p.Poll()
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.True(t, p.IsDispatchComplete())
	assert.Equal(t, []int64{2}, subs)
	assert.EqualValues(t, 4, p.RemainingSubscriptionCount())
}

func TestRecordingSignalPollerAndAdapter(t *testing.T) {
	msgs := func() []testFrame {
		return whole(t,
			signal(otherSession, 1, 1, codecs.RecordingSignal.START),
			response(otherSession, 1, 0, codecs.ControlResponseCode.OK, ""),
			signal(testSession, 2, 3, codecs.RecordingSignal.EXTEND),
			response(testSession, 4, 5, codecs.ControlResponseCode.OK, ""))
	}

	p := NewRecordingSignalPoller(testSession, newFrameSub(t, msgs()), 10)
	n, err := p.Poll()
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, codecs.RecordingSignal.EXTEND, p.RecordingSignal())
	assert.EqualValues(t, 3, p.RecordingId())
	assert.EqualValues(t, 7, p.RecordingSubscriptionId())
	assert.EqualValues(t, 128, p.RecordingPosition())
	_, _ = p.Poll()
	assert.True(t, p.IsPollComplete())
	assert.EqualValues(t, 4, p.CorrelationId())
	assert.EqualValues(t, 5, p.RelevantId())
	assert.Equal(t, codecs.RecordingSignal.NullValue, p.RecordingSignal())

	signals := &signalRecorder{}
	var responses []int64
	a := NewRecordingSignalAdapter(testSession, ControlEventListenerFunc(func(_, correlationId, _ int64, _ codecs.ControlResponseCodeEnum, _ string) {
		responses = append(responses, correlationId)
	}), signals, newFrameSub(t, msgs()), 10)
	_, _ = a.Poll()
	assert.True(t, a.IsDone())
	assert.Equal(t, []codecs.RecordingSignalEnum{codecs.RecordingSignal.EXTEND}, signals.signals)
	assert.Empty(t, responses)
	_, _ = a.Poll()
	assert.Equal(t, []int64{4}, responses)
	_, _ = a.Poll()
	assert.False(t, a.IsDone())
}

type responseRecorder struct {
	responses   []int64
	descriptors []int64
}

func (r *responseRecorder) OnResponse(_, correlationId, _ int64, _ codecs.ControlResponseCodeEnum, _ string) {
	r.responses = append(r.responses, correlationId)
}

func (r *responseRecorder) OnRecordingDescriptor(_, _, recordingId, _, _, _, _ int64, _, _, _, _, _, _ int32, _, _, _ string) {
	r.descriptors = append(r.descriptors, recordingId)
}

func TestControlResponseAdapter(t *testing.T) {
	frames := whole(t,
		response(otherSession, 1, 0, codecs.ControlResponseCode.OK, ""),
		descriptor(testSession, 2, 100),
		unknownTemplate(),
		signal(testSession, 3, 1, codecs.RecordingSignal.DELETE))
	frames = append(frames, split(t, response(testSession, 4, 0, codecs.ControlResponseCode.OK, ""), 2)...)
	listener := &responseRecorder{}
	signals := &signalRecorder{}
	a := NewControlResponseAdapter(listener, signals, newFrameSub(t, frames), 10)
	n, err := a.Poll()
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, []int64{1, 4}, listener.responses)
	assert.Equal(t, []int64{100}, listener.descriptors)
	assert.Equal(t, []codecs.RecordingSignalEnum{codecs.RecordingSignal.DELETE}, signals.signals)
}

func recordingEvents(t *testing.T) []testFrame {
	return whole(t,
		&codecs.RecordingStarted{RecordingId: 1, StartPosition: 0, SessionId: 2, StreamId: 3, Channel: []byte("ch"), SourceIdentity: []byte("src")},
		response(testSession, 1, 0, codecs.ControlResponseCode.OK, ""),
		&codecs.RecordingProgress{RecordingId: 1, StartPosition: 0, Position: 64},
		&codecs.RecordingStopped{RecordingId: 1, StartPosition: 0, StopPosition: 128})
}

func TestRecordingEventsAdapter(t *testing.T) {
	var events []string
	listener := RecordingEventsListenerFuncs{
		Start: func(recordingId, startPosition int64, sessionId, streamId int32, channel, sourceIdentity string) {
			events = append(events, fmt.Sprintf("start %d %d %d %d %s %s", recordingId, startPosition, sessionId, streamId, channel, sourceIdentity))
		},
		Progress: func(recordingId, startPosition, position int64) {
			events = append(events, fmt.Sprintf("progress %d %d %d", recordingId, startPosition, position))
		},
		Stop: func(recordingId, startPosition, stopPosition int64) {
			events = append(events, fmt.Sprintf("stop %d %d %d", recordingId, startPosition, stopPosition))
		},
	}
	a := NewRecordingEventsAdapter(listener, newFrameSub(t, recordingEvents(t)), 10)
	n, err := a.Poll()
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []string{"start 1 0 2 3 ch src", "progress 1 0 64", "stop 1 0 128"}, events)

	t.Run("archive-owned adapter drives legacy listeners", func(t *testing.T) {
		var started *codecs.RecordingStarted
		var stopped *codecs.RecordingStopped
		archive := &Archive{Options: DefaultOptions(), Listeners: &ArchiveListeners{
			RecordingEventStartedListener: func(e *codecs.RecordingStarted) { started = e },
			RecordingEventStoppedListener: func(e *codecs.RecordingStopped) { stopped = e },
		}}
		rea := &RecordingEventsAdapter{Subscription: newFrameSub(t, recordingEvents(t)), Enabled: true, archive: archive}
		total := 0
		for i := 0; i < 5; i++ {
			total += rea.PollWithContext(nil, 1)
		}
		assert.Equal(t, 4, total)
		require.NotNil(t, started)
		assert.Equal(t, "ch", string(started.Channel))
		require.NotNil(t, stopped)
		assert.EqualValues(t, 128, stopped.StopPosition)
	})
}

func TestRecordingEventsPoller(t *testing.T) {
	p := NewRecordingEventsPoller(newFrameSub(t, recordingEvents(t)))
	_, err := p.Poll()
	require.NoError(t, err)
	assert.True(t, p.IsRecordingStarted())
	assert.EqualValues(t, 0, p.RecordingPosition())
	assert.EqualValues(t, aeron.NullValue, p.RecordingStopPosition())

	_, _ = p.Poll() // the control response is skipped, one fragment per poll
	assert.False(t, p.IsPollComplete())

	_, _ = p.Poll()
	assert.True(t, p.IsRecordingProgress())
	assert.EqualValues(t, 64, p.RecordingPosition())

	_, _ = p.Poll()
	assert.True(t, p.IsRecordingStopped())
	assert.EqualValues(t, 128, p.RecordingStopPosition())
	assert.EqualValues(t, 128, p.RecordingPosition())
	assert.EqualValues(t, 1, p.RecordingId())

	n, _ := p.Poll()
	assert.Zero(t, n)
	assert.EqualValues(t, aeron.NullValue, p.TemplateId())
}

func newFrameControl(t *testing.T, frames []testFrame, listeners *ArchiveListeners) *Control {
	control, _ := newTestControl(t)
	control.Subscription = newFrameSub(t, frames)
	control.archive.Listeners = listeners
	control.archive.SessionID = testSession
	return control
}

func TestControl_ResponseFiltering(t *testing.T) {
	t.Run("PollForResponse skips other sessions and requests, dispatches signals and other errors", func(t *testing.T) {
		var signals []codecs.RecordingSignalEnum
		var errs []error
		frames := whole(t,
			response(otherSession, 1, 0, codecs.ControlResponseCode.ERROR, "other session"),
			response(testSession, 2, 0, codecs.ControlResponseCode.OK, ""),
			response(testSession, 3, int64(ErrorCodeActiveRecording), codecs.ControlResponseCode.ERROR, "other request"),
			descriptor(testSession, 9, 1),
			unknownTemplate(),
			signal(otherSession, 1, 1, codecs.RecordingSignal.SYNC),
			signal(testSession, 1, 1, codecs.RecordingSignal.MERGE))
		frames = append(frames, split(t, response(testSession, 1, 77, codecs.ControlResponseCode.OK, ""), 3)...)
		control := newFrameControl(t, frames, &ArchiveListeners{
			RecordingSignalListener: func(e *codecs.RecordingSignalEvent) { signals = append(signals, e.Signal) },
			ErrorListener:           func(err error) { errs = append(errs, err) },
		})
		relevantId, err := control.PollForResponse(1, testSession)
		require.NoError(t, err)
		assert.EqualValues(t, 77, relevantId)
		assert.Equal(t, []codecs.RecordingSignalEnum{codecs.RecordingSignal.MERGE}, signals)
		require.Len(t, errs, 1)
		code, _ := ArchiveErrorCodeOf(errs[0])
		assert.Equal(t, ErrorCodeActiveRecording, code)
	})

	t.Run("PollForResponse non-OK code is an error", func(t *testing.T) {
		control := newFrameControl(t, whole(t, response(testSession, 1, 0, codecs.ControlResponseCode.RECORDING_UNKNOWN, "")), &ArchiveListeners{})
		_, err := control.PollForResponse(1, testSession)
		var ex *ArchiveException
		require.True(t, errors.As(err, &ex))
		assert.Contains(t, ex.Message, "unexpected response code")
	})

	t.Run("PollForDescriptors collects matching descriptors", func(t *testing.T) {
		frames := whole(t,
			descriptor(testSession, 2, 100),
			descriptor(otherSession, 1, 101),
			descriptor(testSession, 1, 102),
			response(testSession, 1, 0, codecs.ControlResponseCode.RECORDING_UNKNOWN, ""),
			response(testSession, 5, 0, codecs.ControlResponseCode.OK, ""))
		control := newFrameControl(t, frames, &ArchiveListeners{})
		require.NoError(t, control.PollForDescriptors(1, testSession, 10))
		require.Len(t, control.Results.RecordingDescriptors, 1)
		assert.EqualValues(t, 102, control.Results.RecordingDescriptors[0].RecordingId)
		assert.Equal(t, codecs.ControlResponseCode.RECORDING_UNKNOWN, control.Results.ControlResponse.Code)

		// The following response is still available to the next request
		relevantId, err := control.PollForResponse(5, testSession)
		require.NoError(t, err)
		assert.Zero(t, relevantId)
	})

	t.Run("PollForDescriptors stops at the requested count", func(t *testing.T) {
		frames := whole(t, descriptor(testSession, 1, 100), descriptor(testSession, 1, 101), descriptor(testSession, 1, 102))
		control := newFrameControl(t, frames, &ArchiveListeners{})
		require.NoError(t, control.PollForDescriptors(1, testSession, 2))
		assert.Len(t, control.Results.RecordingDescriptors, 2)
	})

	t.Run("PollForDescriptors returns ArchiveException", func(t *testing.T) {
		frames := whole(t, response(testSession, 1, int64(ErrorCodeActiveListing), codecs.ControlResponseCode.ERROR, "busy"))
		control := newFrameControl(t, frames, &ArchiveListeners{})
		err := control.PollForDescriptors(1, testSession, 10)
		code, ok := ArchiveErrorCodeOf(err)
		require.True(t, ok)
		assert.Equal(t, ErrorCodeActiveListing, code)
	})

	t.Run("Poll sets ErrorResponse for ERROR responses", func(t *testing.T) {
		frames := whole(t, signal(testSession, 1, 1, codecs.RecordingSignal.REPLICATE),
			response(testSession, 1, int64(ErrorCodeUnknownReplay), codecs.ControlResponseCode.ERROR, "x"))
		var signals []codecs.RecordingSignalEnum
		control := newFrameControl(t, frames, &ArchiveListeners{
			RecordingSignalListener: func(e *codecs.RecordingSignalEvent) { signals = append(signals, e.Signal) },
		})
		assert.Equal(t, 2, control.Poll())
		assert.True(t, control.Results.IsPollComplete)
		code, ok := ArchiveErrorCodeOf(control.Results.ErrorResponse)
		assert.True(t, ok)
		assert.Equal(t, ErrorCodeUnknownReplay, code)
		assert.Equal(t, []codecs.RecordingSignalEnum{codecs.RecordingSignal.REPLICATE}, signals)
		assert.Zero(t, control.Poll())
		assert.Nil(t, control.Results.ControlResponse)
	})

	t.Run("PollForErrorResponse dispatches a signal once", func(t *testing.T) {
		count := 0
		frames := whole(t, signal(testSession, 1, 1, codecs.RecordingSignal.REPLICATE_END))
		control := newFrameControl(t, frames, &ArchiveListeners{
			RecordingSignalListener: func(*codecs.RecordingSignalEvent) { count++ },
		})
		n, err := control.PollForErrorResponse()
		require.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.Equal(t, 1, count)
	})
}
