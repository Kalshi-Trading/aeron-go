package archive

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestControl_PollForResponse(t *testing.T) {
	t.Run("times out when nothing to poll", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image)
		correlations.Store(int64(1), control)
		id, err := control.PollForResponse(1, 0)
		assert.Zero(t, id)
        assert.EqualError(t, err, `timeout waiting for correlationID 1`)
	})

	t.Run("returns the error response", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image,
			&codecs.ControlResponse{
				Code:             codecs.ControlResponseCode.ERROR,
				CorrelationId:    1,
				ErrorMessage:     []byte(`b0rk`),
				RelevantId:       3,
			},
		)
		correlations.Store(int64(1), control)
		id, err := control.PollForResponse(1, 0)
		assert.EqualValues(t, 3, id)
		assert.EqualError(t, err, `Control Response failure: b0rk`)
	})

	t.Run("discards all responses preceding the first error", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image,
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
			&codecs.ControlResponse{
				Code:             codecs.ControlResponseCode.ERROR,
				CorrelationId:    1,
				ErrorMessage:     []byte(`b0rk`),
				RelevantId:       3,
			},
		)
		correlations.Store(int64(1), control)
		id, err := control.PollForResponse(1, 0)
		assert.EqualValues(t, 3, id)
		assert.EqualError(t, err, `Control Response failure: b0rk`)
	})

	t.Run("does not process messages after result", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image,
			&codecs.ControlResponse{
				Code:             codecs.ControlResponseCode.ERROR,
				CorrelationId:    1,
				ErrorMessage:     []byte(`b0rk`),
				RelevantId:       3,
			},
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
		)
		correlations.Store(int64(1), control)
		id, err := control.PollForResponse(1, 0)
		assert.EqualValues(t, 3, id)
		assert.EqualError(t, err, `Control Response failure: b0rk`)
		fragments := image.Poll(func(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) {}, 1)
		assert.EqualValues(t, 1, fragments)
	})
}

func TestControl_PollForErrorResponse(t *testing.T) {
	t.Run("returns zero when nothing to poll", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image)
		cnt, err := control.PollForErrorResponse()
		assert.Zero(t, cnt)
		assert.NoError(t, err)
	})

	t.Run("returns the error response", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image,
			&codecs.ControlResponse{
				Code:         codecs.ControlResponseCode.ERROR,
				ErrorMessage: []byte(`b0rk`),
			},
		)
		cnt, err := control.PollForErrorResponse()
		assert.EqualValues(t, 1, cnt)
		assert.EqualError(t, err, `PollForErrorResponse received a ControlResponse (correlationId:0 Code:ERROR error="b0rk"`)
	})

	t.Run("discards all queued responses unless error", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image,
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
		)
		cnt, err := control.PollForErrorResponse()
		assert.EqualValues(t, 2, cnt)
		assert.NoError(t, err)
	})

	t.Run("does not process messages after error", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image,
			&codecs.ControlResponse{
				Code:         codecs.ControlResponseCode.ERROR,
				ErrorMessage: []byte(`b0rk`),
			},
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
		)
		cnt, err := control.PollForErrorResponse()
		assert.EqualValues(t, 1, cnt)
		assert.Error(t, err)
		cnt, err = control.PollForErrorResponse()
		assert.EqualValues(t, 1, cnt)
		assert.NoError(t, err)
	})
}

func TestControl_SkipsPingAndLogsUnknownTemplatesAtDebug(t *testing.T) {
	ping := rawMessage{templateID: 106, body: binary.LittleEndian.AppendUint64(nil, 7)}
	unknown := rawMessage{templateID: 9999, body: make([]byte, 8)}

	tests := []struct {
		name      string
		wantDebug string
		run       func(t *testing.T, control *Control, image *aeron.MockImage)
	}{
		{
			name:      "PollForResponse",
			wantDebug: "controlFragmentHandler: Unexpected message type 9999",
			run: func(t *testing.T, control *Control, image *aeron.MockImage) {
				mockPollResponses(t, image, ping, unknown,
					&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK, CorrelationId: 1, RelevantId: 3})
				correlations.Store(int64(1), control)
				id, err := control.PollForResponse(1, 0)
				assert.NoError(t, err)
				assert.EqualValues(t, 3, id)
			},
		},
		{
			name:      "PollForErrorResponse",
			wantDebug: "errorResponseFragmentHandler: Insert decoder for type: 9999",
			run: func(t *testing.T, control *Control, image *aeron.MockImage) {
				mockPollResponses(t, image, ping, unknown,
					&codecs.ControlResponse{Code: codecs.ControlResponseCode.ERROR, ErrorMessage: []byte(`b0rk`)})
				cnt, err := control.PollForErrorResponse()
				assert.EqualValues(t, 3, cnt)
				assert.EqualError(t, err, `PollForErrorResponse received a ControlResponse (correlationId:0 Code:ERROR error="b0rk"`)
			},
		},
		{
			name:      "PollForDescriptors",
			wantDebug: "descriptorFragmentHandler: Insert decoder for type: 9999",
			run: func(t *testing.T, control *Control, image *aeron.MockImage) {
				mockPollResponses(t, image, ping, unknown,
					&codecs.RecordingDescriptor{CorrelationId: 1, RecordingId: 5})
				correlations.Store(int64(1), control)
				assert.NoError(t, control.PollForDescriptors(1, 0, 1))
				if assert.Len(t, control.Results.RecordingDescriptors, 1) {
					assert.EqualValues(t, 5, control.Results.RecordingDescriptors[0].RecordingId)
				}
			},
		},
		{
			name:      "Poll",
			wantDebug: "descriptorFragmentHandler: Insert decoder for type: 9999",
			run: func(t *testing.T, control *Control, image *aeron.MockImage) {
				mockPollResponses(t, image, ping, unknown,
					&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK, CorrelationId: 1})
				assert.EqualValues(t, 3, control.Poll())
				assert.True(t, control.Results.IsPollComplete)
				assert.EqualValues(t, 1, control.Results.CorrelationId)
			},
		},
		{
			name:      "ConnectionControlFragmentHandler",
			wantDebug: "ConnectionControlFragmentHandler: Insert decoder for type: 9999",
			run: func(t *testing.T, control *Control, _ *aeron.MockImage) {
				correlations.Store(int64(1), control)
				control.State.state = ControlStateConnectRequestSent
				messages := []encodable{ping, unknown,
					&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK, CorrelationId: 1, ControlSessionId: 9}}
				for _, m := range messages {
					buffer := encode(t, m)
					ConnectionControlFragmentHandler(&PollContext{control, 1}, buffer, 0, buffer.Capacity(), newTestHeader())
				}
				assert.Equal(t, ControlStateConnected, control.State.state)
				assert.EqualValues(t, 9, control.archive.SessionID)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := observeLogs(t)
			control, image := newTestControl(t)
			stdout := captureStdout(t, func() { tt.run(t, control, image) })
			assert.Empty(t, stdout)
			templateLogs := logs.FilterMessageSnippet("type").All()
			if assert.Len(t, templateLogs, 1) {
				assert.Equal(t, zapcore.DebugLevel, templateLogs[0].Level)
				assert.Equal(t, tt.wantDebug, templateLogs[0].Message)
			}
		})
	}
}

type rawMessage struct {
	templateID uint16
	body       []byte
}

func (m rawMessage) SbeBlockLength() uint16 { return uint16(len(m.body)) }
func (m rawMessage) SbeTemplateId() uint16  { return m.templateID }
func (rawMessage) SbeSchemaId() uint16      { return 101 }
func (rawMessage) SbeSchemaVersion() uint16 { return 14 }

func (m rawMessage) Encode(_ *codecs.SbeGoMarshaller, w io.Writer, _ bool) error {
	_, err := w.Write(m.body)
	return err
}

func observeLogs(t *testing.T) *observer.ObservedLogs {
	core, logs := observer.New(zapcore.DebugLevel)
	sugar := logger.Sugar()
	logger.SetSugar(zap.New(core).Sugar())
	t.Cleanup(func() { logger.SetSugar(sugar) })
	return logs
}

func captureStdout(t *testing.T, fn func()) string {
	r, w, err := os.Pipe()
	if !assert.NoError(t, err) {
		return ""
	}
	stdout := os.Stdout
	os.Stdout = w
	func() {
		defer func() { os.Stdout = stdout }()
		fn()
	}()
	assert.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	return string(out)
}

func mockPollResponses(t *testing.T, image *aeron.MockImage, responses ...encodable) {
	image.On("IsClosed").Return(false).Maybe()
	poll := image.On("Poll", mock.Anything, mock.Anything)
	poll.Maybe()
	poll.Run(func(args mock.Arguments) {
		handler := args.Get(0).(term.FragmentHandler)
		fragmentCount := args.Get(1).(int)
		count := image.ControlledPoll(func(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) term.ControlledPollAction {
			handler(buffer, offset, length, header)
			return term.ControlledPollActionContinue
		}, fragmentCount)
		poll.Return(count)
	})
	controlledPoll := image.On("ControlledPoll", mock.Anything, mock.Anything)
	controlledPoll.Run(func(args mock.Arguments) {
		fragmentCount := args.Get(1).(int)
		count := 0
		for count < fragmentCount {
			if len(responses) == 0 {
				break
			}
			buffer := encode(t, responses[0])
			action := args.Get(0).(term.ControlledFragmentHandler)(buffer, 0, buffer.Capacity(), newTestHeader())
			if action == term.ControlledPollActionAbort {
				break
			}
			responses = responses[1:]
			count++
			if action == term.ControlledPollActionBreak {
				break
			}
		}
		controlledPoll.Return(count)
	})
}

type encodable interface {
	SbeBlockLength() uint16
	SbeTemplateId() uint16
	SbeSchemaId() uint16
	SbeSchemaVersion() uint16
	Encode(*codecs.SbeGoMarshaller, io.Writer, bool) error
}

func encode(t *testing.T, data encodable) *atomic.Buffer {
	m := codecs.NewSbeGoMarshaller()
	buf := new(bytes.Buffer)
	header := codecs.MessageHeader{
		BlockLength: data.SbeBlockLength(),
		TemplateId:  data.SbeTemplateId(),
		SchemaId:    data.SbeSchemaId(),
		Version:     data.SbeSchemaVersion(),
	}
	if !assert.NoError(t, header.Encode(m, buf)) {
		return nil
	}
	if !assert.NoError(t, data.Encode(m, buf, false)) {
		return nil
	}
	return atomic.MakeBuffer(buf.Bytes())
}

func newTestControl(t *testing.T) (*Control, *aeron.MockImage) {
	image := aeron.NewMockImage(t)
	c := &Control{
		Subscription: newTestSub(image),
	}
	c.archive = &Archive{
		Listeners: &ArchiveListeners{},
		Options:   &Options{
			Timeout: 100*time.Millisecond,
			IdleStrategy: idlestrategy.Yielding{},
		},
	}
	c.fragmentAssembler = aeron.NewControlledFragmentAssembler(
		c.onFragment, aeron.DefaultFragmentAssemblyBufferLength,
	)
	c.errorFragmentHandler = c.errorResponseFragmentHandler
	return c, image
}

func newTestSub(image aeron.Image) *aeron.Subscription {
	images := aeron.NewImageList()
	images.Set([]aeron.Image{image})
	sub := aeron.NewSubscription(nil, "", 1, 2, 3, nil, nil)
	rsub := reflect.ValueOf(sub)
	rfimages := rsub.Elem().FieldByName("images")
	rfimages = reflect.NewAt(rfimages.Type(), unsafe.Pointer(rfimages.UnsafeAddr())).Elem()
	rfimages.Set(reflect.ValueOf(images))
	return sub
}

func newTestHeader() *logbuffer.Header {
	buffer := atomic.MakeBuffer(make([]byte, logbuffer.DataFrameHeader.Length))
	buffer.PutUInt8(logbuffer.DataFrameHeader.FlagsFieldOffset, 0xc0) // unfragmented
	return new(logbuffer.Header).Wrap(buffer.Ptr(), buffer.Capacity())
}
