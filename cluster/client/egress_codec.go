package client

import (
	"bytes"
	"fmt"
	"io"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/cluster/codecs"
)

const (
	// MessageHeaderLength is the SBE message header length.
	MessageHeaderLength int32 = 8
	// SessionHeaderLength is the length of the SBE header plus SessionMessageHeader prepended to ingress/egress
	// application messages (AeronCluster.SESSION_HEADER_LENGTH).
	SessionHeaderLength int32 = MessageHeaderLength + 24

	nullValue = -1
)

var (
	schemaId      = (&codecs.SessionMessageHeader{}).SbeSchemaId()
	schemaVersion = (&codecs.SessionMessageHeader{}).SbeSchemaVersion()

	sessionMessageHeaderTemplateId = (&codecs.SessionMessageHeader{}).SbeTemplateId()
	sessionEventTemplateId         = (&codecs.SessionEvent{}).SbeTemplateId()
	newLeaderEventTemplateId       = (&codecs.NewLeaderEvent{}).SbeTemplateId()
	challengeTemplateId            = (&codecs.Challenge{}).SbeTemplateId()
	adminResponseTemplateId        = (&codecs.AdminResponse{}).SbeTemplateId()
	sessionKeepAliveTemplateId     = (&codecs.SessionKeepAlive{}).SbeTemplateId()
	sessionCloseRequestTemplateId  = (&codecs.SessionCloseRequest{}).SbeTemplateId()
	adminRequestTemplateId         = (&codecs.AdminRequest{}).SbeTemplateId()

	sessionMessageHeaderBlockLength = (&codecs.SessionMessageHeader{}).SbeBlockLength()
	sessionKeepAliveBlockLength     = (&codecs.SessionKeepAlive{}).SbeBlockLength()
	sessionCloseRequestBlockLength  = (&codecs.SessionCloseRequest{}).SbeBlockLength()
	adminRequestBlockLength         = (&codecs.AdminRequest{}).SbeBlockLength()
)

// ProtocolSemanticVersion mirrors AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION (0.3.0 in 1.53.3).
const (
	ProtocolMajorVersion = 0
	ProtocolMinorVersion = 3
	ProtocolPatchVersion = 0
)

var ProtocolSemanticVersion = int32(ProtocolMajorVersion)<<16 | int32(ProtocolMinorVersion)<<8 |
	int32(ProtocolPatchVersion)

type messageHeader struct {
	blockLength, templateId, schemaId, version uint16
}

func readMessageHeader(buffer *atomic.Buffer, offset int32) messageHeader {
	return messageHeader{
		blockLength: buffer.GetUInt16(offset),
		templateId:  buffer.GetUInt16(offset + 2),
		schemaId:    buffer.GetUInt16(offset + 4),
		version:     buffer.GetUInt16(offset + 6),
	}
}

func writeMessageHeader(buffer *atomic.Buffer, offset int32, blockLength, templateId uint16) {
	buffer.PutUInt16(offset, blockLength)
	buffer.PutUInt16(offset+2, templateId)
	buffer.PutUInt16(offset+4, schemaId)
	buffer.PutUInt16(offset+6, schemaVersion)
}

type sbeDecoder interface {
	Decode(m *codecs.SbeGoMarshaller, r io.Reader, actingVersion uint16, blockLength uint16, doRangeCheck bool) error
}

// decodeBody decodes the message body following the SBE header at offset.
func decodeBody(buffer *atomic.Buffer, offset, length int32, hdr messageHeader, msg sbeDecoder) error {
	bodyLength := length - MessageHeaderLength
	if bodyLength < int32(hdr.blockLength) {
		return fmt.Errorf("%w: message too short: templateId=%d length=%d blockLength=%d",
			ErrCluster, hdr.templateId, length, hdr.blockLength)
	}
	data := buffer.GetBytesArray(offset+MessageHeaderLength, bodyLength)
	return msg.Decode(marshaller, bytes.NewReader(data), hdr.version, hdr.blockLength, false)
}

type sbeEncoder interface {
	Encode(m *codecs.SbeGoMarshaller, w io.Writer, doRangeCheck bool) error
	SbeBlockLength() uint16
	SbeTemplateId() uint16
	SbeSchemaId() uint16
	SbeSchemaVersion() uint16
}

func encodeMessage(msg sbeEncoder) ([]byte, error) {
	header := codecs.MessageHeader{
		BlockLength: msg.SbeBlockLength(),
		TemplateId:  msg.SbeTemplateId(),
		SchemaId:    msg.SbeSchemaId(),
		Version:     msg.SbeSchemaVersion(),
	}
	w := new(bytes.Buffer)
	if err := header.Encode(marshaller, w); err != nil {
		return nil, err
	}
	if err := msg.Encode(marshaller, w, false); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// egressHandler receives decoded egress events for one cluster session.
type egressHandler interface {
	ControlledEgressListener
}

// dispatchEgress decodes a reassembled egress message and invokes the handler if it belongs to clusterSessionId.
// onNewLeaderHook, if non-nil, is called (instead of handler.OnNewLeader) for new leader events of this session.
func dispatchEgress(
	clusterSessionId int64,
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
	handler egressHandler,
	extension ControlledEgressListenerExtension,
	onNewLeaderHook func(header *logbuffer.Header, e *codecs.NewLeaderEvent) term.ControlledPollAction,
	onSessionEventHook func(e *codecs.SessionEvent),
	errorHandler func(error),
) term.ControlledPollAction {
	if length < MessageHeaderLength {
		return term.ControlledPollActionContinue
	}
	hdr := readMessageHeader(buffer, offset)
	if hdr.schemaId != schemaId {
		if extension != nil {
			return extension.OnExtensionMessage(hdr.blockLength, hdr.templateId, hdr.schemaId, hdr.version,
				buffer, offset+MessageHeaderLength, length-MessageHeaderLength)
		}
		errorHandler(fmt.Errorf("%w: expected cluster egress schemaId=%d actual=%d", ErrCluster, schemaId,
			hdr.schemaId))
		return term.ControlledPollActionContinue
	}

	switch hdr.templateId {
	case sessionMessageHeaderTemplateId:
		if length < SessionHeaderLength {
			errorHandler(fmt.Errorf("%w: invalid session message length=%d", ErrCluster, length))
			return term.ControlledPollActionContinue
		}
		body := offset + MessageHeaderLength
		sessionId := buffer.GetInt64(body + 8)
		if sessionId == clusterSessionId {
			timestamp := buffer.GetInt64(body + 16)
			return handler.OnMessage(sessionId, timestamp, buffer, offset+SessionHeaderLength,
				length-SessionHeaderLength, header)
		}

	case sessionEventTemplateId:
		var e codecs.SessionEvent
		if err := decodeBody(buffer, offset, length, hdr, &e); err != nil {
			errorHandler(err)
			return term.ControlledPollActionContinue
		}
		if e.ClusterSessionId == clusterSessionId {
			if onSessionEventHook != nil {
				onSessionEventHook(&e)
			}
			handler.OnSessionEvent(e.CorrelationId, e.ClusterSessionId, e.LeadershipTermId, e.LeaderMemberId,
				e.Code, string(e.Detail))
		}

	case newLeaderEventTemplateId:
		var e codecs.NewLeaderEvent
		if err := decodeBody(buffer, offset, length, hdr, &e); err != nil {
			errorHandler(err)
			return term.ControlledPollActionContinue
		}
		if e.ClusterSessionId == clusterSessionId {
			if onNewLeaderHook != nil {
				return onNewLeaderHook(header, &e)
			}
			handler.OnNewLeader(e.ClusterSessionId, e.LeadershipTermId, e.LeaderMemberId,
				string(e.IngressEndpoints))
		}

	case adminResponseTemplateId:
		var e codecs.AdminResponse
		if err := decodeBody(buffer, offset, length, hdr, &e); err != nil {
			errorHandler(err)
			return term.ControlledPollActionContinue
		}
		if e.ClusterSessionId == clusterSessionId {
			payloadOffset := offset + MessageHeaderLength + int32(hdr.blockLength) + 4 + int32(len(e.Message)) + 4
			handler.OnAdminResponse(e.ClusterSessionId, e.CorrelationId, e.RequestType, e.ResponseCode,
				string(e.Message), buffer, payloadOffset, int32(len(e.Payload)))
		}
	}
	return term.ControlledPollActionContinue
}

func logError(err error) {
	logger.Errorf("%v", err)
}
