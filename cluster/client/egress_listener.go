package client

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/cluster/codecs"
)

// EgressListener receives egress events from the cluster, mirroring io.aeron.cluster.client.EgressListener.
// Embed NoOpEgressEvents to only implement OnMessage.
type EgressListener interface {
	// OnMessage is a message returned from the clustered service. buffer[offset:offset+length] is the payload
	// after the session header.
	OnMessage(clusterSessionId int64, timestamp int64, buffer *atomic.Buffer, offset int32, length int32,
		header *logbuffer.Header)

	// OnSessionEvent is a session event which after connect can indicate an error or session close.
	OnSessionEvent(correlationId int64, clusterSessionId int64, leadershipTermId int64, leaderMemberId int32,
		code codecs.EventCodeEnum, detail string)

	// OnNewLeader indicates a new leader has been elected.
	OnNewLeader(clusterSessionId int64, leadershipTermId int64, leaderMemberId int32, ingressEndpoints string)

	// OnAdminResponse is the response to an admin request; payload[payloadOffset:payloadOffset+payloadLength].
	OnAdminResponse(clusterSessionId int64, correlationId int64, requestType codecs.AdminRequestTypeEnum,
		responseCode codecs.AdminResponseCodeEnum, message string, payload *atomic.Buffer, payloadOffset int32,
		payloadLength int32)
}

// ControlledEgressListener is the controlled-poll variant of EgressListener where OnMessage controls stream
// advancement, mirroring io.aeron.cluster.client.ControlledEgressListener.
type ControlledEgressListener interface {
	OnMessage(clusterSessionId int64, timestamp int64, buffer *atomic.Buffer, offset int32, length int32,
		header *logbuffer.Header) term.ControlledPollAction

	OnSessionEvent(correlationId int64, clusterSessionId int64, leadershipTermId int64, leaderMemberId int32,
		code codecs.EventCodeEnum, detail string)

	OnNewLeader(clusterSessionId int64, leadershipTermId int64, leaderMemberId int32, ingressEndpoints string)

	OnAdminResponse(clusterSessionId int64, correlationId int64, requestType codecs.AdminRequestTypeEnum,
		responseCode codecs.AdminResponseCodeEnum, message string, payload *atomic.Buffer, payloadOffset int32,
		payloadLength int32)
}

// EgressListenerExtension handles egress messages with a schema other than the cluster schema.
type EgressListenerExtension interface {
	OnExtensionMessage(actingBlockLength uint16, templateId uint16, schemaId uint16, actingVersion uint16,
		buffer *atomic.Buffer, offset int32, length int32)
}

// ControlledEgressListenerExtension handles egress messages with a schema other than the cluster schema.
type ControlledEgressListenerExtension interface {
	OnExtensionMessage(actingBlockLength uint16, templateId uint16, schemaId uint16, actingVersion uint16,
		buffer *atomic.Buffer, offset int32, length int32) term.ControlledPollAction
}

// NoOpEgressEvents provides no-op implementations of the optional egress listener callbacks (the Java default
// methods). Embed it in a listener struct.
type NoOpEgressEvents struct{}

func (NoOpEgressEvents) OnSessionEvent(int64, int64, int64, int32, codecs.EventCodeEnum, string) {}
func (NoOpEgressEvents) OnNewLeader(int64, int64, int32, string)                                 {}
func (NoOpEgressEvents) OnAdminResponse(int64, int64, codecs.AdminRequestTypeEnum, codecs.AdminResponseCodeEnum,
	string, *atomic.Buffer, int32, int32) {
}

// EgressMessageHandler is a function implementing only OnMessage.
type EgressMessageHandler func(clusterSessionId int64, timestamp int64, buffer *atomic.Buffer, offset int32,
	length int32, header *logbuffer.Header)

// EgressListenerFunc adapts an EgressMessageHandler to an EgressListener with no-op events.
func EgressListenerFunc(handler EgressMessageHandler) EgressListener {
	return egressListenerFunc{handler: handler}
}

type egressListenerFunc struct {
	NoOpEgressEvents
	handler EgressMessageHandler
}

func (f egressListenerFunc) OnMessage(clusterSessionId int64, timestamp int64, buffer *atomic.Buffer,
	offset int32, length int32, header *logbuffer.Header) {
	f.handler(clusterSessionId, timestamp, buffer, offset, length, header)
}

type missingEgressListener struct{ NoOpEgressEvents }

func (missingEgressListener) OnMessage(int64, int64, *atomic.Buffer, int32, int32, *logbuffer.Header) {
	logger.Errorf("EgressListener must be specified on client.Context")
}

type missingControlledEgressListener struct{ NoOpEgressEvents }

func (missingControlledEgressListener) OnMessage(int64, int64, *atomic.Buffer, int32, int32,
	*logbuffer.Header) term.ControlledPollAction {
	logger.Errorf("ControlledEgressListener must be specified on client.Context")
	return term.ControlledPollActionContinue
}

// uncontrolled adapts an EgressListener to the controlled dispatch used internally.
type uncontrolled struct{ EgressListener }

func (u uncontrolled) OnMessage(clusterSessionId int64, timestamp int64, buffer *atomic.Buffer, offset int32,
	length int32, header *logbuffer.Header) term.ControlledPollAction {
	u.EgressListener.OnMessage(clusterSessionId, timestamp, buffer, offset, length, header)
	return term.ControlledPollActionContinue
}

type uncontrolledExtension struct{ EgressListenerExtension }

func (u uncontrolledExtension) OnExtensionMessage(actingBlockLength uint16, templateId uint16, schemaId uint16,
	actingVersion uint16, buffer *atomic.Buffer, offset int32, length int32) term.ControlledPollAction {
	u.EgressListenerExtension.OnExtensionMessage(actingBlockLength, templateId, schemaId, actingVersion, buffer,
		offset, length)
	return term.ControlledPollActionContinue
}
