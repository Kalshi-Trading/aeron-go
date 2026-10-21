package cluster

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
)

const (
	scheduleTimerBlockLength       = 16
	cancelTimerBlockLength         = 8
	serviceAckBlockLength          = 36
	closeSessionBlockLength        = 8
	clusterMembersQueryBlockLength = 12
	sendAttempts                   = 3
)

// consensusModuleProxy encodes messages from the service to the consensus module on the control channel.
// Mirrors io.aeron.cluster.service.ConsensusModuleProxy for schema v17.
type consensusModuleProxy struct {
	publication *aeron.Publication
	buffer      *atomic.Buffer
}

func newConsensusModuleProxy(publication *aeron.Publication) *consensusModuleProxy {
	return &consensusModuleProxy{
		publication: publication,
		buffer:      atomic.MakeBuffer(make([]byte, 128)),
	}
}

func (proxy *consensusModuleProxy) ack(logPosition, timestamp, ackId, relevantId int64, serviceId int32) bool {
	buf := proxy.initBuffer(serviceAckTemplateId, serviceAckBlockLength)
	buf.PutInt64(SBEHeaderLength, logPosition)
	buf.PutInt64(SBEHeaderLength+8, timestamp)
	buf.PutInt64(SBEHeaderLength+16, ackId)
	buf.PutInt64(SBEHeaderLength+24, relevantId)
	buf.PutInt32(SBEHeaderLength+32, serviceId)
	return proxy.send(buf, SBEHeaderLength+serviceAckBlockLength)
}

func (proxy *consensusModuleProxy) closeSession(clusterSessionId int64) bool {
	buf := proxy.initBuffer(closeSessionTemplateId, closeSessionBlockLength)
	buf.PutInt64(SBEHeaderLength, clusterSessionId)
	return proxy.send(buf, SBEHeaderLength+closeSessionBlockLength)
}

func (proxy *consensusModuleProxy) scheduleTimer(correlationId int64, deadline int64) bool {
	buf := proxy.initBuffer(scheduleTimerTemplateId, scheduleTimerBlockLength)
	buf.PutInt64(SBEHeaderLength, correlationId)
	buf.PutInt64(SBEHeaderLength+8, deadline)
	return proxy.send(buf, SBEHeaderLength+scheduleTimerBlockLength)
}

func (proxy *consensusModuleProxy) cancelTimer(correlationId int64) bool {
	buf := proxy.initBuffer(cancelTimerTemplateId, cancelTimerBlockLength)
	buf.PutInt64(SBEHeaderLength, correlationId)
	return proxy.send(buf, SBEHeaderLength+cancelTimerBlockLength)
}

// clusterMembersQuery requests the cluster membership; the response is delivered as egress to the requester
// (ClusterMembersResponse/ClusterMembersExtendedResponse), matching ConsensusModuleProxy.clusterMembersQuery.
func (proxy *consensusModuleProxy) clusterMembersQuery(correlationId int64, extended bool) bool {
	buf := proxy.initBuffer(clusterMembersQueryTemplateId, clusterMembersQueryBlockLength)
	buf.PutInt64(SBEHeaderLength, correlationId)
	var ext int32
	if extended {
		ext = 1
	}
	buf.PutInt32(SBEHeaderLength+8, ext)
	return proxy.send(buf, SBEHeaderLength+clusterMembersQueryBlockLength)
}

func (proxy *consensusModuleProxy) initBuffer(templateId uint16, blockLength uint16) *atomic.Buffer {
	buf := proxy.buffer
	buf.PutUInt16(0, blockLength)
	buf.PutUInt16(2, templateId)
	buf.PutUInt16(4, ClusterSchemaId)
	buf.PutUInt16(6, ClusterSchemaVersion)
	return buf
}

func (proxy *consensusModuleProxy) send(buffer *atomic.Buffer, length int32) bool {
	for i := 0; i < sendAttempts; i++ {
		result := proxy.publication.Offer(buffer, 0, length, nil)
		if result > 0 {
			return true
		}
		checkResult(result)
	}
	return false
}

// offer2 sends a service-originated session message (header + payload). Not retried, matching Java.
func (proxy *consensusModuleProxy) offer2(
	bufferOne *atomic.Buffer, offsetOne int32, lengthOne int32,
	bufferTwo *atomic.Buffer, offsetTwo int32, lengthTwo int32,
) int64 {
	return proxy.publication.Offer2(bufferOne, offsetOne, lengthOne, bufferTwo, offsetTwo, lengthTwo, nil)
}

func (proxy *consensusModuleProxy) tryClaim(length int32, claim *logbuffer.Claim) int64 {
	return proxy.publication.TryClaim(length, claim)
}

// ClusterError is raised (via panic inside the agent, returned from StartAndRun) for unrecoverable cluster
// protocol failures, analogous to io.aeron.cluster.client.ClusterException.
type ClusterError struct {
	Msg string
}

func (e *ClusterError) Error() string {
	return "ClusterError: " + e.Msg
}

func newClusterError(format string, args ...interface{}) *ClusterError {
	return &ClusterError{Msg: fmt.Sprintf(format, args...)}
}

func checkResult(result int64) {
	if result == aeron.NotConnected || result == aeron.PublicationClosed || result == aeron.MaxPositionExceeded {
		panic(newClusterError("unexpected publication state: %d", result))
	}
}
