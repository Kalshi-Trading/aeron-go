package cluster

import (
	"bytes"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster/codecs"
)

const serviceAdapterFragmentLimit = 1

// serviceAdapterHandler receives the consensus module to service messages decoded by serviceAdapter.
type serviceAdapterHandler interface {
	onJoinLog(logPosition, maxLogPosition int64, memberId, logSessionId, logStreamId int32,
		isStartup bool, role Role, logChannel string, isStandby bool)
	onServiceTerminationPosition(logPosition int64, ackRequired bool)
	onRequestServiceAck(logPosition int64)
}

// serviceAdapter mirrors io.aeron.cluster.service.ServiceAdapter.
type serviceAdapter struct {
	marshaller   *codecs.SbeGoMarshaller
	handler      serviceAdapterHandler
	subscription *aeron.Subscription
	assembler    *aeron.FragmentAssembler
}

func newServiceAdapter(subscription *aeron.Subscription, handler serviceAdapterHandler) *serviceAdapter {
	adapter := &serviceAdapter{
		marshaller:   codecs.NewSbeGoMarshaller(),
		handler:      handler,
		subscription: subscription,
	}
	adapter.assembler = aeron.NewFragmentAssembler(adapter.onFragment, 0)
	return adapter
}

func (adapter *serviceAdapter) poll() int {
	if adapter.subscription.IsClosed() {
		panic(newClusterError("service control subscription closed"))
	}
	return adapter.subscription.Poll(adapter.assembler.OnFragment, serviceAdapterFragmentLimit)
}

func (adapter *serviceAdapter) close() {
	if adapter.subscription != nil {
		closeSubscription(adapter.subscription)
	}
}

func (adapter *serviceAdapter) onFragment(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) {
	if length < SBEHeaderLength {
		return
	}
	blockLength := buffer.GetUInt16(offset)
	templateId := buffer.GetUInt16(offset + 2)
	schemaId := buffer.GetUInt16(offset + 4)
	version := buffer.GetUInt16(offset + 6)
	if schemaId != ClusterSchemaId {
		panic(newClusterError("expected schemaId=%d, actual=%d", ClusterSchemaId, schemaId))
	}
	offset += SBEHeaderLength
	length -= SBEHeaderLength

	switch templateId {
	case joinLogTemplateId:
		buf := &bytes.Buffer{}
		buffer.WriteBytes(buf, offset, length)
		joinLog := &codecs.JoinLog{}
		if err := joinLog.Decode(adapter.marshaller, buf, version, blockLength, true); err != nil {
			panic(newClusterError("join log decode error: %v", err))
		}
		adapter.handler.onJoinLog(
			joinLog.LogPosition,
			joinLog.MaxLogPosition,
			joinLog.MemberId,
			joinLog.LogSessionId,
			joinLog.LogStreamId,
			joinLog.IsStartup == codecs.BooleanType.TRUE,
			Role(joinLog.Role),
			string(joinLog.LogChannel),
			joinLog.IsStandby == codecs.BooleanType.TRUE,
		)
	case serviceTerminationPosTemplateId:
		logPos := buffer.GetInt64(offset)
		ackRequired := true
		if blockLength >= 12 {
			ackRequired = codecs.BooleanTypeEnum(buffer.GetInt32(offset+8)) == codecs.BooleanType.TRUE
		}
		adapter.handler.onServiceTerminationPosition(logPos, ackRequired)
	case requestServiceAckTemplateId:
		adapter.handler.onRequestServiceAck(buffer.GetInt64(offset))
	default:
		logger.Debugf("serviceAdapter: unexpected templateId=%d at pos=%d", templateId, header.Position())
	}
}
