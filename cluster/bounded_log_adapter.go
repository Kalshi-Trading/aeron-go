// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package cluster

import (
	"bytes"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/cluster/codecs"
)

const (
	beginFrag    uint8 = 0x80
	endFrag      uint8 = 0x40
	unfragmented uint8 = 0x80 | 0x40
)

// logAdapterHandler receives the log events decoded by boundedLogAdapter.
type logAdapterHandler interface {
	onTimerEvent(logPosition, correlationId, timestamp int64)
	onSessionOpen(leadershipTermId, logPosition, clusterSessionId, timestamp int64,
		responseStreamId int32, responseChannel string, encodedPrincipal []byte)
	onSessionClose(leadershipTermId, logPosition, clusterSessionId, timestamp int64,
		closeReason codecs.CloseReasonEnum)
	onServiceAction(leadershipTermId, logPosition, timestamp int64, action codecs.ClusterActionEnum, flags int32)
	onNewLeadershipTermEvent(leadershipTermId, logPosition, timestamp, termBaseLogPosition int64,
		leaderMemberId, logSessionId int32, timeUnit codecs.ClusterTimeUnitEnum, appVersion int32)
	onSessionMessage(logPosition, clusterSessionId, timestamp int64, buffer *atomic.Buffer,
		offset, length int32, header *logbuffer.Header)
}

// boundedLogAdapter mirrors io.aeron.cluster.service.BoundedLogAdapter: it consumes the replicated log up to the
// commit position, reassembling fragmented messages.
type boundedLogAdapter struct {
	marshaller     *codecs.SbeGoMarshaller
	options        *Options
	handler        logAdapterHandler
	image          aeron.Image
	builder        *bytes.Buffer
	nextTermOffset int32
	maxLogPosition int64
}

func newBoundedLogAdapter(options *Options, handler logAdapterHandler) *boundedLogAdapter {
	return &boundedLogAdapter{
		marshaller: codecs.NewSbeGoMarshaller(),
		options:    options,
		handler:    handler,
		builder:    &bytes.Buffer{},
	}
}

func (adapter *boundedLogAdapter) isDone() bool {
	return adapter.image.Position() >= adapter.maxLogPosition ||
		adapter.image.IsEndOfStream() ||
		adapter.image.IsClosed()
}

func (adapter *boundedLogAdapter) poll(limitPos int64) int {
	return adapter.image.BoundedPoll(adapter.onFragment, limitPos, adapter.options.LogFragmentLimit)
}

func (adapter *boundedLogAdapter) onFragment(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) {
	flags := header.Flags()
	if (flags & unfragmented) == unfragmented {
		adapter.onMessage(buffer, offset, length, header)
		return
	}
	alignedFrameLength := util.AlignInt32(header.FrameLength(), logbuffer.FrameAlignment)
	if (flags & beginFrag) == beginFrag {
		adapter.builder.Reset()
		buffer.WriteBytes(adapter.builder, offset, length)
		adapter.nextTermOffset = header.Offset() + alignedFrameLength
		return
	}
	if adapter.builder.Len() == 0 {
		return
	}
	if header.Offset() != adapter.nextTermOffset {
		// gap in the fragment sequence - discard the partial message
		adapter.builder.Reset()
		return
	}
	buffer.WriteBytes(adapter.builder, offset, length)
	adapter.nextTermOffset = header.Offset() + alignedFrameLength
	if (flags & endFrag) == endFrag {
		msgLength := int32(adapter.builder.Len())
		adapter.onMessage(atomic.MakeBuffer(adapter.builder.Bytes(), msgLength), 0, msgLength, header)
		adapter.builder.Reset()
	}
}

func (adapter *boundedLogAdapter) onMessage(
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
	case SessionMessageHeaderTemplateId:
		if length < SessionMessageHeaderLength {
			logger.Errorf("received invalid session message - length: %d", length)
			return
		}
		clusterSessionId := buffer.GetInt64(offset + 8)
		timestamp := buffer.GetInt64(offset + 16)
		adapter.handler.onSessionMessage(
			header.Position(),
			clusterSessionId,
			timestamp,
			buffer,
			offset+SessionMessageHeaderLength,
			length-SessionMessageHeaderLength,
			header,
		)
	case timerEventTemplateId:
		correlationId := buffer.GetInt64(offset + 8)
		timestamp := buffer.GetInt64(offset + 16)
		adapter.handler.onTimerEvent(header.Position(), correlationId, timestamp)
	case sessionOpenTemplateId:
		event := &codecs.SessionOpenEvent{}
		if err := event.Decode(adapter.marshaller, toByteBuffer(buffer, offset, length), version, blockLength,
			adapter.options.RangeChecking); err != nil {
			panic(newClusterError("session open decode error: %v", err))
		}
		adapter.handler.onSessionOpen(
			event.LeadershipTermId,
			header.Position(),
			event.ClusterSessionId,
			event.Timestamp,
			event.ResponseStreamId,
			string(event.ResponseChannel),
			event.EncodedPrincipal,
		)
	case sessionCloseTemplateId:
		leadershipTermId := buffer.GetInt64(offset)
		clusterSessionId := buffer.GetInt64(offset + 8)
		timestamp := buffer.GetInt64(offset + 16)
		closeReason := codecs.CloseReasonEnum(buffer.GetInt32(offset + 24))
		adapter.handler.onSessionClose(leadershipTermId, header.Position(), clusterSessionId, timestamp, closeReason)
	case clusterActionReqTemplateId:
		e := codecs.ClusterActionRequest{}
		if err := e.Decode(adapter.marshaller, toByteBuffer(buffer, offset, length), version, blockLength,
			adapter.options.RangeChecking); err != nil {
			panic(newClusterError("cluster action request decode error: %v", err))
		}
		flags := e.Flags
		if flags == e.FlagsNullValue() {
			flags = ClusterActionFlagsDefault
		}
		adapter.handler.onServiceAction(e.LeadershipTermId, e.LogPosition, e.Timestamp, e.Action, flags)
	case newLeadershipTermTemplateId:
		e := codecs.NewLeadershipTermEvent{}
		if err := e.Decode(adapter.marshaller, toByteBuffer(buffer, offset, length), version, blockLength,
			adapter.options.RangeChecking); err != nil {
			panic(newClusterError("new leadership term decode error: %v", err))
		}
		adapter.handler.onNewLeadershipTermEvent(e.LeadershipTermId, e.LogPosition, e.Timestamp,
			e.TermBaseLogPosition, e.LeaderMemberId, e.LogSessionId, e.TimeUnit, e.AppVersion)
	case membershipChangeTemplateId:
		// Dynamic membership was removed from the consensus module; Java's BoundedLogAdapter ignores the event.
	default:
		logger.Debugf("boundedLogAdapter: unexpected templateId=%d at pos=%d", templateId, header.Position())
	}
}

func toByteBuffer(buffer *atomic.Buffer, offset int32, length int32) *bytes.Buffer {
	buf := &bytes.Buffer{}
	buffer.WriteBytes(buf, offset, length)
	return buf
}

func (adapter *boundedLogAdapter) Close() error {
	var err error
	if adapter.image != nil {
		err = adapter.image.Close()
		adapter.image = nil
	}
	return err
}
