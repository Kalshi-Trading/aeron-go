// Copyright (C) 2021-2022 Talos, Inc.
//
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

package archive

import (
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
)

// Proxy class for encapsulating encoding and sending of control protocol messages to an archive
type Proxy struct {
	Publication aeron.Publisher
	archive     *Archive                // link to parent
	marshaller  *codecs.SbeGoMarshaller // currently shared as we're not reentrant (but could be here)
}

// Offer to our request publication with a retry to allow time for the image establishment, some back pressure etc
func (proxy *Proxy) Offer(buffer *atomic.Buffer, offset int32, length int32, reservedValueSupplier term.ReservedValueSupplier) int64 {
	start := time.Now()
	var ret int64
	for time.Since(start) < proxy.archive.Options.Timeout {
		ret = proxy.Publication.Offer(buffer, offset, length, reservedValueSupplier)
		switch ret {
		// Retry on these
		case aeron.NotConnected, aeron.BackPressured, aeron.AdminAction:
			proxy.archive.Options.IdleStrategy.Idle(0)

		// Fail or succeed on other values
		default:
			return ret
		}
	}

	// Give up, returning the last failure
	logger.Debugf("Proxy.Offer timing out [%d]", ret)
	return ret

}

// From here we have all the functions that create a data packet and send it on the
// publication. Responses will be processed on the control

// CloseSessionRequest packet and offer
func (proxy *Proxy) CloseSessionRequest() error {
	bytes, err := codecs.CloseSessionRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID)
	if err != nil {
		return err
	}
	return proxy.offerBytes(bytes)
}

// StartRecordingRequest packet and offer
// Uses the more recent protocol addition StartdRecordingRequest2 which added autoStop
func (proxy *Proxy) StartRecordingRequest(correlationID int64, stream int32, isLocal bool, autoStop bool, channel string) error {
	return proxy.offerPacket(codecs.StartRecordingRequest2Packet(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, stream, isLocal, autoStop, channel))
}

// StopRecordingRequest packet and offer
func (proxy *Proxy) StopRecordingRequest(correlationID int64, stream int32, channel string) error {
	return proxy.offerPacket(codecs.StopRecordingRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, stream, channel))
}

// ReplayRequest packet and offer
func (proxy *Proxy) ReplayRequest(correlationID int64, recordingID int64, position int64, length int64, replayChannel string, replayStream int32) error {
	return proxy.offerPacket(codecs.ReplayRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, recordingID, position, length, replayStream, replayChannel))
}

// StopReplayRequest packet and offer
func (proxy *Proxy) StopReplayRequest(correlationID int64, replaySessionID int64) error {
	return proxy.offerPacket(codecs.StopReplayRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, replaySessionID))
}

// ListRecordingsRequest packet and offer
// Lists up to recordCount recordings starting at fromRecordingID
func (proxy *Proxy) ListRecordingsRequest(correlationID int64, fromRecordingID int64, recordCount int32) error {
	return proxy.offerPacket(codecs.ListRecordingsRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, fromRecordingID, recordCount))
}

// ListRecordingsForUriRequest packet and offer
// Lists up to recordCount recordings that match the channel and stream
func (proxy *Proxy) ListRecordingsForUriRequest(correlationID int64, fromRecordingID int64, recordCount int32, stream int32, channel string) error {
	return proxy.offerPacket(codecs.ListRecordingsForUriRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, fromRecordingID, recordCount, stream, channel))
}

// ListRecordingRequest packet and offer
// Retrieves a recording descriptor for a specific recordingID
func (proxy *Proxy) ListRecordingRequest(correlationID int64, fromRecordingID int64) error {
	return proxy.offerPacket(codecs.ListRecordingRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, fromRecordingID))
}

// ExtendRecordingRequest packet and offer
// Uses the more recent protocol addition ExtendRecordingRequest2 which added autoStop
func (proxy *Proxy) ExtendRecordingRequest(correlationID int64, recordingID int64, stream int32, sourceLocation codecs.SourceLocationEnum, autoStop bool, channel string) error {
	return proxy.offerPacket(codecs.ExtendRecordingRequest2Packet(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, recordingID, stream, sourceLocation, autoStop, channel))
}

// RecordingPositionRequest packet and offer
func (proxy *Proxy) RecordingPositionRequest(correlationID int64, recordingID int64) error {
	return proxy.offerPacket(codecs.RecordingPositionRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, recordingID))
}

// TruncateRecordingRequest packet and offer
func (proxy *Proxy) TruncateRecordingRequest(correlationID int64, recordingID int64, position int64) error {
	return proxy.offerPacket(codecs.TruncateRecordingRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, recordingID, position))
}

// StopRecordingSubscriptionRequest packet and offer
func (proxy *Proxy) StopRecordingSubscriptionRequest(correlationID int64, subscriptionID int64) error {
	return proxy.offerPacket(codecs.StopRecordingSubscriptionPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, subscriptionID))
}

// StopRecordingByIdentityRequest packet and offer
func (proxy *Proxy) StopRecordingByIdentityRequest(correlationID int64, recordingID int64) error {
	return proxy.offerPacket(codecs.StopRecordingByIdentityPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, recordingID))
}

// StopPositionRequest packet and offer
func (proxy *Proxy) StopPositionRequest(correlationID int64, recordingID int64) error {
	return proxy.offerPacket(codecs.StopPositionPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, recordingID))
}

// FindLastMatchingRecordingRequest packet and offer
func (proxy *Proxy) FindLastMatchingRecordingRequest(correlationID int64, minRecordingID int64, sessionID int32, stream int32, channel string) error {
	return proxy.offerPacket(codecs.FindLastMatchingRecordingPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, minRecordingID, sessionID, stream, channel))
}

// ListRecordingSubscriptionsRequest packet and offer
func (proxy *Proxy) ListRecordingSubscriptionsRequest(correlationID int64, pseudoIndex int32, subscriptionCount int32, applyStreamID bool, stream int32, channel string) error {
	return proxy.offerPacket(codecs.ListRecordingSubscriptionsPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, pseudoIndex, subscriptionCount, applyStreamID, stream, channel))
}

// BoundedReplayRequest packet and offer
func (proxy *Proxy) BoundedReplayRequest(correlationID int64, recordingID int64, position int64, length int64, limitCounterID int32, replayStream int32, replayChannel string) error {
	return proxy.offerPacket(codecs.BoundedReplayPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, recordingID, position, length, limitCounterID, replayStream, replayChannel))
}

// StopAllReplaysRequest packet and offer
func (proxy *Proxy) StopAllReplaysRequest(correlationID int64, recordingID int64) error {
	return proxy.offerPacket(codecs.StopAllReplaysPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, recordingID))
}

// ReplicateRequest packet and offer
func (proxy *Proxy) ReplicateRequest(correlationID int64, srcRecordingID int64, dstRecordingID int64, srcControlStreamID int32, srcControlChannel string, liveDestination string) error {
	// Java 1.53.3 sends every replicate variant as ReplicateRequest2.
	params := NewReplicationParams()
	params.DstRecordingID = dstRecordingID
	params.LiveDestination = liveDestination
	return proxy.offerPacket(codecs.ReplicateRequest2FullPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, params.toCodec(srcRecordingID, srcControlStreamID, srcControlChannel)))
}

// ReplicateRequest2 packet and offer
func (proxy *Proxy) ReplicateRequest2(correlationID int64, srcRecordingID int64, dstRecordingID int64, stopPosition int64, channelTagID int64, srcControlStreamID int32, srcControlChannel string, liveDestination string, replicationChannel string) error {
	return proxy.offerPacket(codecs.ReplicateRequest2Packet(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, srcRecordingID, dstRecordingID, stopPosition, channelTagID, srcControlStreamID, srcControlChannel, liveDestination, replicationChannel))
}

// StopReplicationRequest packet and offer
func (proxy *Proxy) StopReplicationRequest(correlationID int64, replicationID int64) error {
	return proxy.offerPacket(codecs.StopReplicationRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, replicationID))
}

// StartPositionRequest packet and offer
func (proxy *Proxy) StartPositionRequest(correlationID int64, recordingID int64) error {
	return proxy.offerPacket(codecs.StartPositionRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, recordingID))
}

// DetachSegmentsRequest packet and offer
func (proxy *Proxy) DetachSegmentsRequest(correlationID int64, recordingID int64, newStartPosition int64) error {
	return proxy.offerPacket(codecs.DetachSegmentsRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, recordingID, newStartPosition))
}

// DeleteDetachedSegmentsRequest packet and offer
func (proxy *Proxy) DeleteDetachedSegmentsRequest(correlationID int64, recordingID int64) error {
	return proxy.offerPacket(codecs.DeleteDetachedSegmentsRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, recordingID))
}

// PurgeSegmentsRequest packet and offer
func (proxy *Proxy) PurgeSegmentsRequest(correlationID int64, recordingID int64, newStartPosition int64) error {
	return proxy.offerPacket(codecs.PurgeSegmentsRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, recordingID, newStartPosition))
}

// AttachSegmentsRequest packet and offer
func (proxy *Proxy) AttachSegmentsRequest(correlationID int64, recordingID int64) error {
	return proxy.offerPacket(codecs.AttachSegmentsRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, recordingID))
}

// MigrateSegmentsRequest packet and offer
func (proxy *Proxy) MigrateSegmentsRequest(correlationID int64, srcRecordingID int64, destRecordingID int64) error {
	return proxy.offerPacket(codecs.MigrateSegmentsRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, srcRecordingID, destRecordingID))
}

// AuthConnectRequest packet and offer, with empty client info. Normally sent by AsyncConnect.
func (proxy *Proxy) AuthConnectRequest(correlationID int64, responseStream int32, responseChannel string, encodedCredentials []uint8) error {
	return proxy.AuthConnectRequestWithClientInfo(correlationID, responseStream, responseChannel, encodedCredentials, "")
}

// AuthConnectRequestWithClientInfo is AuthConnectRequest carrying the client info string of the 1.53.3 schema.
func (proxy *Proxy) AuthConnectRequestWithClientInfo(correlationID int64, responseStream int32, responseChannel string, encodedCredentials []uint8, clientInfo string) error {
	return proxy.offerPacket(codecs.AuthConnectRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, correlationID, responseStream, responseChannel, encodedCredentials, clientInfo))
}

// ChallengeResponse packet and offer
func (proxy *Proxy) ChallengeResponse(correlationID int64, encodedCredentials []uint8) error {
	return proxy.offerPacket(codecs.ChallengeResponsePacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, encodedCredentials))
}

// KeepAliveRequest packet and offer
func (proxy *Proxy) KeepAliveRequest(correlationID int64) error {
	return proxy.offerPacket(codecs.KeepAliveRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID))
}

// TaggedReplicateRequest packet and offer
func (proxy *Proxy) TaggedReplicateRequest(correlationID int64, srcRecordingID int64, dstRecordingID int64, channelTagID int64, subscriptionTagID int64, srcControlStreamID int32, srcControlChannel string, liveDestination string) error {
	// Java 1.53.3 sends every replicate variant as ReplicateRequest2.
	params := NewReplicationParams()
	params.DstRecordingID = dstRecordingID
	params.ChannelTagID = channelTagID
	params.SubscriptionTagID = subscriptionTagID
	params.LiveDestination = liveDestination
	return proxy.offerPacket(codecs.ReplicateRequest2FullPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, params.toCodec(srcRecordingID, srcControlStreamID, srcControlChannel)))
}

// PurgeRecordingRequest packet and offer
func (proxy *Proxy) PurgeRecordingRequest(correlationID int64, replaySessionID int64) error {
	return proxy.offerPacket(codecs.PurgeRecordingRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID, replaySessionID))
}
