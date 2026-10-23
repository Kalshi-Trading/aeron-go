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

// Package codecs contains the archive protocol packet encoding and decoding
package codecs

func CloseSessionRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64) ([]byte, error) {
	var request CloseSessionRequest

	request.ControlSessionId = controlSessionId

	return encodePacket(marshaller, rangeChecking, &request)
}

// deprecated
func StartRecordingRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, stream int32, isLocal bool, channel string) ([]byte, error) {
	return StartRecordingRequest2Packet(marshaller, rangeChecking, controlSessionId, correlationId, stream, isLocal, false, channel)
}

func StartRecordingRequest2Packet(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, stream int32, isLocal bool, autoStop bool, channel string) ([]byte, error) {
	var request StartRecordingRequest2

	request.Channel = []uint8(channel)
	request.StreamId = stream
	if isLocal {
		request.SourceLocation = SourceLocation.LOCAL
	} else {
		request.SourceLocation = SourceLocation.REMOTE
	}
	if autoStop {
		request.AutoStop = BooleanType.TRUE
	} // else FALSE by default
	request.CorrelationId = correlationId
	request.ControlSessionId = controlSessionId

	return encodePacket(marshaller, rangeChecking, &request)
}

func StopRecordingRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, stream int32, channel string) ([]byte, error) {
	var request StopRecordingRequest

	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.StreamId = stream
	request.Channel = []uint8(channel)

	return encodePacket(marshaller, rangeChecking, &request)
}

func ReplayRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64, position int64, length int64, replayStream int32, replayChannel string) ([]byte, error) {
	return ReplayRequestWithParamsPacket(marshaller, rangeChecking, controlSessionId, correlationId, recordingId, position, length, replayStream, NullValue, NullValue, replayChannel)
}

func StopReplayRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, replaySessionId int64) ([]byte, error) {
	var request StopReplayRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.ReplaySessionId = replaySessionId

	return encodePacket(marshaller, rangeChecking, &request)
}

func ListRecordingsRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, fromRecordingId int64, recordCount int32) ([]byte, error) {
	var request ListRecordingsRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.FromRecordingId = fromRecordingId
	request.RecordCount = recordCount

	return encodePacket(marshaller, rangeChecking, &request)
}

func ListRecordingsForUriRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, fromRecordingId int64, recordCount int32, stream int32, channel string) ([]byte, error) {
	var request ListRecordingsForUriRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.FromRecordingId = fromRecordingId
	request.RecordCount = recordCount
	request.StreamId = stream
	request.Channel = []uint8(channel)

	return encodePacket(marshaller, rangeChecking, &request)
}

func ListRecordingRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64) ([]byte, error) {
	var request ListRecordingRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId

	return encodePacket(marshaller, rangeChecking, &request)
}

// deprecated
func ExtendRecordingRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64, stream int32, sourceLocation SourceLocationEnum, channel string) ([]byte, error) {
	return ExtendRecordingRequest2Packet(marshaller, rangeChecking, controlSessionId, correlationId, recordingId, stream, sourceLocation, false, channel)
}

func ExtendRecordingRequest2Packet(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64, stream int32, sourceLocation SourceLocationEnum, autoStop bool, channel string) ([]byte, error) {
	var request ExtendRecordingRequest2
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId
	request.StreamId = stream
	request.SourceLocation = sourceLocation
	if autoStop {
		request.AutoStop = BooleanType.TRUE
	} // else FALSE by default
	request.Channel = []uint8(channel)

	return encodePacket(marshaller, rangeChecking, &request)
}

func RecordingPositionRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64) ([]byte, error) {
	var request RecordingPositionRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId

	return encodePacket(marshaller, rangeChecking, &request)
}

func TruncateRecordingRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64, position int64) ([]byte, error) {
	var request TruncateRecordingRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId
	request.Position = position

	return encodePacket(marshaller, rangeChecking, &request)
}

func StopRecordingSubscriptionPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, subscriptionId int64) ([]byte, error) {
	var request StopRecordingSubscriptionRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.SubscriptionId = subscriptionId

	return encodePacket(marshaller, rangeChecking, &request)
}

func StopRecordingByIdentityPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64) ([]byte, error) {
	var request StopRecordingByIdentityRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId

	return encodePacket(marshaller, rangeChecking, &request)
}

func StopPositionPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64) ([]byte, error) {
	var request StopPositionRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId

	return encodePacket(marshaller, rangeChecking, &request)
}

func FindLastMatchingRecordingPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, minRecordingId int64, sessionId int32, stream int32, channel string) ([]byte, error) {
	var request FindLastMatchingRecordingRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.MinRecordingId = minRecordingId
	request.SessionId = sessionId
	request.StreamId = stream
	request.Channel = []uint8(channel)

	return encodePacket(marshaller, rangeChecking, &request)
}

func ListRecordingSubscriptionsPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, pseudoIndex int32, subscriptionCount int32, applyStreamId bool, stream int32, channel string) ([]byte, error) {
	var request ListRecordingSubscriptionsRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.PseudoIndex = pseudoIndex
	request.SubscriptionCount = subscriptionCount
	if applyStreamId {
		request.ApplyStreamId = BooleanType.TRUE
	} // else FALSE by default
	request.StreamId = stream
	request.Channel = []uint8(channel)

	return encodePacket(marshaller, rangeChecking, &request)
}

func BoundedReplayPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64, position int64, length int64, limitCounterId int32, replayStream int32, replayChannel string) ([]byte, error) {
	return BoundedReplayWithParamsPacket(marshaller, rangeChecking, controlSessionId, correlationId, recordingId, position, length, limitCounterId, replayStream, NullValue, NullValue, replayChannel)
}

func StopAllReplaysPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64) ([]byte, error) {
	var request StopAllReplaysRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId

	return encodePacket(marshaller, rangeChecking, &request)
}

func CatalogHeaderPacket(marshaller *SbeGoMarshaller, rangeChecking bool, version int32, length int32, nextRecordingId int64, alignment int32) ([]byte, error) {
	var request CatalogHeader
	request.Version = version
	request.Length = length
	request.NextRecordingId = nextRecordingId
	request.Alignment = alignment

	return encodePacket(marshaller, rangeChecking, &request)
}

func ReplicateRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, srcRecordingId int64, dstRecordingId int64, srcControlStreamId int32, srcControlChannel string, liveDestination string) ([]byte, error) {
	var request ReplicateRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.SrcRecordingId = srcRecordingId
	request.DstRecordingId = dstRecordingId
	request.SrcControlStreamId = srcControlStreamId
	request.SrcControlChannel = []uint8(srcControlChannel)
	request.LiveDestination = []uint8(liveDestination)

	return encodePacket(marshaller, rangeChecking, &request)
}

func StopReplicationRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, replicationId int64) ([]byte, error) {
	var request StopReplicationRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.ReplicationId = replicationId

	return encodePacket(marshaller, rangeChecking, &request)
}

func StartPositionRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64) ([]byte, error) {
	var request StartPositionRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId

	return encodePacket(marshaller, rangeChecking, &request)
}

func DetachSegmentsRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64, newStartPosition int64) ([]byte, error) {
	var request DetachSegmentsRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId
	request.NewStartPosition = newStartPosition

	return encodePacket(marshaller, rangeChecking, &request)
}

func DeleteDetachedSegmentsRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64) ([]byte, error) {
	var request DeleteDetachedSegmentsRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId

	return encodePacket(marshaller, rangeChecking, &request)
}

func PurgeSegmentsRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64, newStartPosition int64) ([]byte, error) {
	var request PurgeSegmentsRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId
	request.NewStartPosition = newStartPosition

	return encodePacket(marshaller, rangeChecking, &request)
}

func AttachSegmentsRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64) ([]byte, error) {
	var request AttachSegmentsRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId

	return encodePacket(marshaller, rangeChecking, &request)
}

func AuthConnectRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, correlationID int64, responseStream int32, responseChannel string, encodedCredentials []uint8, clientInfo string) ([]byte, error) {
	var request AuthConnectRequest

	request.CorrelationId = correlationID
	request.Version = SemanticVersion()
	request.ResponseStreamId = responseStream
	request.ResponseChannel = []uint8(responseChannel)
	request.EncodedCredentials = encodedCredentials
	request.ClientInfo = []uint8(clientInfo)

	return encodePacket(marshaller, rangeChecking, &request)
}

func ChallengeResponsePacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, encodedCredentials []uint8) ([]byte, error) {
	var request ChallengeResponse
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.EncodedCredentials = encodedCredentials

	return encodePacket(marshaller, rangeChecking, &request)
}

func MigrateSegmentsRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, srcRecordingId int64, destRecordingId int64) ([]byte, error) {
	var request MigrateSegmentsRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.SrcRecordingId = srcRecordingId
	request.DstRecordingId = destRecordingId

	return encodePacket(marshaller, rangeChecking, &request)
}

func KeepAliveRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64) ([]byte, error) {
	var request KeepAliveRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId

	return encodePacket(marshaller, rangeChecking, &request)
}

func TaggedReplicateRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, srcRecordingId int64, dstRecordingId int64, channelTagId int64, subscriptionTagId int64, srcControlStreamId int32, srcControlChannel string, liveDestination string) ([]byte, error) {
	var request TaggedReplicateRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.SrcRecordingId = srcRecordingId
	request.DstRecordingId = dstRecordingId
	request.ChannelTagId = channelTagId
	request.SubscriptionTagId = subscriptionTagId
	request.SrcControlStreamId = srcControlStreamId
	request.SrcControlChannel = []uint8(srcControlChannel)
	request.LiveDestination = []uint8(liveDestination)

	return encodePacket(marshaller, rangeChecking, &request)
}

func ReplicateRequest2Packet(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, srcRecordingId int64, dstRecordingId int64, stopPosition int64, channelTagId int64, srcControlStreamId int32, srcControlChannel string, liveDestination string, replicationChannel string) ([]byte, error) {
	// Unset optional fields must carry the Aeron NULL_VALUE (-1), not zero: a zero
	// replicationSessionId or subscriptionTagId is a valid value to the archive.
	return ReplicateRequest2FullPacket(marshaller, rangeChecking, controlSessionId, correlationId, &ReplicateRequest2Params{
		SrcRecordingId:       srcRecordingId,
		DstRecordingId:       dstRecordingId,
		StopPosition:         stopPosition,
		ChannelTagId:         channelTagId,
		SubscriptionTagId:    -1,
		SrcControlStreamId:   srcControlStreamId,
		FileIoMaxLength:      -1,
		ReplicationSessionId: -1,
		SrcControlChannel:    srcControlChannel,
		LiveDestination:      liveDestination,
		ReplicationChannel:   replicationChannel,
	})
}

func PurgeRecordingRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64) ([]byte, error) {
	var request PurgeRecordingRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId

	return encodePacket(marshaller, rangeChecking, &request)
}

func ArchiveIdRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64) ([]byte, error) {
	var request ArchiveIdRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId

	return encodePacket(marshaller, rangeChecking, &request)
}
