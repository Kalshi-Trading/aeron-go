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

package codecs

import (
	"bytes"
)

// ReplicateRequest2Params holds every field of the ReplicateRequest2 message (schema v14). Java's
// ArchiveProxy sends all replicate and taggedReplicate variants through this message.
type ReplicateRequest2Params struct {
	SrcRecordingId       int64
	DstRecordingId       int64
	StopPosition         int64
	ChannelTagId         int64
	SubscriptionTagId    int64
	SrcControlStreamId   int32
	FileIoMaxLength      int32
	ReplicationSessionId int32
	SrcControlChannel    string
	LiveDestination      string
	ReplicationChannel   string
	EncodedCredentials   []byte
	SrcResponseChannel   string
}

// ReplicateRequest2FullPacket encodes a ReplicateRequest2 with all fields populated from params.
func ReplicateRequest2FullPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, params *ReplicateRequest2Params) ([]byte, error) {
	var request ReplicateRequest2
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.SrcRecordingId = params.SrcRecordingId
	request.DstRecordingId = params.DstRecordingId
	request.StopPosition = params.StopPosition
	request.ChannelTagId = params.ChannelTagId
	request.SubscriptionTagId = params.SubscriptionTagId
	request.SrcControlStreamId = params.SrcControlStreamId
	request.FileIoMaxLength = params.FileIoMaxLength
	request.ReplicationSessionId = params.ReplicationSessionId
	request.SrcControlChannel = []uint8(params.SrcControlChannel)
	request.LiveDestination = []uint8(params.LiveDestination)
	request.ReplicationChannel = []uint8(params.ReplicationChannel)
	request.EncodedCredentials = params.EncodedCredentials
	request.SrcResponseChannel = []uint8(params.SrcResponseChannel)

	header := MessageHeader{BlockLength: request.SbeBlockLength(), TemplateId: request.SbeTemplateId(), SchemaId: request.SbeSchemaId(), Version: request.SbeSchemaVersion()}
	buffer := new(bytes.Buffer)
	if err := header.Encode(marshaller, buffer); err != nil {
		return nil, err
	}
	if err := request.Encode(marshaller, buffer, rangeChecking); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// MaxRecordedPositionRequestPacket encodes a MaxRecordedPositionRequest.
func MaxRecordedPositionRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64) ([]byte, error) {
	var request MaxRecordedPositionRequest
	request.ControlSessionId = controlSessionId
	request.CorrelationId = correlationId
	request.RecordingId = recordingId

	header := MessageHeader{BlockLength: request.SbeBlockLength(), TemplateId: request.SbeTemplateId(), SchemaId: request.SbeSchemaId(), Version: request.SbeSchemaVersion()}
	buffer := new(bytes.Buffer)
	if err := header.Encode(marshaller, buffer); err != nil {
		return nil, err
	}
	if err := request.Encode(marshaller, buffer, rangeChecking); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
