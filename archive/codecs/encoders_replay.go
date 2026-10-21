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
	"io"
)

// NullValue is the Aeron NULL_VALUE sentinel used for optional int32/int64 request fields.
const NullValue = -1

type sbeEncodable interface {
	SbeBlockLength() uint16
	SbeTemplateId() uint16
	SbeSchemaId() uint16
	SbeSchemaVersion() uint16
	Encode(*SbeGoMarshaller, io.Writer, bool) error
}

func encodePacket(marshaller *SbeGoMarshaller, rangeChecking bool, request sbeEncodable) ([]byte, error) {
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

// ReplayRequestWithParamsPacket encodes a ReplayRequest including the fileIoMaxLength and replayToken
// fields. Use NullValue for either to request the archive default.
func ReplayRequestWithParamsPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64, position int64, length int64, replayStream int32, fileIoMaxLength int32, replayToken int64, replayChannel string) ([]byte, error) {
	request := &ReplayRequest{
		ControlSessionId: controlSessionId,
		CorrelationId:    correlationId,
		RecordingId:      recordingId,
		Position:         position,
		Length:           length,
		ReplayStreamId:   replayStream,
		FileIoMaxLength:  fileIoMaxLength,
		ReplayToken:      replayToken,
		ReplayChannel:    []uint8(replayChannel),
	}
	return encodePacket(marshaller, rangeChecking, request)
}

// BoundedReplayWithParamsPacket encodes a BoundedReplayRequest including the fileIoMaxLength and replayToken
// fields. Use NullValue for either to request the archive default.
func BoundedReplayWithParamsPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64, position int64, length int64, limitCounterId int32, replayStream int32, fileIoMaxLength int32, replayToken int64, replayChannel string) ([]byte, error) {
	request := &BoundedReplayRequest{
		ControlSessionId: controlSessionId,
		CorrelationId:    correlationId,
		RecordingId:      recordingId,
		Position:         position,
		Length:           length,
		LimitCounterId:   limitCounterId,
		ReplayStreamId:   replayStream,
		FileIoMaxLength:  fileIoMaxLength,
		ReplayToken:      replayToken,
		ReplayChannel:    []uint8(replayChannel),
	}
	return encodePacket(marshaller, rangeChecking, request)
}

// ReplayTokenRequestPacket encodes a request for a replay token used for replays over response channels.
func ReplayTokenRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64) ([]byte, error) {
	request := &ReplayTokenRequest{
		ControlSessionId: controlSessionId,
		CorrelationId:    correlationId,
		RecordingId:      recordingId,
	}
	return encodePacket(marshaller, rangeChecking, request)
}

// MaxRecordedPositionRequestPacket encodes a request for the max recorded position of a recording.
func MaxRecordedPositionRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64, recordingId int64) ([]byte, error) {
	request := &MaxRecordedPositionRequest{
		ControlSessionId: controlSessionId,
		CorrelationId:    correlationId,
		RecordingId:      recordingId,
	}
	return encodePacket(marshaller, rangeChecking, request)
}

// ArchiveIdRequestPacket encodes a request for the id of the archive.
func ArchiveIdRequestPacket(marshaller *SbeGoMarshaller, rangeChecking bool, controlSessionId int64, correlationId int64) ([]byte, error) {
	request := &ArchiveIdRequest{
		ControlSessionId: controlSessionId,
		CorrelationId:    correlationId,
	}
	return encodePacket(marshaller, rangeChecking, request)
}
