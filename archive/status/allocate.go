// Copyright 2026 Kalshi Trading
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

package status

import (
	"fmt"
	"strconv"
	"unicode/utf16"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
	"github.com/lirm/aeron-go/aeron/util"
)

// TempBufferLength is large enough for the key and label of any archive counter.
const TempBufferLength = counters.MaxKeyLength + counters.FullLabelLength

// EncodeRecordingPos writes the key and label of a RecordingPos counter into tempBuffer exactly as Java
// RecordingPos.allocate does: the source identity is truncated to MaxSourceIdentityLength, the label is
// "rec-pos: <recordingId> <sessionId> <streamId> <strippedChannel> - archiveId=<archiveId>" with the channel
// truncated to fit MaxLabelLength, and the label starts at the key length aligned to 4.
func EncodeRecordingPos(tempBuffer *atomic.Buffer, archiveId int64, recordingId int64, sessionId int32,
	streamId int32, strippedChannel string, sourceIdentity string) (keyLength, labelOffset, labelLength int32, err error) {
	if tempBuffer == nil || tempBuffer.Capacity() < TempBufferLength {
		return 0, 0, 0, fmt.Errorf("tempBuffer capacity must be at least %d", TempBufferLength)
	}
	tempBuffer.PutInt64(RecordingIdOffset, recordingId)
	tempBuffer.PutInt32(SessionIdOffset, sessionId)

	sourceIdentityLength := putASCII(tempBuffer, SourceIdentityOffset, sourceIdentity, MaxSourceIdentityLength)
	tempBuffer.PutInt32(SourceIdentityLengthOffset, sourceIdentityLength)
	archiveIdOffset := SourceIdentityOffset + sourceIdentityLength
	tempBuffer.PutInt64(archiveIdOffset, archiveId)
	keyLength = archiveIdOffset + util.SizeOfInt64

	labelOffset = util.AlignInt32(keyLength, util.SizeOfInt32)
	prefix := RecordingPosName + ": " + strconv.FormatInt(recordingId, 10) + " " +
		strconv.FormatInt(int64(sessionId), 10) + " " + strconv.FormatInt(int64(streamId), 10) + " "
	labelLength = putASCII(tempBuffer, labelOffset, prefix, counters.MaxLabelLength)
	labelLength += putASCII(tempBuffer, labelOffset+labelLength, strippedChannel,
		counters.MaxLabelLength-labelLength-int32(LengthOfArchiveIdLabel(archiveId)))
	labelLength += putASCII(tempBuffer, labelOffset+labelLength, ArchiveIdLabel(archiveId), counters.MaxLabelLength)
	return keyLength, labelOffset, labelLength, nil
}

// AllocateRecordingPos allocates a RecordingPos counter (Java RecordingPos.allocate). tempBuffer may be nil, in
// which case one of TempBufferLength bytes is allocated.
func AllocateRecordingPos(a *aeron.Aeron, tempBuffer *atomic.Buffer, archiveId int64, recordingId int64,
	sessionId int32, streamId int32, strippedChannel string, sourceIdentity string) (*aeron.Counter, error) {
	if tempBuffer == nil {
		tempBuffer = atomic.MakeBuffer(make([]byte, TempBufferLength))
	}
	keyLength, labelOffset, labelLength, err := EncodeRecordingPos(
		tempBuffer, archiveId, recordingId, sessionId, streamId, strippedChannel, sourceIdentity)
	if err != nil {
		return nil, err
	}
	return a.AddCounter(RecordingPositionTypeId, tempBuffer, 0, keyLength, tempBuffer, labelOffset, labelLength)
}

// EncodeArchiveCounter writes the key (archiveId) and label ("<name> - archiveId=<archiveId>") of a per-archive
// counter into tempBuffer as Java ArchiveCounters.allocate does. The label follows the key at ArchiveCounterKeyLength.
// Unlike Java, which fails in the driver, a label longer than MaxLabelLength is truncated, keeping the archive id
// suffix.
func EncodeArchiveCounter(tempBuffer *atomic.Buffer, name string, archiveId int64) (keyLength, labelLength int32,
	err error) {
	if tempBuffer == nil || tempBuffer.Capacity() < TempBufferLength {
		return 0, 0, fmt.Errorf("tempBuffer capacity must be at least %d", TempBufferLength)
	}
	tempBuffer.PutInt64(ArchiveIdKeyOffset, archiveId)
	keyLength = ArchiveCounterKeyLength
	labelLength = putASCII(tempBuffer, keyLength, name, counters.MaxLabelLength-int32(LengthOfArchiveIdLabel(archiveId)))
	labelLength += putASCII(tempBuffer, keyLength+labelLength, ArchiveIdLabel(archiveId), counters.MaxLabelLength)
	return keyLength, labelLength, nil
}

// AllocateArchiveCounter allocates a per-archive counter of typeId (Java ArchiveCounters.allocate). tempBuffer may
// be nil, in which case one of TempBufferLength bytes is allocated.
func AllocateArchiveCounter(a *aeron.Aeron, tempBuffer *atomic.Buffer, typeId int32, name string,
	archiveId int64) (*aeron.Counter, error) {
	if tempBuffer == nil {
		tempBuffer = atomic.MakeBuffer(make([]byte, TempBufferLength))
	}
	keyLength, labelLength, err := EncodeArchiveCounter(tempBuffer, name, archiveId)
	if err != nil {
		return nil, err
	}
	return a.AddCounter(typeId, tempBuffer, 0, keyLength, tempBuffer, keyLength, labelLength)
}

// putASCII writes at most maxLength UTF-16 code units of s as Agrona putStringWithoutLengthAscii does (code units
// above 127 become '?') and returns the number of bytes written.
func putASCII(buffer *atomic.Buffer, offset int32, s string, maxLength int32) int32 {
	if maxLength <= 0 {
		return 0
	}
	var units []uint16
	ascii := true
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			ascii = false
			break
		}
	}
	n := int32(len(s))
	if !ascii {
		units = utf16.Encode([]rune(s))
		n = int32(len(units))
	}
	if n > maxLength {
		n = maxLength
	}
	for i := int32(0); i < n; i++ {
		var c uint16
		if ascii {
			c = uint16(s[i])
		} else {
			c = units[i]
		}
		if c > 127 {
			c = '?'
		}
		buffer.PutUInt8(offset+i, uint8(c))
	}
	return n
}
