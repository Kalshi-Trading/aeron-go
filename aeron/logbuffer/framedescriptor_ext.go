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

package logbuffer

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
)

// Fragmentation flags and limits from io.aeron.logbuffer.FrameDescriptor.
const (
	BeginFrag        uint8  = 0x80
	EndFrag          uint8  = 0x40
	Unfragmented     uint8  = BeginFrag | EndFrag
	PaddingFrameType uint16 = 0x00
	// MaxMessageLength is the max length of a message, irrespective of term length.
	MaxMessageLength int32 = 16 * 1024 * 1024
)

// CheckHeaderLength checks the default header length matches DataFrameHeader.Length.
func CheckHeaderLength(length int32) error {
	if length != DataFrameHeader.Length {
		return fmt.Errorf("frame header length %d must be equal to %d", length, DataFrameHeader.Length)
	}
	return nil
}

// CheckMaxFrameLength checks a max frame length is a multiple of FrameAlignment.
func CheckMaxFrameLength(length int32) error {
	if length&(FrameAlignment-1) != 0 {
		return fmt.Errorf("max frame length must be a multiple of %d, length=%d", FrameAlignment, length)
	}
	return nil
}

func termOffsetOffset(frameOffset int32) int32 {
	return frameOffset + DataFrameHeader.TermOffsetFieldOffset
}
func versionOffset(frameOffset int32) int32 { return frameOffset + DataFrameHeader.VersionFieldOffset }

// FrameVersion reads the version field of a frame.
func FrameVersion(buffer *atomic.Buffer, frameOffset int32) uint8 {
	return buffer.GetUInt8(versionOffset(frameOffset))
}

// FrameType reads the type field of a frame.
func FrameType(buffer *atomic.Buffer, frameOffset int32) uint16 {
	return buffer.GetUInt16(typeOffset(frameOffset))
}

// GetFrameTermOffset reads the term offset field of a frame.
func GetFrameTermOffset(buffer *atomic.Buffer, frameOffset int32) int32 {
	return buffer.GetInt32(termOffsetOffset(frameOffset))
}

// FrameTermOffset writes the term offset field of a frame.
func FrameTermOffset(buffer *atomic.Buffer, frameOffset int32, termOffset int32) {
	buffer.PutInt32(termOffsetOffset(frameOffset), termOffset)
}

// FrameTermID writes the term id field of a frame.
func FrameTermID(buffer *atomic.Buffer, frameOffset int32, termID int32) {
	buffer.PutInt32(termIdOffset(frameOffset), termID)
}

// FrameSessionID writes the session id field of a frame.
func FrameSessionID(buffer *atomic.Buffer, frameOffset int32, sessionID int32) {
	buffer.PutInt32(sessionIdOffset(frameOffset), sessionID)
}

// GetFrameLengthPlain reads the frame length without volatile semantics.
func GetFrameLengthPlain(buffer *atomic.Buffer, frameOffset int32) int32 {
	return buffer.GetInt32(lengthOffset(frameOffset))
}

// IsFrameAligned returns true if length is a multiple of FrameAlignment.
func IsFrameAligned(length int32) bool {
	return length&(FrameAlignment-1) == 0
}
