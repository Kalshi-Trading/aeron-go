/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package term

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/util"
)

// DirectBufferVector is a slice of a buffer to be gathered into a message (io.aeron.DirectBufferVector).
type DirectBufferVector struct {
	Buffer *atomic.Buffer
	Offset int32
	Length int32
}

// NewDirectBufferVector creates a vector over buffer[offset:offset+length].
func NewDirectBufferVector(buffer *atomic.Buffer, offset, length int32) DirectBufferVector {
	return DirectBufferVector{Buffer: buffer, Offset: offset, Length: length}
}

// Reset re-points the vector, allowing it to be reused without allocation.
func (v *DirectBufferVector) Reset(buffer *atomic.Buffer, offset, length int32) *DirectBufferVector {
	v.Buffer, v.Offset, v.Length = buffer, offset, length
	return v
}

// Validate checks that the vector lies within its buffer.
func (v *DirectBufferVector) Validate() error {
	if v.Buffer == nil {
		return fmt.Errorf("nil buffer in vector")
	}
	capacity := v.Buffer.Capacity()
	if v.Offset < 0 || v.Offset >= capacity {
		return fmt.Errorf("offset=%d capacity=%d", v.Offset, capacity)
	}
	if v.Length < 0 || v.Length > capacity-v.Offset {
		return fmt.Errorf("offset=%d capacity=%d length=%d", v.Offset, capacity, v.Length)
	}
	return nil
}

// ValidateAndComputeLength validates each vector and returns their total length.
func ValidateAndComputeLength(vectors []DirectBufferVector) (int32, error) {
	var messageLength int32
	for i := range vectors {
		if err := vectors[i].Validate(); err != nil {
			return 0, err
		}
		messageLength += vectors[i].Length
		if messageLength < 0 {
			return 0, fmt.Errorf("length overflow in %d vectors", len(vectors))
		}
	}
	return messageLength, nil
}

// ComputeFragmentedFrameLength returns the total aligned length of the frames needed to carry a message of length
// bytes when fragmented into payloads of at most maxPayloadLength (FrameDescriptor.computeFragmentedFrameLength).
func ComputeFragmentedFrameLength(length, maxPayloadLength int32) int32 {
	numMaxPayloads := length / maxPayloadLength
	remainingPayload := length % maxPayloadLength
	var lastFrameLength int32
	if remainingPayload > 0 {
		lastFrameLength = util.AlignInt32(remainingPayload+logbuffer.DataFrameHeader.Length, logbuffer.FrameAlignment)
	}
	return numMaxPayloads*(maxPayloadLength+logbuffer.DataFrameHeader.Length) + lastFrameLength
}

// headerWriter writes the default frame header of a publication as three 64-bit words, the first (frame length,
// version, flags, type) with a negative length and an ordered store so the frame is not visible until committed.
type headerWriter struct {
	versionFlagsType int64
	sessionID        int64
	streamID         int64
}

func (header *headerWriter) fill(defaultHdr *atomic.Buffer) {
	header.versionFlagsType = int64(defaultHdr.GetInt32(logbuffer.DataFrameHeader.VersionFieldOffset)) << 32
	header.sessionID = int64(defaultHdr.GetInt32(logbuffer.DataFrameHeader.SessionIDFieldOffset)) << 32
	header.streamID = int64(uint32(defaultHdr.GetInt32(logbuffer.DataFrameHeader.StreamIDFieldOffset)))
}

func (header *headerWriter) write(termBuffer *atomic.Buffer, offset, length, termID int32) {
	termBuffer.PutInt64Ordered(offset, header.versionFlagsType|int64(uint32(-length)))
	termBuffer.PutInt64(offset+logbuffer.DataFrameHeader.TermOffsetFieldOffset, header.sessionID|int64(uint32(offset)))
	termBuffer.PutInt64(offset+logbuffer.DataFrameHeader.StreamIDFieldOffset, int64(termID)<<32|header.streamID)
}

func putReservedValue(termBuffer *atomic.Buffer, offset, frameLength int32, supplier ReservedValueSupplier) {
	if supplier != nil {
		value := supplier(termBuffer, offset, frameLength)
		termBuffer.PutInt64(offset+logbuffer.DataFrameHeader.ReservedValueFieldOffset, value)
	}
}

func writeVectors(termBuffer *atomic.Buffer, offset int32, vectors []DirectBufferVector) {
	for i := range vectors {
		v := &vectors[i]
		termBuffer.PutBytes(offset, v.Buffer, v.Offset, v.Length)
		offset += v.Length
	}
}

func finishFragment(termBuffer *atomic.Buffer, frameOffset, frameLength int32, flags uint8,
	supplier ReservedValueSupplier) {
	logbuffer.FrameFlags(termBuffer, frameOffset, flags)
	putReservedValue(termBuffer, frameOffset, frameLength, supplier)
	termBuffer.PutInt32Ordered(frameOffset, frameLength)
}

func fragmentSingleSource(termBuffer *atomic.Buffer, header *headerWriter, frameOffset, termID int32,
	src *atomic.Buffer, srcOffset, length, maxPayloadLength int32, supplier ReservedValueSupplier) {
	flags := beginFrag
	remaining := length
	for remaining > 0 {
		bytesToWrite := minInt32(remaining, maxPayloadLength)
		frameLength := bytesToWrite + logbuffer.DataFrameHeader.Length
		alignedLength := util.AlignInt32(frameLength, logbuffer.FrameAlignment)

		header.write(termBuffer, frameOffset, frameLength, termID)
		termBuffer.PutBytes(frameOffset+logbuffer.DataFrameHeader.Length, src, srcOffset+(length-remaining), bytesToWrite)
		if remaining <= maxPayloadLength {
			flags |= endFrag
		}
		finishFragment(termBuffer, frameOffset, frameLength, flags, supplier)

		flags = 0
		frameOffset += alignedLength
		remaining -= bytesToWrite
	}
}

func fragmentTwoSources(termBuffer *atomic.Buffer, header *headerWriter, frameOffset, termID int32,
	srcOne *atomic.Buffer, offsetOne, lengthOne int32, srcTwo *atomic.Buffer, offsetTwo, lengthTwo int32,
	maxPayloadLength int32, supplier ReservedValueSupplier) {
	flags := beginFrag
	remaining := lengthOne + lengthTwo
	var posOne, posTwo int32
	for remaining > 0 {
		bytesToWrite := minInt32(remaining, maxPayloadLength)
		frameLength := bytesToWrite + logbuffer.DataFrameHeader.Length
		alignedLength := util.AlignInt32(frameLength, logbuffer.FrameAlignment)

		header.write(termBuffer, frameOffset, frameLength, termID)
		var bytesWritten int32
		payloadOffset := frameOffset + logbuffer.DataFrameHeader.Length
		for bytesWritten < bytesToWrite {
			if remainingOne := lengthOne - posOne; remainingOne > 0 {
				numBytes := minInt32(bytesToWrite-bytesWritten, remainingOne)
				termBuffer.PutBytes(payloadOffset, srcOne, offsetOne+posOne, numBytes)
				bytesWritten += numBytes
				payloadOffset += numBytes
				posOne += numBytes
			} else {
				numBytes := minInt32(bytesToWrite-bytesWritten, lengthTwo-posTwo)
				termBuffer.PutBytes(payloadOffset, srcTwo, offsetTwo+posTwo, numBytes)
				bytesWritten += numBytes
				payloadOffset += numBytes
				posTwo += numBytes
			}
		}
		if remaining <= maxPayloadLength {
			flags |= endFrag
		}
		finishFragment(termBuffer, frameOffset, frameLength, flags, supplier)

		flags = 0
		frameOffset += alignedLength
		remaining -= bytesToWrite
	}
}

func fragmentVectors(termBuffer *atomic.Buffer, header *headerWriter, frameOffset, termID int32,
	vectors []DirectBufferVector, length, maxPayloadLength int32, supplier ReservedValueSupplier) {
	flags := beginFrag
	remaining := length
	vectorIndex := 0
	var vectorOffset int32
	for remaining > 0 {
		bytesToWrite := minInt32(remaining, maxPayloadLength)
		frameLength := bytesToWrite + logbuffer.DataFrameHeader.Length
		alignedLength := util.AlignInt32(frameLength, logbuffer.FrameAlignment)

		header.write(termBuffer, frameOffset, frameLength, termID)
		var bytesWritten int32
		payloadOffset := frameOffset + logbuffer.DataFrameHeader.Length
		for bytesWritten < bytesToWrite {
			v := &vectors[vectorIndex]
			vectorRemaining := v.Length - vectorOffset
			numBytes := minInt32(bytesToWrite-bytesWritten, vectorRemaining)
			if numBytes > 0 {
				termBuffer.PutBytes(payloadOffset, v.Buffer, v.Offset+vectorOffset, numBytes)
			}
			bytesWritten += numBytes
			payloadOffset += numBytes
			vectorOffset += numBytes
			if vectorRemaining <= numBytes {
				vectorIndex++
				vectorOffset = 0
			}
		}
		if remaining <= maxPayloadLength {
			flags |= endFrag
		}
		finishFragment(termBuffer, frameOffset, frameLength, flags, supplier)

		flags = 0
		frameOffset += alignedLength
		remaining -= bytesToWrite
	}
}
