/*
Copyright 2026 Kalshi Trading

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

package errorlog

import (
	"sync"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
)

// DistinctErrorLog records distinct errors into a buffer (ideally memory mapped) in the format read by Read. Errors
// are distinct by their encoded text: repeated observations only bump the count and last timestamp. Equivalent to
// org.agrona.concurrent.errors.DistinctErrorLog, with the encoding of an error being err.Error() (or a custom
// Encoder) instead of a Java stack trace. Safe for concurrent use.
type DistinctErrorLog struct {
	mu         sync.Mutex
	buffer     *atomic.Buffer
	nextOffset int32
	offsets    map[string]int32
	clock      func() int64
	encoder    func(error) string
}

// NewDistinctErrorLog creates a log over buffer. clock returns epoch ms; nil uses the wall clock.
func NewDistinctErrorLog(buffer *atomic.Buffer, clock func() int64) *DistinctErrorLog {
	if clock == nil {
		clock = func() int64 { return time.Now().UnixMilli() }
	}
	l := &DistinctErrorLog{buffer: buffer, offsets: map[string]int32{}, clock: clock,
		encoder: func(err error) string { return err.Error() }}
	// Pick up existing records so a reopened log keeps appending.
	Read(buffer, func(_ int32, _, _ int64, encoded string) {
		l.offsets[encoded] = l.nextOffset
		l.nextOffset += util.AlignInt32(EncodedErrorOffset+int32(len(encoded)), RecordAlignment)
	})
	return l
}

// SetEncoder overrides how errors are turned into text (e.g. to include a stack trace).
func (l *DistinctErrorLog) SetEncoder(encoder func(error) string) {
	l.mu.Lock()
	l.encoder = encoder
	l.mu.Unlock()
}

// Buffer returns the underlying buffer.
func (l *DistinctErrorLog) Buffer() *atomic.Buffer { return l.buffer }

// Record records an observation of err, returning false if the buffer has no space for a new distinct error.
func (l *DistinctErrorLog) Record(err error) bool {
	if err == nil {
		return false
	}
	now := l.clock()
	l.mu.Lock()
	defer l.mu.Unlock()
	encoded := l.encoder(err)
	if offset, ok := l.offsets[encoded]; ok {
		for {
			c := l.buffer.GetInt32Volatile(offset + ObservationCountOffset)
			if l.buffer.CompareAndSetInt32(offset+ObservationCountOffset, c, c+1) {
				break
			}
		}
		l.buffer.PutInt64Ordered(offset+LastObservationTimestampOffset, now)
		return true
	}
	offset := l.nextOffset
	if int64(offset)+int64(EncodedErrorOffset)+int64(len(encoded)) > int64(l.buffer.Capacity()) {
		return false
	}
	length := EncodedErrorOffset + int32(len(encoded))
	if len(encoded) > 0 {
		b := []byte(encoded)
		l.buffer.PutBytesSlice(offset+EncodedErrorOffset, b)
	}
	l.buffer.PutInt64(offset+FirstObservationTimestampOffset, now)
	l.buffer.PutInt64Ordered(offset+LastObservationTimestampOffset, now)
	l.buffer.PutInt32Ordered(offset+ObservationCountOffset, 1)
	l.buffer.PutInt32Ordered(offset+LengthOffset, length)
	l.offsets[encoded] = offset
	l.nextOffset = offset + util.AlignInt32(length, RecordAlignment)
	return true
}
