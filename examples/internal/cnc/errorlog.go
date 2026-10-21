package cnc

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
)

// Distinct error log record layout (org.agrona.concurrent.errors.DistinctErrorLog).
const (
	errorLengthOffset           = 0
	errorObservationCountOffset = 4
	errorLastObservationOffset  = 8
	errorFirstObservationOffset = 16
	errorEncodedErrorOffset     = 24
	errorRecordAlignment        = 8
)

// ErrorObservation is one distinct error in the error log.
type ErrorObservation struct {
	ObservationCount   int32
	FirstObservationMs int64
	LastObservationMs  int64
	EncodedError       string
}

// ReadErrors calls fn for every distinct error whose last observation is at or after sinceMs and
// returns the number of errors passed to fn (ErrorLogReader.read).
func ReadErrors(buffer *atomic.Buffer, sinceMs int64, fn func(ErrorObservation)) int {
	count := 0
	capacity := buffer.Capacity()
	for offset := int32(0); offset+errorEncodedErrorOffset <= capacity; {
		length := buffer.GetInt32Volatile(offset + errorLengthOffset)
		if length <= 0 || offset+length > capacity {
			break
		}
		last := buffer.GetInt64Volatile(offset + errorLastObservationOffset)
		if last >= sinceMs {
			count++
			fn(ErrorObservation{
				ObservationCount:   buffer.GetInt32Volatile(offset + errorObservationCountOffset),
				FirstObservationMs: buffer.GetInt64(offset + errorFirstObservationOffset),
				LastObservationMs:  last,
				EncodedError:       string(buffer.GetBytesArray(offset+errorEncodedErrorOffset, length-errorEncodedErrorOffset)),
			})
		}
		offset += util.AlignInt32(length, errorRecordAlignment)
	}
	return count
}
