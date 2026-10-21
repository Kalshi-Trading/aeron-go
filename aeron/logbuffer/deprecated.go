// Deprecated aliases kept for one release after the Id -> ID rename; remove in the next breaking release.

package logbuffer

import "github.com/lirm/aeron-go/aeron/atomic"

// Deprecated: use TermID.
func (hdr *Header) TermId() int32 {
	return hdr.TermID()
}

// Deprecated: use SessionID.
func (hdr *Header) SessionId() int32 {
	return hdr.SessionID()
}

// Deprecated: use StreamID.
func (hdr *Header) StreamId() int32 {
	return hdr.StreamID()
}

// Deprecated: use InitialTermID.
func (hdr *Header) InitialTermId() int32 {
	return hdr.InitialTermID()
}

// Deprecated: use SetSessionID.
func (hdr *Header) SetSessionId(value int32) *Header {
	return hdr.SetSessionID(value)
}

// Deprecated: use GetFrameTermID.
func GetTermId(logBuffer *atomic.Buffer, frameOffset int32) int32 {
	return GetFrameTermID(logBuffer, frameOffset)
}

// Deprecated: use GetFrameSessionID.
func GetSessionId(logBuffer *atomic.Buffer, frameOffset int32) int32 {
	return GetFrameSessionID(logBuffer, frameOffset)
}

// Deprecated: use GetFrameStreamID.
func GetStreamId(logBuffer *atomic.Buffer, frameOffset int32) int32 {
	return GetFrameStreamID(logBuffer, frameOffset)
}
