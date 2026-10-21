// Deprecated aliases kept for one release after the Id -> ID rename; remove in the next breaking release.

package logbuffer

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
