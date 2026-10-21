package atomic

import "fmt"

// IndexOutOfBoundsError is the panic value raised by Buffer accessors on out of bounds access,
// the equivalent of Java's IndexOutOfBoundsException.
type IndexOutOfBoundsError struct {
	Index, Length, Capacity int32
}

func (e *IndexOutOfBoundsError) Error() string {
	return fmt.Sprintf("index=%d length=%d capacity=%d: out of bounds", e.Index, e.Length, e.Capacity)
}

// BoundsChecksEnabled reports whether Buffer accessors check bounds. Checks are disabled by building
// with -tags aeron_disable_bounds_checks (Agrona's agrona.disable.bounds.checks).
func BoundsChecksEnabled() bool {
	return boundsChecksEnabled
}

// BoundsCheck panics with *IndexOutOfBoundsError if [index, index+length) is not within [0, capacity).
//
//go:norace
func BoundsCheck(index int32, length int32, capacity int32) {
	if boundsChecksEnabled && (index < 0 || length < 0 || int64(index)+int64(length) > int64(capacity)) {
		panic(&IndexOutOfBoundsError{Index: index, Length: length, Capacity: capacity})
	}
}

// BoundsCheck checks that [index, index+length) lies within the buffer.
//
//go:norace
func (buf *Buffer) BoundsCheck(index, length int32) {
	BoundsCheck(index, length, buf.length)
}

// AlignmentError is returned by VerifyAlignment.
type AlignmentError struct {
	Address   uintptr
	Alignment int
}

func (e *AlignmentError) Error() string {
	return fmt.Sprintf("buffer address %#x is not aligned to %d bytes", e.Address, e.Alignment)
}

// Alignment required of the buffer address for atomic 64-bit operations.
const Alignment = 8

// VerifyAlignment returns an error if the buffer address is not 8-byte aligned, which is required for
// atomic int64 operations on all architectures (Agrona verifyAlignment).
//
//go:norace
func (buf *Buffer) VerifyAlignment() error {
	addr := uintptr(buf.bufferPtr)
	if addr&(Alignment-1) != 0 {
		return &AlignmentError{Address: addr, Alignment: Alignment}
	}
	return nil
}

// IsAligned reports whether index within this buffer is aligned to alignment (a power of two).
//
//go:norace
func (buf *Buffer) IsAligned(index int32, alignment int) bool {
	return (uintptr(buf.bufferPtr)+uintptr(index))&uintptr(alignment-1) == 0
}
