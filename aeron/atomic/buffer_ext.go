package atomic

import (
	"fmt"
	"math"
	"sync/atomic"
	"unicode/utf8"
	"unsafe"
)

// Additional accessors bringing Buffer to parity with Agrona's AtomicBuffer/UnsafeBuffer.
//
// Go's sync/atomic operations are sequentially consistent, so the Release/Acquire/Opaque/Volatile
// variants below all map to the same (strongest) operation; they exist so code ported from Java reads
// the same. Atomic 32/64-bit operations require a naturally aligned address (see VerifyAlignment).

//go:norace
func (buf *Buffer) GetInt8(offset int32) int8 {
	buf.BoundsCheck(offset, 1)
	return *(*int8)(buf.at(offset))
}

//go:norace
func (buf *Buffer) GetInt16(offset int32) int16 {
	buf.BoundsCheck(offset, 2)
	return *(*int16)(buf.at(offset))
}

//go:norace
func (buf *Buffer) PutInt16(offset int32, value int16) {
	buf.BoundsCheck(offset, 2)
	*(*int16)(buf.at(offset)) = value
}

//go:norace
func (buf *Buffer) GetUInt32(offset int32) uint32 {
	buf.BoundsCheck(offset, 4)
	return *(*uint32)(buf.at(offset))
}

//go:norace
func (buf *Buffer) PutUInt32(offset int32, value uint32) {
	buf.BoundsCheck(offset, 4)
	*(*uint32)(buf.at(offset)) = value
}

//go:norace
func (buf *Buffer) GetUInt64(offset int32) uint64 {
	buf.BoundsCheck(offset, 8)
	return *(*uint64)(buf.at(offset))
}

//go:norace
func (buf *Buffer) PutUInt64(offset int32, value uint64) {
	buf.BoundsCheck(offset, 8)
	*(*uint64)(buf.at(offset)) = value
}

//go:norace
func (buf *Buffer) GetFloat32(offset int32) float32 {
	return math.Float32frombits(buf.GetUInt32(offset))
}

//go:norace
func (buf *Buffer) PutFloat32(offset int32, value float32) {
	buf.PutUInt32(offset, math.Float32bits(value))
}

//go:norace
func (buf *Buffer) GetFloat64(offset int32) float64 {
	return math.Float64frombits(buf.GetUInt64(offset))
}

//go:norace
func (buf *Buffer) PutFloat64(offset int32, value float64) {
	buf.PutUInt64(offset, math.Float64bits(value))
}

// PutInt32Volatile is a sequentially consistent store.
func (buf *Buffer) PutInt32Volatile(offset int32, value int32) {
	buf.PutInt32Ordered(offset, value)
}

// PutInt64Volatile is a sequentially consistent store.
func (buf *Buffer) PutInt64Volatile(offset int32, value int64) {
	buf.PutInt64Ordered(offset, value)
}

// PutInt32Release is a release store (alias of PutInt32Ordered).
func (buf *Buffer) PutInt32Release(offset int32, value int32) { buf.PutInt32Ordered(offset, value) }

// PutInt64Release is a release store (alias of PutInt64Ordered).
func (buf *Buffer) PutInt64Release(offset int32, value int64) { buf.PutInt64Ordered(offset, value) }

// GetInt32Acquire is an acquire load (alias of GetInt32Volatile).
func (buf *Buffer) GetInt32Acquire(offset int32) int32 { return buf.GetInt32Volatile(offset) }

// GetInt64Acquire is an acquire load (alias of GetInt64Volatile).
func (buf *Buffer) GetInt64Acquire(offset int32) int64 { return buf.GetInt64Volatile(offset) }

// GetInt32Opaque is an atomic load with no ordering requirement beyond coherence.
func (buf *Buffer) GetInt32Opaque(offset int32) int32 { return buf.GetInt32Volatile(offset) }

// GetInt64Opaque is an atomic load with no ordering requirement beyond coherence.
func (buf *Buffer) GetInt64Opaque(offset int32) int64 { return buf.GetInt64Volatile(offset) }

// PutInt32Opaque is an atomic store with no ordering requirement beyond coherence.
func (buf *Buffer) PutInt32Opaque(offset int32, value int32) { buf.PutInt32Ordered(offset, value) }

// PutInt64Opaque is an atomic store with no ordering requirement beyond coherence.
func (buf *Buffer) PutInt64Opaque(offset int32, value int64) { buf.PutInt64Ordered(offset, value) }

// AddInt32Ordered adds delta with release semantics for a single writer (Agrona addIntOrdered/addIntRelease).
func (buf *Buffer) AddInt32Ordered(offset int32, delta int32) {
	buf.BoundsCheck(offset, 4)
	p := (*int32)(buf.at(offset))
	atomic.StoreInt32(p, atomic.LoadInt32(p)+delta)
}

// AddInt64Ordered adds delta with release semantics for a single writer (Agrona addLongOrdered/addLongRelease).
func (buf *Buffer) AddInt64Ordered(offset int32, delta int64) {
	buf.BoundsCheck(offset, 8)
	p := (*int64)(buf.at(offset))
	atomic.StoreInt64(p, atomic.LoadInt64(p)+delta)
}

// AddInt32Opaque adds delta for a single writer with opaque semantics.
func (buf *Buffer) AddInt32Opaque(offset int32, delta int32) { buf.AddInt32Ordered(offset, delta) }

// AddInt64Opaque adds delta for a single writer with opaque semantics.
func (buf *Buffer) AddInt64Opaque(offset int32, delta int64) { buf.AddInt64Ordered(offset, delta) }

// GetAndAddInt32 atomically adds delta and returns the previous value.
func (buf *Buffer) GetAndAddInt32(offset int32, delta int32) int32 {
	buf.BoundsCheck(offset, 4)
	return atomic.AddInt32((*int32)(buf.at(offset)), delta) - delta
}

// GetAndSetInt32 atomically sets value and returns the previous value.
func (buf *Buffer) GetAndSetInt32(offset int32, value int32) int32 {
	buf.BoundsCheck(offset, 4)
	return atomic.SwapInt32((*int32)(buf.at(offset)), value)
}

// GetAndSetInt64 atomically sets value and returns the previous value.
func (buf *Buffer) GetAndSetInt64(offset int32, value int64) int64 {
	buf.BoundsCheck(offset, 8)
	return atomic.SwapInt64((*int64)(buf.at(offset)), value)
}

// CompareAndExchangeInt32 atomically sets updateValue if the current value is expectedValue and
// returns the value witnessed (expectedValue on success).
func (buf *Buffer) CompareAndExchangeInt32(offset int32, expectedValue, updateValue int32) int32 {
	buf.BoundsCheck(offset, 4)
	p := (*int32)(buf.at(offset))
	for {
		if atomic.CompareAndSwapInt32(p, expectedValue, updateValue) {
			return expectedValue
		}
		if v := atomic.LoadInt32(p); v != expectedValue {
			return v
		}
	}
}

// CompareAndExchangeInt64 atomically sets updateValue if the current value is expectedValue and
// returns the value witnessed (expectedValue on success).
func (buf *Buffer) CompareAndExchangeInt64(offset int32, expectedValue, updateValue int64) int64 {
	buf.BoundsCheck(offset, 8)
	p := (*int64)(buf.at(offset))
	for {
		if atomic.CompareAndSwapInt64(p, expectedValue, updateValue) {
			return expectedValue
		}
		if v := atomic.LoadInt64(p); v != expectedValue {
			return v
		}
	}
}

// GetStringAscii reads a string prefixed by its int32 length at index.
//
//go:norace
func (buf *Buffer) GetStringAscii(index int32) string {
	buf.BoundsCheck(index, 4)
	length := buf.GetInt32(index)
	return buf.GetStringWithoutLengthAscii(index+4, length)
}

// GetStringAsciiLength reads length bytes following the int32 length prefix at index.
//
//go:norace
func (buf *Buffer) GetStringAsciiLength(index, length int32) string {
	return buf.GetStringWithoutLengthAscii(index+4, length)
}

// GetStringWithoutLengthAscii reads length bytes at index as a string.
//
//go:norace
func (buf *Buffer) GetStringWithoutLengthAscii(index, length int32) string {
	buf.BoundsCheck(index, length)
	if length == 0 {
		return ""
	}
	return string(unsafe.Slice((*byte)(buf.at(index)), length))
}

// PutStringAscii writes value prefixed by its int32 length and returns the number of bytes written.
// Characters outside the ASCII range are written as '?', like Agrona.
//
//go:norace
func (buf *Buffer) PutStringAscii(index int32, value string) int32 {
	length := asciiLength(value)
	buf.BoundsCheck(index, 4+length)
	buf.PutInt32(index, length)
	putAscii(buf, index+4, value)
	return 4 + length
}

// PutStringWithoutLengthAscii writes value without a length prefix and returns the bytes written.
//
//go:norace
func (buf *Buffer) PutStringWithoutLengthAscii(index int32, value string) int32 {
	length := asciiLength(value)
	buf.BoundsCheck(index, length)
	putAscii(buf, index, value)
	return length
}

func asciiLength(value string) int32 {
	for i := 0; i < len(value); i++ {
		if value[i] >= utf8.RuneSelf {
			return int32(utf8.RuneCountInString(value))
		}
	}
	return int32(len(value))
}

//go:norace
func putAscii(buf *Buffer, index int32, value string) {
	i := index
	for j := 0; j < len(value); {
		c := value[j]
		if c < utf8.RuneSelf {
			j++
		} else {
			_, size := utf8.DecodeRuneInString(value[j:])
			j += size
			c = '?'
		}
		*(*byte)(buf.at(i)) = c
		i++
	}
}

// GetStringUtf8 reads a UTF-8 string prefixed by its int32 byte length at index.
//
//go:norace
func (buf *Buffer) GetStringUtf8(index int32) string {
	return buf.GetStringAscii(index)
}

// GetStringWithoutLengthUtf8 reads length bytes at index as a UTF-8 string.
//
//go:norace
func (buf *Buffer) GetStringWithoutLengthUtf8(index, length int32) string {
	return buf.GetStringWithoutLengthAscii(index, length)
}

// PutStringUtf8 writes value as UTF-8 prefixed by its int32 byte length and returns the bytes written.
// It panics if the encoded length exceeds maxEncodedLength (pass math.MaxInt32 for no limit).
//
//go:norace
func (buf *Buffer) PutStringUtf8(index int32, value string, maxEncodedLength int32) int32 {
	if int64(len(value)) > int64(maxEncodedLength) {
		panic(fmt.Sprintf("encoded string larger than maximum size: %d", maxEncodedLength))
	}
	length := int32(len(value))
	buf.BoundsCheck(index, 4+length)
	buf.PutInt32(index, length)
	buf.PutBytesSlice(index+4, unsafe.Slice(unsafe.StringData(value), len(value)))
	return 4 + length
}

// PutStringWithoutLengthUtf8 writes value as UTF-8 without a length prefix and returns the bytes written.
//
//go:norace
func (buf *Buffer) PutStringWithoutLengthUtf8(index int32, value string) int32 {
	buf.PutBytesSlice(index, unsafe.Slice(unsafe.StringData(value), len(value)))
	return int32(len(value))
}

// Equals reports whether two buffers have identical capacity and content.
//
//go:norace
func (buf *Buffer) Equals(other *Buffer) bool {
	if buf.length != other.length {
		return false
	}
	return string(buf.Slice(0, buf.length)) == string(other.Slice(0, other.length))
}

// String returns a debug representation of the buffer.
func (buf *Buffer) String() string {
	return fmt.Sprintf("Buffer{address=%p, capacity=%d}", buf.bufferPtr, buf.length)
}
