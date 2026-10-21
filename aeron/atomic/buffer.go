/*
Copyright 2016 Stanislav Liberman

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

package atomic

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"unsafe"
)

// Buffer is the equivalent of AtomicBuffer used by Aeron Java and C++ implementations. It provides
// plain, ordered (release), volatile and atomic operations on a raw region of memory, which may be
// a Go byte slice or memory-mapped file. All multi-byte values use native (little-endian on
// amd64/arm64) byte order, the same as Agrona's UnsafeBuffer.
//
// Bounds are checked on every access unless the package is built with the
// aeron_disable_bounds_checks build tag (see BoundsChecksEnabled). Out of bounds access panics
// with an *IndexOutOfBoundsError.
type Buffer struct {
	bufferPtr unsafe.Pointer
	length    int32
}

// MakeBuffer takes a variety of argument options and returns a new atomic.Buffer.
//
//	MakeBuffer()                       // empty buffer
//	MakeBuffer([]byte)
//	MakeBuffer([]byte, length)
//	MakeBuffer([]byte, offset, length)
//	MakeBuffer(unsafe.Pointer, length)
//
// Unsupported argument types panic. Prefer the typed constructors NewBufferSlice and
// NewBufferPointer.
func MakeBuffer(args ...interface{}) *Buffer {
	buf := new(Buffer)
	switch len(args) {
	case 0:
		return buf
	case 1:
		switch v := args[0].(type) {
		case nil:
			return buf
		case []byte:
			return buf.WrapSlice(v)
		case unsafe.Pointer:
			return buf.Wrap(v, 0)
		}
	case 2:
		length, ok := toInt32(args[1])
		if !ok {
			break
		}
		switch v := args[0].(type) {
		case []byte:
			checkSliceRange(len(v), 0, length)
			return buf.Wrap(unsafe.Pointer(unsafe.SliceData(v)), length)
		case unsafe.Pointer:
			return buf.Wrap(v, length)
		}
	case 3:
		offset, ok1 := toInt32(args[1])
		length, ok2 := toInt32(args[2])
		if v, ok := args[0].([]byte); ok && ok1 && ok2 {
			checkSliceRange(len(v), offset, length)
			return buf.WrapSlice(v[offset : offset+length])
		}
	}
	panic(fmt.Sprintf("atomic.MakeBuffer: unsupported arguments %T", args))
}

// NewBufferSlice returns a Buffer wrapping the whole of b.
func NewBufferSlice(b []byte) *Buffer {
	return new(Buffer).WrapSlice(b)
}

// NewBufferPointer returns a Buffer wrapping length bytes of raw memory at ptr.
func NewBufferPointer(ptr unsafe.Pointer, length int32) *Buffer {
	return new(Buffer).Wrap(ptr, length)
}

func toInt32(v interface{}) (int32, bool) {
	switch n := v.(type) {
	case int:
		return int32(n), true
	case int32:
		return n, true
	case int64:
		return int32(n), true
	case uint32:
		return int32(n), true
	case int16:
		return int32(n), true
	case uint16:
		return int32(n), true
	case uint8:
		return int32(n), true
	case int8:
		return int32(n), true
	}
	return 0, false
}

func checkSliceRange(sliceLen int, offset, length int32) {
	if offset < 0 || length < 0 || int64(offset)+int64(length) > int64(sliceLen) {
		panic(&IndexOutOfBoundsError{Index: offset, Length: length, Capacity: int32(sliceLen)})
	}
}

// Wrap raw memory with this buffer instance.
//
//go:norace
func (buf *Buffer) Wrap(buffer unsafe.Pointer, length int32) *Buffer {
	buf.bufferPtr = buffer
	buf.length = length
	return buf
}

// WrapSlice wraps the whole of b. The buffer keeps b's backing array alive.
//
//go:norace
func (buf *Buffer) WrapSlice(b []byte) *Buffer {
	buf.bufferPtr = unsafe.Pointer(unsafe.SliceData(b))
	buf.length = int32(len(b))
	return buf
}

// WrapBuffer wraps the region [offset, offset+length) of another buffer, like Agrona's
// wrap(DirectBuffer, offset, length).
//
//go:norace
func (buf *Buffer) WrapBuffer(src *Buffer, offset, length int32) *Buffer {
	src.BoundsCheck(offset, length)
	if length == 0 {
		// Avoid forming a pointer one past the end of the source allocation.
		buf.bufferPtr = src.bufferPtr
	} else {
		buf.bufferPtr = unsafe.Add(src.bufferPtr, offset)
	}
	buf.length = length
	return buf
}

// Ptr will return the raw memory pointer for the underlying buffer
//
//go:norace
func (buf *Buffer) Ptr() unsafe.Pointer {
	return buf.bufferPtr
}

// PtrAt returns a pointer to the byte at index after checking that length bytes are in bounds.
//
//go:norace
func (buf *Buffer) PtrAt(index, length int32) unsafe.Pointer {
	buf.BoundsCheck(index, length)
	if index == buf.length {
		return buf.bufferPtr
	}
	return unsafe.Add(buf.bufferPtr, index)
}

// Capacity of the buffer, which is used for bound checking
//
//go:norace
func (buf *Buffer) Capacity() int32 {
	return buf.length
}

// Slice returns a []byte aliasing [index, index+length) of the buffer memory. Writes through the
// slice are visible in the buffer.
//
//go:norace
func (buf *Buffer) Slice(index, length int32) []byte {
	buf.BoundsCheck(index, length)
	if length == 0 {
		return []byte{}
	}
	return unsafe.Slice((*byte)(unsafe.Add(buf.bufferPtr, index)), length)
}

//go:norace
func (buf *Buffer) at(offset int32) unsafe.Pointer {
	return unsafe.Add(buf.bufferPtr, offset)
}

// Fill the buffer with the value of the argument byte. Generally used for initialization,
// since it's somewhat expensive.
//
//go:norace
func (buf *Buffer) Fill(b uint8) {
	buf.SetMemory(0, buf.length, b)
}

// SetMemory sets length bytes starting at index to value.
//
//go:norace
func (buf *Buffer) SetMemory(index, length int32, value uint8) {
	buf.BoundsCheck(index, length)
	if length == 0 {
		return
	}
	s := unsafe.Slice((*byte)(buf.at(index)), length)
	for i := range s {
		s[i] = value
	}
}

//go:norace
func (buf *Buffer) GetUInt8(offset int32) uint8 {
	buf.BoundsCheck(offset, 1)
	return *(*uint8)(buf.at(offset))
}

//go:norace
func (buf *Buffer) GetUInt16(offset int32) uint16 {
	buf.BoundsCheck(offset, 2)
	return *(*uint16)(buf.at(offset))
}

//go:norace
func (buf *Buffer) GetInt32(offset int32) int32 {
	buf.BoundsCheck(offset, 4)
	return *(*int32)(buf.at(offset))
}

//go:norace
func (buf *Buffer) GetInt64(offset int32) int64 {
	buf.BoundsCheck(offset, 8)
	return *(*int64)(buf.at(offset))
}

//go:norace
func (buf *Buffer) PutUInt8(offset int32, value uint8) {
	buf.BoundsCheck(offset, 1)
	*(*uint8)(buf.at(offset)) = value
}

//go:norace
func (buf *Buffer) PutInt8(offset int32, value int8) {
	buf.BoundsCheck(offset, 1)
	*(*int8)(buf.at(offset)) = value
}

//go:norace
func (buf *Buffer) PutUInt16(offset int32, value uint16) {
	buf.BoundsCheck(offset, 2)
	*(*uint16)(buf.at(offset)) = value
}

//go:norace
func (buf *Buffer) PutInt32(offset int32, value int32) {
	buf.BoundsCheck(offset, 4)
	*(*int32)(buf.at(offset)) = value
}

//go:norace
func (buf *Buffer) PutInt64(offset int32, value int64) {
	buf.BoundsCheck(offset, 8)
	*(*int64)(buf.at(offset)) = value
}

// GetAndAddInt64 atomically adds delta and returns the previous value.
func (buf *Buffer) GetAndAddInt64(offset int32, delta int64) int64 {
	buf.BoundsCheck(offset, 8)
	return atomic.AddInt64((*int64)(buf.at(offset)), delta) - delta
}

// GetInt32Volatile is an acquire (sequentially consistent) load.
func (buf *Buffer) GetInt32Volatile(offset int32) int32 {
	buf.BoundsCheck(offset, 4)
	return atomic.LoadInt32((*int32)(buf.at(offset)))
}

// GetInt64Volatile is an acquire (sequentially consistent) load.
func (buf *Buffer) GetInt64Volatile(offset int32) int64 {
	buf.BoundsCheck(offset, 8)
	return atomic.LoadInt64((*int64)(buf.at(offset)))
}

// PutInt64Ordered is a release store (Agrona putLongOrdered/putLongRelease).
func (buf *Buffer) PutInt64Ordered(offset int32, value int64) {
	buf.BoundsCheck(offset, 8)
	atomic.StoreInt64((*int64)(buf.at(offset)), value)
}

// PutInt32Ordered is a release store (Agrona putIntOrdered/putIntRelease).
func (buf *Buffer) PutInt32Ordered(offset int32, value int32) {
	buf.BoundsCheck(offset, 4)
	atomic.StoreInt32((*int32)(buf.at(offset)), value)
}

// PutIntOrdered is a release store of a 32-bit value.
//
// Deprecated: use PutInt32Ordered.
func (buf *Buffer) PutIntOrdered(offset int32, value int) {
	buf.PutInt32Ordered(offset, int32(value))
}

// CompareAndSetInt64 atomically sets the value to updateValue if it currently equals expectedValue.
func (buf *Buffer) CompareAndSetInt64(offset int32, expectedValue, updateValue int64) bool {
	buf.BoundsCheck(offset, 8)
	return atomic.CompareAndSwapInt64((*int64)(buf.at(offset)), expectedValue, updateValue)
}

// CompareAndSetInt32 atomically sets the value to updateValue if it currently equals expectedValue.
func (buf *Buffer) CompareAndSetInt32(offset int32, expectedValue, updateValue int32) bool {
	buf.BoundsCheck(offset, 4)
	return atomic.CompareAndSwapInt32((*int32)(buf.at(offset)), expectedValue, updateValue)
}

// PutBytes copies length bytes from srcBuffer at srcIndex into this buffer at index.
//
//go:norace
func (buf *Buffer) PutBytes(index int32, srcBuffer *Buffer, srcIndex int32, length int32) {
	buf.BoundsCheck(index, length)
	srcBuffer.BoundsCheck(srcIndex, length)
	if length == 0 {
		return
	}
	copy(unsafe.Slice((*byte)(buf.at(index)), length), unsafe.Slice((*byte)(srcBuffer.at(srcIndex)), length))
}

// GetBytesArray returns a copy of length bytes starting at offset.
//
//go:norace
func (buf *Buffer) GetBytesArray(offset int32, length int32) []byte {
	buf.BoundsCheck(offset, length)
	bArr := make([]byte, length)
	if length > 0 {
		copy(bArr, unsafe.Slice((*byte)(buf.at(offset)), length))
	}
	return bArr
}

// GetBytes copies len(b) bytes starting at offset into b.
//
//go:norace
func (buf *Buffer) GetBytes(offset int32, b []byte) {
	length := int32(len(b))
	buf.BoundsCheck(offset, length)
	if length > 0 {
		copy(b, unsafe.Slice((*byte)(buf.at(offset)), length))
	}
}

// WriteBytes writes data from offset and length to the given dest buffer. This will
// grow the buffer as needed.
//
//go:norace
func (buf *Buffer) WriteBytes(dest *bytes.Buffer, offset int32, length int32) {
	buf.BoundsCheck(offset, length)
	if length > 0 {
		dest.Write(unsafe.Slice((*byte)(buf.at(offset)), length))
	}
}

// PutBytesArray copies length bytes from (*arr)[srcIndex:] into this buffer at index.
//
// Deprecated: use PutBytesSlice, which does not need a pointer to the slice.
//
//go:norace
func (buf *Buffer) PutBytesArray(index int32, arr *[]byte, srcIndex int32, length int32) {
	checkSliceRange(len(*arr), srcIndex, length)
	buf.PutBytesSlice(index, (*arr)[srcIndex:srcIndex+length])
}

// PutBytesSlice copies all of src into this buffer at index.
//
//go:norace
func (buf *Buffer) PutBytesSlice(index int32, src []byte) {
	length := int32(len(src))
	buf.BoundsCheck(index, length)
	if length > 0 {
		copy(unsafe.Slice((*byte)(buf.at(index)), length), src)
	}
}
