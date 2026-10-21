package counters

import (
	"fmt"
	syncatomic "sync/atomic"
	"unsafe"
)

// AtomicCounter is a writable view of a counter's value in the values buffer (Agrona AtomicCounter). Go atomics are
// sequentially consistent, so the Java Ordered/Release/Opaque/Acquire variants map onto the same operations; the
// Plain variants are non-atomic.
type AtomicCounter struct {
	Reader         *Reader
	CounterId      int32
	metaDataOffset int32
	valueOffset    int32
}

func NewAtomicCounter(reader *Reader, counterId int32) (*AtomicCounter, error) {
	if err := reader.ValidateCounterId(counterId); err != nil {
		return nil, err
	}
	metaDataOffset := counterId * MetadataLength
	valueOffset := counterId * CounterLength
	return &AtomicCounter{reader, counterId, metaDataOffset, valueOffset}, nil
}

func (ac *AtomicCounter) ptr() *int64 {
	return (*int64)(unsafe.Add(ac.Reader.values.Ptr(), ac.valueOffset))
}

// Id returns the counter id.
func (ac *AtomicCounter) Id() int32 {
	return ac.CounterId
}

func (ac *AtomicCounter) State() int32 {
	return ac.Reader.metaData.GetInt32Volatile(ac.metaDataOffset)
}

func (ac *AtomicCounter) Label() string {
	return ac.Reader.labelValue(ac.metaDataOffset)
}

// TypeId returns the type id of the counter.
func (ac *AtomicCounter) TypeId() int32 {
	return ac.Reader.metaData.GetInt32(ac.metaDataOffset + TypeIdOffset)
}

func (ac *AtomicCounter) Get() int64 {
	return ac.Reader.values.GetInt64Volatile(ac.valueOffset)
}

func (ac *AtomicCounter) GetAcquire() int64 { return ac.Get() }

func (ac *AtomicCounter) GetWeak() int64 {
	return ac.Reader.values.GetInt64(ac.valueOffset)
}

func (ac *AtomicCounter) GetPlain() int64 { return ac.GetWeak() }

func (ac *AtomicCounter) Set(value int64) {
	ac.Reader.values.PutInt64Ordered(ac.valueOffset, value)
}

func (ac *AtomicCounter) SetOrdered(value int64) { ac.Set(value) }
func (ac *AtomicCounter) SetRelease(value int64) { ac.Set(value) }

func (ac *AtomicCounter) SetWeak(value int64) {
	ac.Reader.values.PutInt64(ac.valueOffset, value)
}

func (ac *AtomicCounter) SetPlain(value int64) { ac.SetWeak(value) }

// Increment atomically adds 1 and returns the previous value.
func (ac *AtomicCounter) Increment() int64 { return ac.GetAndAdd(1) }

func (ac *AtomicCounter) IncrementOrdered() int64 { return ac.GetAndAdd(1) }
func (ac *AtomicCounter) IncrementRelease() int64 { return ac.GetAndAdd(1) }

// IncrementPlain adds 1 without atomicity and returns the previous value.
func (ac *AtomicCounter) IncrementPlain() int64 { return ac.GetAndAddPlain(1) }

// Decrement atomically subtracts 1 and returns the previous value.
func (ac *AtomicCounter) Decrement() int64 { return ac.GetAndAdd(-1) }

func (ac *AtomicCounter) DecrementOrdered() int64 { return ac.GetAndAdd(-1) }
func (ac *AtomicCounter) DecrementRelease() int64 { return ac.GetAndAdd(-1) }
func (ac *AtomicCounter) DecrementPlain() int64   { return ac.GetAndAddPlain(-1) }

// GetAndAdd atomically adds increment and returns the previous value.
func (ac *AtomicCounter) GetAndAdd(increment int64) int64 {
	return syncatomic.AddInt64(ac.ptr(), increment) - increment
}

func (ac *AtomicCounter) GetAndAddOrdered(increment int64) int64 { return ac.GetAndAdd(increment) }
func (ac *AtomicCounter) GetAndAddRelease(increment int64) int64 { return ac.GetAndAdd(increment) }

// GetAndAddPlain adds increment without atomicity and returns the previous value.
func (ac *AtomicCounter) GetAndAddPlain(increment int64) int64 {
	p := ac.ptr()
	v := *p
	*p = v + increment
	return v
}

// GetAndSet atomically sets value and returns the previous value.
func (ac *AtomicCounter) GetAndSet(value int64) int64 {
	return syncatomic.SwapInt64(ac.ptr(), value)
}

// CompareAndSet atomically sets updateValue if the current value equals expectedValue.
func (ac *AtomicCounter) CompareAndSet(expectedValue, updateValue int64) bool {
	return syncatomic.CompareAndSwapInt64(ac.ptr(), expectedValue, updateValue)
}

// ProposeMax sets proposedValue if it is greater than the current value (single writer). Returns true if updated.
func (ac *AtomicCounter) ProposeMax(proposedValue int64) bool {
	if ac.GetPlain() < proposedValue {
		ac.SetPlain(proposedValue)
		return true
	}
	return false
}

// ProposeMaxOrdered sets proposedValue with an ordered store if it is greater than the current value (single writer).
func (ac *AtomicCounter) ProposeMaxOrdered(proposedValue int64) bool {
	if ac.GetPlain() < proposedValue {
		ac.Set(proposedValue)
		return true
	}
	return false
}

func (ac *AtomicCounter) ProposeMaxRelease(proposedValue int64) bool {
	return ac.ProposeMaxOrdered(proposedValue)
}

func (ac *AtomicCounter) String() string {
	return fmt.Sprintf("AtomicCounter{id=%d, value=%d}", ac.CounterId, ac.Get())
}
