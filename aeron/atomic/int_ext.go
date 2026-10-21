package atomic

import "sync/atomic"

// CompareAndSet atomically sets the value to update if it currently equals expect.
func (i *Int) CompareAndSet(expect, update int32) bool {
	return atomic.CompareAndSwapInt32(&i.value, expect, update)
}

// GetAndSet atomically sets v and returns the previous value.
func (i *Int) GetAndSet(v int32) int32 {
	return atomic.SwapInt32(&i.value, v)
}

// CompareAndSet atomically sets the value to update if it currently equals expect.
func (i *Long) CompareAndSet(expect, update int64) bool {
	return atomic.CompareAndSwapInt64(&i.value, expect, update)
}

// GetAndSet atomically sets v and returns the previous value.
func (i *Long) GetAndSet(v int64) int64 {
	return atomic.SwapInt64(&i.value, v)
}
