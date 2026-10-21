package atomic

import "sync/atomic"

// FullFence is a full (StoreLoad) memory barrier, the equivalent of Java's VarHandle.fullFence().
// It is implemented with a locked read-modify-write on a stack word, which orders all prior loads
// and stores before all subsequent ones on every architecture Go supports.
//
//go:noinline
func FullFence() {
	var x int32
	atomic.AddInt32(&x, 0)
}
