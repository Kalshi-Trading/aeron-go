package atomic

// AcquireFence orders prior loads before subsequent loads and stores (VarHandle.acquireFence()).
// amd64 (TSO) never reorders loads with later loads/stores, so only a compiler barrier is needed;
// a non-inlined call is one.
//
//go:noinline
func AcquireFence() {}

// ReleaseFence orders prior loads and stores before subsequent stores (VarHandle.releaseFence()).
// amd64 (TSO) never reorders stores with prior loads/stores, so only a compiler barrier is needed.
//
//go:noinline
func ReleaseFence() {}
