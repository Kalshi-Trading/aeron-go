//go:build !amd64

package atomic

// AcquireFence orders prior loads before subsequent loads and stores (VarHandle.acquireFence()).
// On weakly ordered architectures (arm64, ppc64, ...) this needs a hardware barrier.
func AcquireFence() { FullFence() }

// ReleaseFence orders prior loads and stores before subsequent stores (VarHandle.releaseFence()).
func ReleaseFence() { FullFence() }
