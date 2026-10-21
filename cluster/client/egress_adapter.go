package client

import (
	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
)

// EgressAdapter decodes egress for a session and dispatches to an EgressListener, mirroring
// io.aeron.cluster.client.EgressAdapter. Useful when polling the egress subscription outside of AeronCluster.
type EgressAdapter struct {
	clusterSessionId  int64
	fragmentLimit     int
	listener          EgressListener
	extension         EgressListenerExtension
	subscription      *aeron.Subscription
	fragmentAssembler *aeron.FragmentAssembler
	// ErrorHandler receives decode errors and unexpected schemas. Defaults to logging.
	ErrorHandler func(error)
}

// NewEgressAdapter creates an adapter; extension may be nil.
func NewEgressAdapter(listener EgressListener, extension EgressListenerExtension, clusterSessionId int64,
	subscription *aeron.Subscription, fragmentLimit int) *EgressAdapter {
	a := &EgressAdapter{
		clusterSessionId: clusterSessionId,
		fragmentLimit:    fragmentLimit,
		listener:         listener,
		extension:        extension,
		subscription:     subscription,
		ErrorHandler:     logError,
	}
	a.fragmentAssembler = aeron.NewFragmentAssembler(a.OnFragment, 0)
	return a
}

// Poll the egress subscription and dispatch to the listener.
func (a *EgressAdapter) Poll() int {
	return a.subscription.Poll(a.fragmentAssembler.OnFragment, a.fragmentLimit)
}

// OnFragment handles a reassembled egress message.
func (a *EgressAdapter) OnFragment(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
	var ext ControlledEgressListenerExtension
	if a.extension != nil {
		ext = uncontrolledExtension{a.extension}
	}
	dispatchEgress(a.clusterSessionId, buffer, offset, length, header, uncontrolled{a.listener}, ext, nil, nil,
		a.ErrorHandler)
}

// ControlledEgressAdapter is the controlled-poll variant of EgressAdapter, mirroring
// io.aeron.cluster.client.ControlledEgressAdapter.
type ControlledEgressAdapter struct {
	clusterSessionId  int64
	fragmentLimit     int
	listener          ControlledEgressListener
	extension         ControlledEgressListenerExtension
	subscription      *aeron.Subscription
	fragmentAssembler *aeron.ControlledFragmentAssembler
	ErrorHandler      func(error)
}

// NewControlledEgressAdapter creates an adapter; extension may be nil.
func NewControlledEgressAdapter(listener ControlledEgressListener, extension ControlledEgressListenerExtension,
	clusterSessionId int64, subscription *aeron.Subscription, fragmentLimit int) *ControlledEgressAdapter {
	a := &ControlledEgressAdapter{
		clusterSessionId: clusterSessionId,
		fragmentLimit:    fragmentLimit,
		listener:         listener,
		extension:        extension,
		subscription:     subscription,
		ErrorHandler:     logError,
	}
	a.fragmentAssembler = aeron.NewControlledFragmentAssembler(a.OnFragment, 0)
	return a
}

// Poll the egress subscription and dispatch to the listener.
func (a *ControlledEgressAdapter) Poll() int {
	return a.subscription.ControlledPoll(a.fragmentAssembler.OnFragment, a.fragmentLimit)
}

// OnFragment handles a reassembled egress message.
func (a *ControlledEgressAdapter) OnFragment(buffer *atomic.Buffer, offset int32, length int32,
	header *logbuffer.Header) term.ControlledPollAction {
	return dispatchEgress(a.clusterSessionId, buffer, offset, length, header, a.listener, a.extension, nil, nil,
		a.ErrorHandler)
}
