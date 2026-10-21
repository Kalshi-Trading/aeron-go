package aeron

import (
	"time"

	"github.com/lirm/aeron-go/aeron/idlestrategy"
)

// awaitingIdleStrategy is used by blocking calls waiting on the conductor (Java
// Aeron.Context.awaitingIdleStrategy, 1ms sleep). It must be stateless: it is shared by all callers,
// unlike Context.IdleStrategy which belongs to the conductor goroutine.
var awaitingIdleStrategy idlestrategy.Idler = idlestrategy.Sleeping{SleepFor: time.Millisecond}
