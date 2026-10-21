package mm

import (
	"fmt"
	"math/rand"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
)

// Go consumer kinds. Both reassemble fragmented messages; every raw fragment is position checked first.
const (
	SubPoll       = "poll"       // Subscription.Poll + FragmentAssembler
	SubControlled = "controlled" // Subscription.ControlledPoll + ControlledFragmentAssembler, random ABORT/BREAK/COMMIT
)

// Sub is a Go consumer of one kind feeding a Tracker.
type Sub struct {
	Kind    string
	Sub     *aeron.Subscription
	Tracker *Tracker
	poll    func() int
}

// AddSub creates a subscription of the given kind whose image handlers feed the tracker. Destinations are added
// for multi-destination (control-mode=manual) channels.
func AddSub(a *aeron.Aeron, kind, channel string, stream int32, rnd *rand.Rand, allowGaps bool,
	dests ...string) (*Sub, error) {
	t := NewTracker(allowGaps)
	sub, err := a.AddSubscriptionWithHandlers(channel, stream, t.OnAvailable, t.OnUnavailable)
	if err != nil {
		return nil, err
	}
	for _, d := range dests {
		if err := sub.AddDestination(d); err != nil {
			_ = sub.Close()
			return nil, err
		}
	}
	s := &Sub{Kind: kind, Sub: sub, Tracker: t}
	switch kind {
	case SubPoll:
		asm := aeron.NewFragmentAssembler(t.Message, 4096)
		handler := func(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) {
			t.Fragment(header)
			asm.OnFragment(buffer, offset, length, header)
		}
		s.poll = func() int { return sub.Poll(handler, 32) }
	case SubControlled:
		asm := aeron.NewControlledFragmentAssembler(
			func(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) term.ControlledPollAction {
				switch r := rnd.Intn(100); {
				case r < 5:
					return term.ControlledPollActionAbort
				case r < 10:
					t.Message(buffer, offset, length, header)
					return term.ControlledPollActionBreak
				case r < 15:
					t.Message(buffer, offset, length, header)
					return term.ControlledPollActionCommit
				}
				t.Message(buffer, offset, length, header)
				return term.ControlledPollActionContinue
			}, 4096)
		handler := func(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) term.ControlledPollAction {
			t.Fragment(header)
			action := asm.OnFragment(buffer, offset, length, header)
			if action == term.ControlledPollActionAbort {
				t.Unconsume(header)
			}
			return action
		}
		s.poll = func() int { return sub.ControlledPoll(handler, 32) }
	default:
		_ = sub.Close()
		return nil, fmt.Errorf("unknown consumer kind %q", kind)
	}
	return s, nil
}

// Poll polls once.
func (s *Sub) Poll() int { return s.poll() }

// Close closes the subscription.
func (s *Sub) Close() error { return s.Sub.Close() }
