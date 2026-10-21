package rb

import "github.com/lirm/aeron-go/aeron/atomic"

// MessageHandler is called for each message read from a ring buffer (Agrona MessageHandler).
type MessageHandler func(msgTypeID int32, buffer *atomic.Buffer, index int32, length int32)

// ControlledAction is returned by a ControlledMessageHandler (Agrona ControlledMessageHandler.Action).
type ControlledAction int

const (
	// Abort the current read, the message will be redelivered on the next read.
	Abort ControlledAction = iota
	// Break from the current read after this message; its position is committed.
	Break
	// Commit the position up to and including this message and continue.
	Commit
	// Continue processing; the position is committed at the end of the read.
	Continue
)

func (a ControlledAction) String() string {
	switch a {
	case Abort:
		return "ABORT"
	case Break:
		return "BREAK"
	case Commit:
		return "COMMIT"
	case Continue:
		return "CONTINUE"
	}
	return "UNKNOWN"
}

// ControlledMessageHandler is called for each message in a controlled read.
type ControlledMessageHandler func(msgTypeID int32, buffer *atomic.Buffer, index int32, length int32) ControlledAction
