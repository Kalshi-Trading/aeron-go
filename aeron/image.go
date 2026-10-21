// Copyright 2016 Stanislav Liberman
// Copyright 2022 Steven Stern
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aeron

import "github.com/lirm/aeron-go/aeron/logbuffer/term"

// Image is a Java-style interface for the image struct.  This is to allow dependency injection and testing of
// the many structs that use image, without deviating from the existing function signatures and code structure.
type Image interface {
	// Poll delivers available fragments to the handler, up to fragmentLimit. Returns fragments consumed.
	Poll(handler term.FragmentHandler, fragmentLimit int) int
	// BoundedPoll is Poll limited to fragments before limitPosition.
	BoundedPoll(handler term.FragmentHandler, limitPosition int64, fragmentLimit int) int
	// ControlledPoll is Poll where the handler controls consumption with ABORT/BREAK/COMMIT/CONTINUE.
	ControlledPoll(handler term.ControlledFragmentHandler, fragmentLimit int) int
	// BoundedControlledPoll is ControlledPoll limited to fragments before limitPosition.
	BoundedControlledPoll(handler term.ControlledFragmentHandler, limitPosition int64, fragmentLimit int) int
	// ControlledPeek peeks fragments from initialPosition up to limitPosition without consuming them. Returns the
	// position after the last whole message peeked.
	ControlledPeek(initialPosition int64, handler term.ControlledFragmentHandler, limitPosition int64) (int64, error)
	// BlockPoll delivers a block of whole fragments of up to blockLengthLimit bytes. Returns bytes consumed.
	BlockPoll(handler term.BlockHandler, blockLengthLimit int32) int32
	// RawPoll delivers a block of whole fragments with its location in the log file. Returns bytes consumed.
	RawPoll(handler term.RawBlockHandler, blockLengthLimit int32) int32

	// Position returns the position this image has been consumed to by the subscriber.
	Position() int64
	// SetPosition sets the subscriber position after validating it is within the current term and aligned.
	SetPosition(newPosition int64) error
	// JoinPosition returns the position the subscriber joined the stream at.
	JoinPosition() int64
	// IsEndOfStream returns true if the image has been consumed up to the end of stream position.
	IsEndOfStream() bool
	// EndOfStreamPosition returns the end of stream position, or math.MaxInt64 if the publisher is open.
	EndOfStreamPosition() int64
	// IsPublicationRevoked returns true if the publication was revoked.
	IsPublicationRevoked() bool
	// IsClosed returns whether this image has been closed. No further operations are valid.
	IsClosed() bool

	SessionID() int32
	CorrelationID() int64
	SubscriptionRegistrationID() int64
	SubscriberPositionID() int32
	SourceIdentity() string
	TermBufferLength() int32
	MtuLength() int32
	InitialTermID() int32
	PositionBitsToShift() int32
	ActiveTransportCount() int32

	// Subscription returns the subscription the image belongs to.
	Subscription() *Subscription
	// Reject asks the driver to reject this image at its current position with the given reason.
	Reject(reason string) error
	// Close releases the image. Normally called by the client conductor after lingering.
	Close() error
}
