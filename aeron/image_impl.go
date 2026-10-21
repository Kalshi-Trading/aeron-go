/*
Copyright 2016 Stanislav Liberman

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aeron

import (
	"errors"
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/aeron/util"
)

// logIsPublicationRevokedOffset is LogBufferDescriptor.LOG_IS_PUBLICATION_REVOKED_OFFSET (Aeron 1.48+).
const logIsPublicationRevokedOffset int32 = 496

// endFragFlag is FrameDescriptor.END_FRAG_FLAG.
const endFragFlag uint8 = 0x40

// ErrImageClosed is returned by operations attempted on a closed Image.
var ErrImageClosed = errors.New("image is closed")

type image struct {
	sourceIdentity     string
	logFileName        string
	logBuffers         *logbuffer.LogBuffers
	termBuffers        [logbuffer.PartitionCount]*atomic.Buffer
	subscriberPosition Position
	header             logbuffer.Header
	subscription       *Subscription

	isClosed   atomic.Bool
	isClosing  atomic.Bool
	isReleased atomic.Bool
	isEos      bool
	isRevoked  bool

	termLengthMask             int32
	positionBitsToShift        uint8
	sessionID                  int32
	joinPosition               int64
	finalPosition              int64
	eosPosition                int64
	subscriptionRegistrationID int64
	correlationID              int64
}

// NewImage wraps around provided LogBuffers setting up the structures for polling
func NewImage(sessionID int32, correlationID int64, logBuffers *logbuffer.LogBuffers) *image {

	image := new(image)

	image.correlationID = correlationID
	image.sessionID = sessionID
	image.logBuffers = logBuffers
	for i := 0; i < logbuffer.PartitionCount; i++ {
		image.termBuffers[i] = logBuffers.Buffer(i)
	}
	capacity := logBuffers.Buffer(0).Capacity()
	image.termLengthMask = capacity - 1
	image.positionBitsToShift = util.NumberOfTrailingZeroes(uint32(capacity))
	image.header.SetInitialTermID(logBuffers.Meta().InitTermID.Get())
	image.header.SetPositionBitsToShift(int32(image.positionBitsToShift))
	image.header.SetContext(image)
	image.isClosed.Set(false)

	return image
}

// IsClosed returns whether this image has been closed. No further operations are valid.
func (image *image) IsClosed() bool {
	return image.isClosed.Get()
}

// Poll for new messages in a stream. If new messages are found beyond the last consumed position then they will be
// delivered to the FragmentHandler up to a limited number of fragments as specified. Use a FragmentAssembler to
// assemble messages which span multiple fragments. Returns the number of fragments that have been consumed.
//
//go:norace
func (image *image) Poll(handler term.FragmentHandler, fragmentLimit int) int {
	if image.isClosed.Get() {
		return 0
	}

	position := image.subscriberPosition.get()
	initialOffset := int32(position) & image.termLengthMask
	offset := initialOffset
	termBuffer := image.activeTermBuffer(position)
	capacity := termBuffer.Capacity()
	header := &image.header
	header.Wrap(termBuffer.Ptr(), capacity)

	completed := false
	defer func() {
		if !completed {
			image.advanceAfterPanic(position, position+int64(offset-initialOffset))
		}
	}()

	fragmentsRead := 0
	for fragmentsRead < fragmentLimit && offset < capacity && !image.isClosed.Get() {
		length := logbuffer.GetFrameLength(termBuffer, offset)
		if length <= 0 {
			break
		}

		frameOffset := offset
		offset += util.AlignInt32(length, logbuffer.FrameAlignment)

		if logbuffer.IsPaddingFrame(termBuffer, frameOffset) {
			continue
		}
		fragmentsRead++
		header.SetOffset(frameOffset)
		handler(termBuffer, frameOffset+logbuffer.DataFrameHeader.Length, length-logbuffer.DataFrameHeader.Length, header)
	}

	completed = true
	newPosition := position + int64(offset-initialOffset)
	if newPosition > position && !image.isClosed.Get() {
		image.subscriberPosition.setRelease(newPosition)
	}
	return fragmentsRead
}

// BoundedPoll polls for new messages in a stream. If new messages are found beyond the last consumed position then
// they will be delivered to the FragmentHandler up to a limited number of fragments as specified or the maximum
// position specified. Use a FragmentAssembler to assemble messages which span multiple fragments. Returns the
// number of fragments that have been consumed.
//
//go:norace
func (image *image) BoundedPoll(handler term.FragmentHandler, limitPosition int64, fragmentLimit int) int {
	if image.isClosed.Get() {
		return 0
	}

	initialPosition := image.subscriberPosition.get()
	if initialPosition >= limitPosition {
		return 0
	}
	initialOffset := int32(initialPosition) & image.termLengthMask
	offset := initialOffset
	termBuffer := image.activeTermBuffer(initialPosition)
	capacity := termBuffer.Capacity()
	limitOffset := boundedLimitOffset(capacity, limitPosition, initialPosition, initialOffset)
	header := &image.header
	header.Wrap(termBuffer.Ptr(), capacity)

	completed := false
	defer func() {
		if !completed {
			image.advanceAfterPanic(initialPosition, initialPosition+int64(offset-initialOffset))
		}
	}()

	fragmentsRead := 0
	for fragmentsRead < fragmentLimit && offset < limitOffset && !image.isClosed.Get() {
		length := logbuffer.GetFrameLength(termBuffer, offset)
		if length <= 0 {
			break
		}

		frameOffset := offset
		offset += util.AlignInt32(length, logbuffer.FrameAlignment)

		if logbuffer.IsPaddingFrame(termBuffer, frameOffset) {
			continue
		}
		fragmentsRead++
		header.SetOffset(frameOffset)
		handler(termBuffer, frameOffset+logbuffer.DataFrameHeader.Length, length-logbuffer.DataFrameHeader.Length, header)
	}

	completed = true
	resultingPosition := initialPosition + int64(offset-initialOffset)
	if resultingPosition > initialPosition && !image.isClosed.Get() {
		image.subscriberPosition.setRelease(resultingPosition)
	}
	return fragmentsRead
}

// ControlledPoll polls for new messages in a stream. If new messages are found beyond the last consumed position
// then they will be delivered to the ControlledFragmentHandler up to a limited number of fragments as specified.
// The handler's returned action (ABORT, BREAK, COMMIT, CONTINUE) controls consumption. Use a
// ControlledFragmentAssembler to assemble messages that span multiple fragments. Returns the number of fragments
// that have been consumed.
//
//go:norace
func (image *image) ControlledPoll(handler term.ControlledFragmentHandler, fragmentLimit int) int {
	if image.isClosed.Get() {
		return 0
	}
	initialPosition := image.subscriberPosition.get()
	termBuffer := image.activeTermBuffer(initialPosition)
	return image.controlledPoll(handler, termBuffer, initialPosition, termBuffer.Capacity(), fragmentLimit)
}

// BoundedControlledPoll is ControlledPoll with an upper bound on the position consumed to, like Java's
// Image.boundedControlledPoll. Returns the number of fragments that have been consumed.
//
//go:norace
func (image *image) BoundedControlledPoll(
	handler term.ControlledFragmentHandler,
	limitPosition int64,
	fragmentLimit int,
) int {
	if image.isClosed.Get() {
		return 0
	}
	initialPosition := image.subscriberPosition.get()
	if initialPosition >= limitPosition {
		return 0
	}
	initialOffset := int32(initialPosition) & image.termLengthMask
	termBuffer := image.activeTermBuffer(initialPosition)
	limitOffset := boundedLimitOffset(termBuffer.Capacity(), limitPosition, initialPosition, initialOffset)
	return image.controlledPoll(handler, termBuffer, initialPosition, limitOffset, fragmentLimit)
}

//go:norace
func (image *image) controlledPoll(
	handler term.ControlledFragmentHandler,
	termBuffer *atomic.Buffer,
	initialPosition int64,
	limitOffset int32,
	fragmentLimit int,
) int {
	initialOffset := int32(initialPosition) & image.termLengthMask
	offset := initialOffset
	header := &image.header
	header.Wrap(termBuffer.Ptr(), termBuffer.Capacity())

	completed := false
	defer func() {
		if !completed {
			image.advanceAfterPanic(initialPosition, initialPosition+int64(offset-initialOffset))
		}
	}()

	fragmentsRead := 0
	for fragmentsRead < fragmentLimit && offset < limitOffset && !image.isClosed.Get() {
		length := logbuffer.GetFrameLength(termBuffer, offset)
		if length <= 0 {
			break
		}

		frameOffset := offset
		alignedLength := util.AlignInt32(length, logbuffer.FrameAlignment)
		offset += alignedLength

		if logbuffer.IsPaddingFrame(termBuffer, frameOffset) {
			continue
		}
		fragmentsRead++
		header.SetOffset(frameOffset)

		action := handler(termBuffer, frameOffset+logbuffer.DataFrameHeader.Length,
			length-logbuffer.DataFrameHeader.Length, header)
		if action == term.ControlledPollActionAbort {
			fragmentsRead--
			offset -= alignedLength
			break
		}
		if action == term.ControlledPollActionBreak {
			break
		}
		if action == term.ControlledPollActionCommit {
			initialPosition += int64(offset - initialOffset)
			initialOffset = offset
			if !image.isClosed.Get() {
				image.subscriberPosition.setRelease(initialPosition)
			}
		}
	}

	completed = true
	resultingPosition := initialPosition + int64(offset-initialOffset)
	if resultingPosition > initialPosition && !image.isClosed.Get() {
		image.subscriberPosition.setRelease(resultingPosition)
	}
	return fragmentsRead
}

// advanceAfterPanic mirrors the finally block of Java's Image poll methods: when a handler panics, the position is
// still advanced past the fragments consumed, including the one that panicked, and the panic propagates (Java with
// RethrowingErrorHandler as the subscriber error handler, as used by the archive and cluster).
func (image *image) advanceAfterPanic(initialPosition int64, resultingPosition int64) {
	if resultingPosition > initialPosition && !image.isClosed.Get() {
		image.subscriberPosition.setRelease(resultingPosition)
	}
}

// ControlledPeek peeks for new messages from initialPosition up to limitPosition without moving the subscriber
// position. Fragments are delivered to the handler until it returns ABORT or BREAK, the limit is reached, or the end
// of the term. Returns the resulting position after the last complete message (END fragment) that was peeked,
// which may be passed to SetPosition to consume. ABORT does not advance past the fragment it was returned for.
// Returns an error if initialPosition is not a valid position (see SetPosition).
//
//go:norace
func (image *image) ControlledPeek(
	initialPosition int64,
	handler term.ControlledFragmentHandler,
	limitPosition int64,
) (int64, error) {
	if image.isClosed.Get() {
		return initialPosition, nil
	}
	if err := image.validatePosition(initialPosition); err != nil {
		return initialPosition, err
	}

	initialOffset := int32(initialPosition) & image.termLengthMask
	offset := initialOffset
	position := initialPosition
	termBuffer := image.activeTermBuffer(initialPosition)
	limitOffset := boundedLimitOffset(termBuffer.Capacity(), limitPosition, initialPosition, initialOffset)
	header := &image.header
	header.Wrap(termBuffer.Ptr(), termBuffer.Capacity())
	resultingPosition := initialPosition

	for offset < limitOffset && !image.isClosed.Get() {
		length := logbuffer.GetFrameLength(termBuffer, offset)
		if length <= 0 {
			break
		}

		frameOffset := offset
		offset += util.AlignInt32(length, logbuffer.FrameAlignment)

		if logbuffer.IsPaddingFrame(termBuffer, frameOffset) {
			position += int64(offset - initialOffset)
			initialOffset = offset
			resultingPosition = position
			continue
		}

		header.SetOffset(frameOffset)
		action := handler(termBuffer, frameOffset+logbuffer.DataFrameHeader.Length,
			length-logbuffer.DataFrameHeader.Length, header)
		if action == term.ControlledPollActionAbort {
			break
		}

		position += int64(offset - initialOffset)
		initialOffset = offset

		if header.Flags()&endFragFlag == endFragFlag {
			resultingPosition = position
		}

		if action == term.ControlledPollActionBreak {
			break
		}
	}

	return resultingPosition, nil
}

// BlockPoll polls for new messages in a stream, delivering a block of whole fragments (including frame headers)
// from a single term of up to blockLengthLimit bytes to the handler. Returns the number of bytes consumed.
//
//go:norace
func (image *image) BlockPoll(handler term.BlockHandler, blockLengthLimit int32) int32 {
	if image.isClosed.Get() {
		return 0
	}

	position := image.subscriberPosition.get()
	offset := int32(position) & image.termLengthMask
	limitOffset := blockLimitOffset(offset, blockLengthLimit, image.termLengthMask+1)
	termBuffer := image.activeTermBuffer(position)
	resultingOffset := term.ScanBlock(termBuffer, offset, limitOffset)
	length := resultingOffset - offset

	if resultingOffset > offset {
		defer image.advanceAfterPanic(position, position+int64(length))
		termID := termBuffer.GetInt32(offset + logbuffer.DataFrameHeader.TermIDFieldOffset)
		handler(termBuffer, offset, length, image.sessionID, termID)
	}
	return length
}

// RawPoll polls for new messages in a stream, delivering a block of whole fragments (including frame headers) from
// a single term of up to blockLengthLimit bytes to the handler along with the log file name and the offset of the
// block in that file, so the data can be transferred directly from the file. Returns the number of bytes consumed.
//
//go:norace
func (image *image) RawPoll(handler term.RawBlockHandler, blockLengthLimit int32) int32 {
	if image.isClosed.Get() {
		return 0
	}

	position := image.subscriberPosition.get()
	termOffset := int32(position) & image.termLengthMask
	activeIndex := indexByPosition(position, image.positionBitsToShift)
	termBuffer := image.termBuffers[activeIndex]
	capacity := termBuffer.Capacity()
	limitOffset := blockLimitOffset(termOffset, blockLengthLimit, capacity)
	resultingOffset := term.ScanBlock(termBuffer, termOffset, limitOffset)
	length := resultingOffset - termOffset

	if resultingOffset > termOffset {
		fileOffset := int64(capacity)*int64(activeIndex) + int64(termOffset)
		termID := termBuffer.GetInt32(termOffset + logbuffer.DataFrameHeader.TermIDFieldOffset)
		defer image.advanceAfterPanic(position, position+int64(length))
		handler(image.logFileName, fileOffset, termBuffer, termOffset, length, image.sessionID, termID)
	}
	return length
}

// Position returns the position this image has been consumed to by the subscriber.
func (image *image) Position() int64 {
	if image.isClosed.Get() {
		return image.finalPosition
	}
	return image.subscriberPosition.get()
}

// SetPosition sets the subscriber position for this image, e.g. after ControlledPeek. The position must be
// within the current term window [Position(), end of current term] and aligned to FrameAlignment, as validated by
// Java's Image.validatePosition. Returns ErrImageClosed if the image has been closed.
func (image *image) SetPosition(newPosition int64) error {
	if image.isClosed.Get() {
		return ErrImageClosed
	}
	if err := image.validatePosition(newPosition); err != nil {
		return err
	}
	image.subscriberPosition.setRelease(newPosition)
	return nil
}

func (image *image) validatePosition(position int64) error {
	currentPosition := image.subscriberPosition.get()
	termLengthMask := int64(image.termLengthMask)
	limitPosition := (currentPosition - (currentPosition & termLengthMask)) + termLengthMask + 1
	if position < currentPosition || position > limitPosition {
		return fmt.Errorf("%d position out of range: %d-%d", position, currentPosition, limitPosition)
	}
	if position&int64(logbuffer.FrameAlignment-1) != 0 {
		return fmt.Errorf("%d position not aligned to FRAME_ALIGNMENT", position)
	}
	return nil
}

// JoinPosition returns the position at which the subscriber joined the stream.
func (image *image) JoinPosition() int64 {
	return image.joinPosition
}

// IsEndOfStream returns true if the image has consumed to the end of stream position set by the publisher
// closing.
func (image *image) IsEndOfStream() bool {
	if image.isClosed.Get() {
		return image.isEos
	}
	return image.subscriberPosition.get() >= image.endOfStreamPositionVolatile()
}

// EndOfStreamPosition returns the position the stream reached when the publisher closed, or math.MaxInt64 when the
// publisher is still open.
func (image *image) EndOfStreamPosition() int64 {
	if image.isClosed.Get() {
		return image.eosPosition
	}
	return image.endOfStreamPositionVolatile()
}

func (image *image) endOfStreamPositionVolatile() int64 {
	return image.logBuffers.Buffer(logbuffer.LogMetaDataSectionIndex).GetInt64Volatile(
		logbuffer.LogEndOfStreamPositionOffset)
}

// IsPublicationRevoked returns true if the publication was revoked by the publisher (Aeron 1.48+).
func (image *image) IsPublicationRevoked() bool {
	if image.isClosed.Get() {
		return image.isRevoked
	}
	return image.readRevoked()
}

func (image *image) readRevoked() bool {
	return image.logBuffers.Buffer(logbuffer.LogMetaDataSectionIndex).GetUInt8(logIsPublicationRevokedOffset) == 1
}

// SessionID returns the sessionId for the steam of messages.
func (image *image) SessionID() int32 {
	return image.sessionID
}

// CorrelationID returns the correlationId for identification of the image with the media driver.
func (image *image) CorrelationID() int64 {
	return image.correlationID
}

// SubscriptionRegistrationID returns the registrationId for the Subscription of the image.
func (image *image) SubscriptionRegistrationID() int64 {
	return image.subscriptionRegistrationID
}

// SubscriberPositionID returns the counter id of the subscriber position counter for this image.
func (image *image) SubscriberPositionID() int32 {
	return image.subscriberPosition.id
}

// SourceIdentity returns the source identity of the sending publisher (e.g. "192.168.0.1:40123" or "aeron:ipc").
func (image *image) SourceIdentity() string {
	return image.sourceIdentity
}

// TermBufferLength returns the length in bytes for each term partition in the log buffer.
func (image *image) TermBufferLength() int32 {
	return image.termLengthMask + 1
}

// MtuLength returns the MTU length in bytes of the sending publication.
func (image *image) MtuLength() int32 {
	return image.logBuffers.Meta().MTULen.Get()
}

// InitialTermID returns the initial term id assigned when the publication was created.
func (image *image) InitialTermID() int32 {
	return image.header.InitialTermID()
}

// PositionBitsToShift returns the number of bits to right shift a position to get a term count.
func (image *image) PositionBitsToShift() int32 {
	return int32(image.positionBitsToShift)
}

// ActiveTransportCount returns the number of observed active transports within the image liveness timeout.
//
// Returns 0 if the image is closed, if no datagrams have arrived or the image is IPC
func (image *image) ActiveTransportCount() int32 {
	if image.isClosed.Get() {
		return 0
	}
	return image.logBuffers.Meta().ActiveTransportCount()
}

// Subscription returns the Subscription this image belongs to, or nil if not yet added to one.
func (image *image) Subscription() *Subscription {
	return image.subscription
}

// Reject this image at its current position with the given reason, which the driver passes on to the publisher
// (Aeron 1.47+). The image will be removed and may later rejoin. Not supported for spy subscriptions.
func (image *image) Reject(reason string) error {
	if image.subscription == nil {
		return errors.New("image has no subscription")
	}
	return image.subscription.RejectImage(image.correlationID, image.Position(), reason)
}

// Close marks the image as closed and releases its log buffer mappings. The image becomes unusable after closing.
func (image *image) Close() error {
	image.markClosed()
	var err error
	if image.isReleased.CompareAndSet(false, true) {
		logger.Debugf("Closing %v", image)
		err = image.logBuffers.Close()
	}
	return err
}

// markClosed captures the final state of the image and marks it as closed without releasing the log buffers, as
// Java's package-private Image.close(). Called when the image is removed from its subscription so that pollers
// stop using it before the mappings are released after the linger period.
func (image *image) markClosed() {
	if !image.isClosing.CompareAndSet(false, true) {
		return
	}
	image.finalPosition = image.subscriberPosition.getVolatile()
	image.eosPosition = image.endOfStreamPositionVolatile()
	image.isEos = image.finalPosition >= image.eosPosition
	image.isRevoked = image.readRevoked()
	image.isClosed.Set(true)
}

// String returns a description of the image, as Java's Image.toString.
func (image *image) String() string {
	regID := image.subscriptionRegistrationID
	if image.subscription != nil {
		regID = image.subscription.registrationID
	}
	return fmt.Sprintf("Image{correlationId=%d, sessionId=%d, isClosed=%t, isEos=%t, initialTermId=%d, termLength=%d, "+
		"joinPosition=%d, position=%d, endOfStreamPosition=%d, activeTransportCount=%d, sourceIdentity='%s', "+
		"subscriptionRegistrationId=%d}",
		image.correlationID, image.sessionID, image.IsClosed(), image.IsEndOfStream(), image.InitialTermID(),
		image.TermBufferLength(), image.joinPosition, image.Position(), image.EndOfStreamPosition(),
		image.ActiveTransportCount(), image.sourceIdentity, regID)
}

//go:norace
func (image *image) activeTermBuffer(position int64) *atomic.Buffer {
	return image.termBuffers[indexByPosition(position, image.positionBitsToShift)]
}

func boundedLimitOffset(capacity int32, limitPosition int64, initialPosition int64, initialOffset int32) int32 {
	limit := limitPosition - initialPosition + int64(initialOffset)
	if limit > int64(capacity) {
		return capacity
	}
	if limit < int64(initialOffset) {
		return initialOffset
	}
	return int32(limit)
}

func blockLimitOffset(offset int32, blockLengthLimit int32, capacity int32) int32 {
	limit := int64(offset) + int64(blockLengthLimit)
	if limit > int64(capacity) {
		return capacity
	}
	return int32(limit)
}

func indexByPosition(position int64, positionBitsToShift uint8) int32 {
	term := uint64(position) >> positionBitsToShift
	return util.FastMod3(term)
}
