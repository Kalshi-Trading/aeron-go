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

package broadcast

import (
	"errors"
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
)

// Handler is called with (msgTypeID, buffer, offset, length) for each received message.
type Handler func(int32, *atomic.Buffer, int32, int32)

// ScratchBufferLength is the default scratch buffer size of a CopyReceiver.
const ScratchBufferLength = 4096

// ErrUnableToKeepUp is returned (or panicked with, from Receive) when the receiver was lapped by the transmitter.
var ErrUnableToKeepUp = errors.New("unable to keep up with broadcast")

// CopyReceiver copies each broadcast message into a scratch buffer and validates it before handing it to
// the handler, so handlers never observe a message being overwritten (Agrona CopyBroadcastReceiver).
type CopyReceiver struct {
	receiver      *Receiver
	scratchBuffer *atomic.Buffer
}

// NewCopyReceiver creates a CopyReceiver with a ScratchBufferLength scratch buffer. Any messages already
// in the buffer are skipped so only messages transmitted after creation are delivered.
func NewCopyReceiver(receiver *Receiver) *CopyReceiver {
	bcast := NewCopyReceiverWithScratch(receiver, atomic.MakeBuffer(make([]byte, ScratchBufferLength)))
	for bcast.receiver.ReceiveNext() {
	}
	return bcast
}

// NewCopyReceiverWithScratch creates a CopyReceiver using the given scratch buffer and starts from the
// receiver's current position.
func NewCopyReceiverWithScratch(receiver *Receiver, scratchBuffer *atomic.Buffer) *CopyReceiver {
	return &CopyReceiver{receiver: receiver, scratchBuffer: scratchBuffer}
}

// BroadcastReceiver returns the underlying Receiver.
func (bcast *CopyReceiver) BroadcastReceiver() *Receiver { return bcast.receiver }

// Receive delivers at most one message to handler and returns the number delivered. It panics with
// ErrUnableToKeepUp if the receiver was lapped, like Agrona's IllegalStateException; use TryReceive for
// an error instead.
func (bcast *CopyReceiver) Receive(handler Handler) int {
	n, err := bcast.TryReceive(handler)
	if err != nil {
		panic(err)
	}
	return n
}

// TryReceive delivers at most one message to handler and returns the number delivered, or an error
// wrapping ErrUnableToKeepUp if the receiver was lapped or the message exceeds the scratch buffer.
func (bcast *CopyReceiver) TryReceive(handler Handler) (int, error) {
	receiver := bcast.receiver
	lastSeenLappedCount := receiver.LappedCount()

	if !receiver.ReceiveNext() {
		return 0, nil
	}
	if lastSeenLappedCount != receiver.LappedCount() {
		return 0, ErrUnableToKeepUp
	}

	length := receiver.Length()
	if length > bcast.scratchBuffer.Capacity() {
		return 0, fmt.Errorf("buffer required length of %d but only has %d", length, bcast.scratchBuffer.Capacity())
	}

	msgTypeID := receiver.TypeID()
	bcast.scratchBuffer.PutBytes(0, receiver.Buffer(), receiver.Offset(), length)

	if !receiver.Validate() {
		return 0, ErrUnableToKeepUp
	}

	handler(msgTypeID, bcast.scratchBuffer, 0, length)
	return 1, nil
}
