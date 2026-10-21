/*
Copyright 2016-2018 Stanislav Liberman
Copyright 2022 Steven Stern

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
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
)

// DirectBufferVector is a gather vector for Publisher.OfferVectors (io.aeron.DirectBufferVector).
type DirectBufferVector = term.DirectBufferVector

// BufferClaim is the zero copy claim type used by TryClaim (io.aeron.logbuffer.BufferClaim).
type BufferClaim = logbuffer.Claim

// Publisher is the API common to Publication (concurrent) and ExclusivePublication, i.e. the abstract
// io.aeron.Publication.
type Publisher interface {
	Channel() string
	StreamID() int32
	SessionID() int32
	InitialTermID() int32
	RegistrationID() int64
	OriginalRegistrationID() int64
	IsOriginal() bool

	TermBufferLength() int32
	MaxMessageLength() int32
	MaxPayloadLength() int32
	MaxPossiblePosition() int64
	PositionBitsToShift() int32

	IsConnected() bool
	IsClosed() bool
	IsRevoked() bool
	Close() error

	Position() int64
	PositionLimit() int64
	PositionLimitID() int32
	AvailableWindow() int64

	ChannelStatus() int
	ChannelStatusID() int32
	LocalSocketAddresses() []string
	AddDestination(endpointChannel string) error
	RemoveDestination(endpointChannel string) error
	AsyncAddDestination(endpointChannel string) (int64, error)
	AsyncRemoveDestination(endpointChannel string) (int64, error)

	Offer(buffer *atomic.Buffer, offset int32, length int32, reservedValueSupplier term.ReservedValueSupplier) int64
	Offer2(bufferOne *atomic.Buffer, offsetOne int32, lengthOne int32,
		bufferTwo *atomic.Buffer, offsetTwo int32, lengthTwo int32,
		reservedValueSupplier term.ReservedValueSupplier) int64
	OfferVectors(vectors []term.DirectBufferVector, reservedValueSupplier term.ReservedValueSupplier) int64
	TryClaim(length int32, bufferClaim *logbuffer.Claim) int64
}

var (
	_ Publisher = (*Publication)(nil)
	_ Publisher = (*ExclusivePublication)(nil)
)
