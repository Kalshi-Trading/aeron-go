// Copyright 2026 Kalshi Trading.
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

package archive

import (
	"bytes"
	"errors"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/archive/codecs"
)

// ErrProxyNotConnected is returned by the non-blocking Try* proxy methods when the request publication is not
// connected, closed, or has reached its maximum position. The archive session should be considered lost.
var ErrProxyNotConnected = errors.New("archive request publication not connected")

// tryOfferRetryAttempts mirrors Java's ArchiveProxy default retry attempts for back pressure / admin action.
const tryOfferRetryAttempts = 3

// NewProxyForPublication creates a Proxy sharing this archive's control session that sends requests on the given
// publication. Used for response-channel replays, where the replay request must be sent on a dedicated request
// publication whose response-correlation-id links it to the replay subscription.
func (archive *Archive) NewProxyForPublication(publication *aeron.Publication) *Proxy {
	return &Proxy{Publication: publication, archive: archive, marshaller: codecs.NewSbeGoMarshaller()}
}

// tryOffer offers the encoded request once (with a small bounded retry on back pressure) without blocking.
// Returns (false, nil) when back pressured and (false, ErrProxyNotConnected) when the publication is unusable.
func (proxy *Proxy) tryOffer(packet []byte) (bool, error) {
	buffer := atomic.MakeBuffer(packet, len(packet))
	for i := 0; i < tryOfferRetryAttempts; i++ {
		ret := proxy.Publication.Offer(buffer, 0, int32(len(packet)), nil)
		switch {
		case ret > 0:
			return true, nil
		case ret == aeron.NotConnected || ret == aeron.PublicationClosed || ret == aeron.MaxPositionExceeded:
			return false, ErrProxyNotConnected
		}
	}
	return false, nil
}

func encodePacket(marshaller *codecs.SbeGoMarshaller, rangeChecking bool, request interface {
	SbeBlockLength() uint16
	SbeTemplateId() uint16
	SbeSchemaId() uint16
	SbeSchemaVersion() uint16
}, encode func(*bytes.Buffer) error) ([]byte, error) {
	header := codecs.MessageHeader{
		BlockLength: request.SbeBlockLength(),
		TemplateId:  request.SbeTemplateId(),
		SchemaId:    request.SbeSchemaId(),
		Version:     request.SbeSchemaVersion(),
	}
	buffer := new(bytes.Buffer)
	if err := header.Encode(marshaller, buffer); err != nil {
		return nil, err
	}
	if err := encode(buffer); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// TryListRecordingRequest is a non-blocking ListRecordingRequest. Returns true if the request was sent.
func (proxy *Proxy) TryListRecordingRequest(correlationID int64, recordingID int64) (bool, error) {
	packet, err := codecs.ListRecordingRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking,
		proxy.archive.SessionID, correlationID, recordingID)
	if err != nil {
		return false, err
	}
	return proxy.tryOffer(packet)
}

// TryStopReplayRequest is a non-blocking StopReplayRequest. Returns true if the request was sent.
func (proxy *Proxy) TryStopReplayRequest(correlationID int64, replaySessionID int64) (bool, error) {
	packet, err := codecs.StopReplayRequestPacket(proxy.marshaller, proxy.archive.Options.RangeChecking,
		proxy.archive.SessionID, correlationID, replaySessionID)
	if err != nil {
		return false, err
	}
	return proxy.tryOffer(packet)
}

// TryMaxRecordedPositionRequest is a non-blocking MaxRecordedPositionRequest. The OK response's relevantId is the
// maximum recorded position for the recording (which, unlike the recording position, is also defined for stopped
// recordings). Returns true if the request was sent.
func (proxy *Proxy) TryMaxRecordedPositionRequest(correlationID int64, recordingID int64) (bool, error) {
	request := codecs.MaxRecordedPositionRequest{
		ControlSessionId: proxy.archive.SessionID,
		CorrelationId:    correlationID,
		RecordingId:      recordingID,
	}
	packet, err := encodePacket(proxy.marshaller, proxy.archive.Options.RangeChecking, &request,
		func(b *bytes.Buffer) error {
			return request.Encode(proxy.marshaller, b, proxy.archive.Options.RangeChecking)
		})
	if err != nil {
		return false, err
	}
	return proxy.tryOffer(packet)
}

// TryReplayTokenRequest is a non-blocking ReplayTokenRequest, used to obtain the token for a response-channel replay.
// The OK response's relevantId is the token. Returns true if the request was sent.
func (proxy *Proxy) TryReplayTokenRequest(correlationID int64, recordingID int64) (bool, error) {
	request := codecs.ReplayTokenRequest{
		ControlSessionId: proxy.archive.SessionID,
		CorrelationId:    correlationID,
		RecordingId:      recordingID,
	}
	packet, err := encodePacket(proxy.marshaller, proxy.archive.Options.RangeChecking, &request,
		func(b *bytes.Buffer) error {
			return request.Encode(proxy.marshaller, b, proxy.archive.Options.RangeChecking)
		})
	if err != nil {
		return false, err
	}
	return proxy.tryOffer(packet)
}

// TryReplayRequestWithToken is a non-blocking ReplayRequest that also carries fileIoMaxLength and replayToken
// (use -1 / aeron.NullValue for either to leave them unset). Returns true if the request was sent.
func (proxy *Proxy) TryReplayRequestWithToken(correlationID int64, recordingID int64, position int64, length int64,
	replayStream int32, replayChannel string, fileIoMaxLength int32, replayToken int64) (bool, error) {
	request := codecs.ReplayRequest{
		ControlSessionId: proxy.archive.SessionID,
		CorrelationId:    correlationID,
		RecordingId:      recordingID,
		Position:         position,
		Length:           length,
		ReplayStreamId:   replayStream,
		FileIoMaxLength:  fileIoMaxLength,
		ReplayToken:      replayToken,
		ReplayChannel:    []uint8(replayChannel),
	}
	packet, err := encodePacket(proxy.marshaller, proxy.archive.Options.RangeChecking, &request,
		func(b *bytes.Buffer) error {
			return request.Encode(proxy.marshaller, b, proxy.archive.Options.RangeChecking)
		})
	if err != nil {
		return false, err
	}
	return proxy.tryOffer(packet)
}
