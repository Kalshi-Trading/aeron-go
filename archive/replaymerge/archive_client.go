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

package replaymerge

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
)

// messageRetryAttempts mirrors AeronArchive.Configuration.MESSAGE_RETRY_ATTEMPTS_DEFAULT: ReplayMerge requests are
// offered a bounded number of times so the duty cycle is never blocked for the archive's full message timeout.
const messageRetryAttempts = 3

// mergeSubscription is the subset of *aeron.Subscription used by ReplayMerge.
type mergeSubscription interface {
	Channel() string
	StreamID() int32
	IsConnected() bool
	ImageBySessionID(sessionID int32) aeron.Image
	ResolvedEndpoint() string
	AddDestination(endpointChannel string) error
	RemoveDestination(endpointChannel string) error
	AsyncAddDestination(endpointChannel string) (int64, error)
	AsyncRemoveDestination(endpointChannel string) (int64, error)
}

// mergeArchive is the subset of archive client operations used by ReplayMerge. The send methods are non-blocking
// beyond a few retries and return false if the request could not be offered, like Java's ArchiveProxy.
type mergeArchive interface {
	isAeronClosed() bool
	isRequestConnected() bool
	isResponseConnected() bool
	nextCorrelationID() int64
	controlSessionID() int64
	maxRecordedPosition(correlationID, recordingID int64) (bool, error)
	replay(correlationID, recordingID, position, length int64, replayChannel string, replayStreamID int32) (bool, error)
	stopReplay(correlationID, replaySessionID int64) (bool, error)
	// pollResponse polls the control response stream once, returning the fragment count and the decoded control
	// response if one has completed.
	pollResponse() (int, *codecs.ControlResponse)
}

type archiveClient struct {
	archive    *archive.Archive
	marshaller *codecs.SbeGoMarshaller
}

func newArchiveClient(a *archive.Archive) *archiveClient {
	return &archiveClient{archive: a, marshaller: codecs.NewSbeGoMarshaller()}
}

func (c *archiveClient) isAeronClosed() bool {
	return c.archive.Aeron() == nil || c.archive.Aeron().IsClosed()
}

func (c *archiveClient) isRequestConnected() bool {
	return c.archive.Proxy.Publication != nil && c.archive.Proxy.Publication.IsConnected()
}

func (c *archiveClient) isResponseConnected() bool {
	return c.archive.Control.Subscription != nil && c.archive.Control.Subscription.IsConnected()
}

func (c *archiveClient) nextCorrelationID() int64 {
	return c.archive.Aeron().NextCorrelationID()
}

func (c *archiveClient) controlSessionID() int64 {
	return c.archive.SessionID
}

func (c *archiveClient) maxRecordedPosition(correlationID, recordingID int64) (bool, error) {
	request := codecs.MaxRecordedPositionRequest{
		ControlSessionId: c.archive.SessionID,
		CorrelationId:    correlationID,
		RecordingId:      recordingID,
	}
	return c.send(&request)
}

func (c *archiveClient) replay(
	correlationID, recordingID, position, length int64, replayChannel string, replayStreamID int32) (bool, error) {
	request := codecs.ReplayRequest{
		ControlSessionId: c.archive.SessionID,
		CorrelationId:    correlationID,
		RecordingId:      recordingID,
		Position:         position,
		Length:           length,
		ReplayStreamId:   replayStreamID,
		FileIoMaxLength:  aeron.NullValue,
		ReplayToken:      aeron.NullValue,
		ReplayChannel:    []uint8(replayChannel),
	}
	return c.send(&request)
}

func (c *archiveClient) stopReplay(correlationID, replaySessionID int64) (bool, error) {
	request := codecs.StopReplayRequest{
		ControlSessionId: c.archive.SessionID,
		CorrelationId:    correlationID,
		ReplaySessionId:  replaySessionID,
	}
	return c.send(&request)
}

func (c *archiveClient) pollResponse() (int, *codecs.ControlResponse) {
	control := c.archive.Control
	count := control.Poll()
	if control.Results.IsPollComplete && control.Results.ControlResponse != nil {
		return count, control.Results.ControlResponse
	}
	return count, nil
}

type sbeRequest interface {
	SbeBlockLength() uint16
	SbeTemplateId() uint16
	SbeSchemaId() uint16
	SbeSchemaVersion() uint16
}

func (c *archiveClient) send(request sbeRequest) (bool, error) {
	buffer := new(bytes.Buffer)
	header := codecs.MessageHeader{
		BlockLength: request.SbeBlockLength(),
		TemplateId:  request.SbeTemplateId(),
		SchemaId:    request.SbeSchemaId(),
		Version:     request.SbeSchemaVersion(),
	}
	if err := header.Encode(c.marshaller, buffer); err != nil {
		return false, err
	}
	rangeChecking := c.archive.Options.RangeChecking
	var err error
	switch r := request.(type) {
	case *codecs.MaxRecordedPositionRequest:
		err = r.Encode(c.marshaller, buffer, rangeChecking)
	case *codecs.ReplayRequest:
		err = r.Encode(c.marshaller, buffer, rangeChecking)
	case *codecs.StopReplayRequest:
		err = r.Encode(c.marshaller, buffer, rangeChecking)
	default:
		err = fmt.Errorf("unsupported request type %T", request)
	}
	if err != nil {
		return false, err
	}
	b := buffer.Bytes()
	return offer(c.archive.Proxy.Publication, atomic.MakeBuffer(b, len(b)), int32(len(b)))
}

// ErrArchiveConnectionClosed is returned when the archive control request publication is closed.
var ErrArchiveConnectionClosed = errors.New("connection to the archive has been closed")

// ErrArchiveConnectionUnavailable is returned when the archive control request publication is not connected.
var ErrArchiveConnectionUnavailable = errors.New("connection to the archive is no longer available")

func offer(publication aeron.Publisher, buffer *atomic.Buffer, length int32) (bool, error) {
	if publication == nil {
		return false, ErrArchiveConnectionClosed
	}
	for attempts := messageRetryAttempts; ; {
		position := publication.Offer(buffer, 0, length, nil)
		switch {
		case position > 0:
			return true, nil
		case position == aeron.PublicationClosed:
			return false, ErrArchiveConnectionClosed
		case position == aeron.NotConnected:
			return false, ErrArchiveConnectionUnavailable
		case position == aeron.MaxPositionExceeded:
			return false, errors.New("offer failed due to max position being reached")
		}
		attempts--
		if attempts <= 0 {
			return false, nil
		}
		runtime.Gosched()
	}
}
