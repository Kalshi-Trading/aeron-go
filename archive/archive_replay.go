// Copyright (C) 2021-2022 Talos, Inc.
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
	"fmt"
	"strconv"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/archive/codecs"
)

// Sentinel values matching io.aeron.archive.client.AeronArchive.
const (
	NullValue          = int64(-1) // Aeron.NULL_VALUE
	NullPosition       = int64(-1) // AeronArchive.NULL_POSITION: no position / recording not active
	NullLength         = int64(-1) // AeronArchive.NULL_LENGTH
	ReplayAllAndFollow = int64(-1) // AeronArchive.REPLAY_ALL_AND_FOLLOW: replay and follow a live recording
	ReplayAllAndStop   = int64(-2) // AeronArchive.REPLAY_ALL_AND_STOP: replay up to the stop/limit position then stop
)

// replayRequestTermLength mirrors AeronArchive.Context controlTermBufferLength default (64KiB).
const replayRequestTermLength = 64 * 1024

// ReplayParams contains the optional parameters for a replay, equivalent to
// io.aeron.archive.client.ReplayParams. Use NewReplayParams() for the defaults.
type ReplayParams struct {
	// Position to start the replay from, NullPosition replays from the start of the recording.
	Position int64
	// Length to replay, ReplayAllAndFollow (default) follows a live recording, ReplayAllAndStop stops at the
	// current stop/limit position.
	Length int64
	// BoundingLimitCounterID, if not NullValue, bounds the replay by the value of this counter (bounded replay).
	BoundingLimitCounterID int32
	// FileIoMaxLength is the maximum file read size for the replay, NullValue uses the archive default.
	// Must be at least the MTU of the replay channel.
	FileIoMaxLength int32
	// ReplayToken authorises a replay over a response channel; set by the client when replaying via a
	// response channel.
	ReplayToken int64
	// SubscriptionRegistrationID of a subscription on a response channel (control-mode=response). Required for
	// StartReplayWithParams over a response channel.
	SubscriptionRegistrationID int64
}

// NewReplayParams returns ReplayParams with the default (Java ReplayParams.reset()) values.
func NewReplayParams() *ReplayParams {
	params := &ReplayParams{}
	params.Reset()
	return params
}

// Reset to the default values.
func (p *ReplayParams) Reset() *ReplayParams {
	p.Position = NullPosition
	p.Length = ReplayAllAndFollow
	p.BoundingLimitCounterID = int32(NullValue)
	p.FileIoMaxLength = int32(NullValue)
	p.ReplayToken = NullValue
	p.SubscriptionRegistrationID = NullValue
	return p
}

// IsBounded is true if a BoundingLimitCounterID is set.
func (p *ReplayParams) IsBounded() bool {
	return p.BoundingLimitCounterID != int32(NullValue)
}

func (p ReplayParams) String() string {
	return fmt.Sprintf("ReplayParams{position=%d, length=%d, boundingLimitCounterId=%d, fileIoMaxLength=%d, replayToken=%d, subscriptionRegistrationId=%d}",
		p.Position, p.Length, p.BoundingLimitCounterID, p.FileIoMaxLength, p.ReplayToken, p.SubscriptionRegistrationID)
}

// IsResponseChannel is true if the channel uses control-mode=response.
func IsResponseChannel(channel string) bool {
	uri, err := aeron.ParseChannelUri(channel)
	if err != nil {
		return false
	}
	return uri.Get("control-mode") == "response"
}

// buildReplayRequestChannel builds the request channel used to send a replay request on a dedicated
// publication so that the archive can respond on the response channel identified by responseCorrelationID.
func buildReplayRequestChannel(controlRequestChannel string, responseCorrelationID int64) (string, error) {
	uri, err := aeron.ParseChannelUri(controlRequestChannel)
	if err != nil {
		return "", err
	}
	uri.Remove("session-id")
	uri.Remove("term-id")
	uri.Remove("init-term-id")
	uri.Remove("term-offset")
	uri.Set("response-correlation-id", strconv.FormatInt(responseCorrelationID, 10))
	uri.Set("term-length", strconv.Itoa(replayRequestTermLength))
	uri.Set("ssc", "false")
	return uri.String(), nil
}

// requestLocked sends a request built by send and polls for its response. archive.mtx must be held.
// Error responses are returned as *ArchiveError.
func (archive *Archive) requestLocked(correlationID int64, send func(correlationID int64) error) (int64, error) {
	correlations.Store(correlationID, archive.Control) // For subsequent lookup in the fragment assemblers
	defer correlations.Delete(correlationID)

	if err := send(correlationID); err != nil {
		return 0, err
	}
	return archive.pollForResponseLocked(correlationID)
}

func (archive *Archive) pollForResponseLocked(correlationID int64) (int64, error) {
	relevantID, err := archive.Control.PollForResponse(correlationID, archive.SessionID)
	if err != nil {
		if r := archive.Control.Results.ControlResponse; r != nil && archive.Control.Results.IsPollComplete &&
			r.CorrelationId == correlationID && r.Code == codecs.ControlResponseCode.ERROR {
			return relevantID, newArchiveResponseException(string(r.ErrorMessage), r.RelevantId, correlationID)
		}
		return relevantID, err
	}
	return relevantID, nil
}

func (archive *Archive) request(send func(correlationID int64) error) (int64, error) {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()
	return archive.requestLocked(nextCorrelationID(), send)
}

// StartReplayWithParams starts a replay of recordingID to replayChannel/replayStream as configured by params
// (nil for defaults). A bounded replay is requested if params.BoundingLimitCounterID is set.
//
// If replayChannel is a response channel (control-mode=response) then params.SubscriptionRegistrationID must
// be the registration id of a subscription the caller has already added on replayChannel/replayStream; a replay
// token is requested and params.ReplayToken is updated.
//
// Returns the replay session id; the lower 32 bits are the session id of the replay image.
func (archive *Archive) StartReplayWithParams(recordingID int64, replayChannel string, replayStream int32, params *ReplayParams) (int64, error) {
	if params == nil {
		params = NewReplayParams()
	}
	logger.Debugf("StartReplayWithParams(%d, %s:%d, %s)", recordingID, replayChannel, replayStream, params)

	if IsResponseChannel(replayChannel) {
		if params.SubscriptionRegistrationID == NullValue {
			return 0, fmt.Errorf("when using StartReplay with a response channel, ReplayParams.SubscriptionRegistrationID must be set")
		}
		archive.mtx.Lock()
		defer archive.mtx.Unlock()
		return archive.replayViaResponseChannelLocked(recordingID, replayChannel, replayStream, params, params.SubscriptionRegistrationID)
	}

	return archive.request(func(correlationID int64) error {
		return archive.Proxy.ReplayRequestWithParams(correlationID, recordingID, replayChannel, replayStream, params)
	})
}

// Replay a recording from position for length to replayChannel/replayStream and return a subscription for it,
// equivalent to AeronArchive.replay(recordingId, position, length, channel, streamId).
func (archive *Archive) Replay(recordingID int64, position int64, length int64, replayChannel string, replayStream int32) (*aeron.Subscription, error) {
	params := NewReplayParams()
	params.Position = position
	params.Length = length
	return archive.ReplayWithParams(recordingID, replayChannel, replayStream, params)
}

// ReplayWithParams starts a replay as configured by params (nil for defaults) and returns a subscription to it.
//
// For normal channels the subscription is added with the replay session id once the replay has started.
// For response channels (control-mode=response) the subscription is added first, a replay token is requested
// (updating params.ReplayToken) and the call waits for the subscription to be connected to the replay.
// The caller owns the returned subscription.
func (archive *Archive) ReplayWithParams(recordingID int64, replayChannel string, replayStream int32, params *ReplayParams) (*aeron.Subscription, error) {
	if params == nil {
		params = NewReplayParams()
	}
	logger.Debugf("ReplayWithParams(%d, %s:%d, %s)", recordingID, replayChannel, replayStream, params)

	if IsResponseChannel(replayChannel) {
		archive.mtx.Lock()
		defer archive.mtx.Unlock()
		subscription, err := archive.aeron.AddSubscription(replayChannel, replayStream)
		if err != nil {
			return nil, err
		}
		if _, err := archive.replayViaResponseChannelLocked(recordingID, replayChannel, replayStream, params, subscription.RegistrationID()); err != nil {
			subscription.Close()
			return nil, err
		}
		if err := archive.awaitLocked("replay subscription to connect", subscription.IsConnected); err != nil {
			subscription.Close()
			return nil, err
		}
		return subscription, nil
	}

	uri, err := aeron.ParseChannelUri(replayChannel)
	if err != nil {
		return nil, err
	}
	replaySessionID, err := archive.StartReplayWithParams(recordingID, replayChannel, replayStream, params)
	if err != nil {
		return nil, err
	}
	uri.SetSessionID(int32(replaySessionID))
	return archive.aeron.AddSubscription(uri.String(), replayStream)
}

func (archive *Archive) awaitLocked(what string, done func() bool) error {
	deadline := time.Now().Add(archive.Options.Timeout)
	for !done() {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s", what)
		}
		archive.Options.IdleStrategy.Idle(0)
	}
	return nil
}

// replayViaResponseChannelLocked follows AeronArchive.startReplayViaResponseChannel: request a replay token,
// then send the replay request over a dedicated publication whose response-correlation-id ties the archive's
// replay publication to the response subscription. archive.mtx must be held.
func (archive *Archive) replayViaResponseChannelLocked(recordingID int64, replayChannel string, replayStream int32, params *ReplayParams, subscriptionRegistrationID int64) (int64, error) {
	correlationID := nextCorrelationID()
	correlations.Store(correlationID, archive.Control)
	defer correlations.Delete(correlationID)

	if err := archive.Proxy.ReplayTokenRequest(correlationID, recordingID); err != nil {
		return 0, err
	}
	replayToken, err := archive.pollForResponseLocked(correlationID)
	if err != nil {
		return 0, err
	}
	params.ReplayToken = replayToken

	requestChannel, err := buildReplayRequestChannel(archive.Options.RequestChannel, subscriptionRegistrationID)
	if err != nil {
		return 0, err
	}
	publication, err := archive.aeron.AddExclusivePublication(requestChannel, archive.Options.RequestStream)
	if err != nil {
		return 0, err
	}
	defer publication.Close()

	if err := archive.awaitLocked("replay request publication to connect", publication.IsConnected); err != nil {
		return 0, err
	}
	if err := archive.awaitLocked("replay request publication to have a publication limit", func() bool {
		return archive.publicationLimit(publication) > 0
	}); err != nil {
		return 0, err
	}

	responseProxy := &Proxy{Publication: publication, archive: archive, marshaller: codecs.NewSbeGoMarshaller()}
	if err := responseProxy.ReplayRequestWithParams(correlationID, recordingID, replayChannel, replayStream, params); err != nil {
		return 0, err
	}
	return archive.pollForResponseLocked(correlationID)
}

// driverPublisherLimitTypeID is AeronCounters.DRIVER_PUBLISHER_LIMIT_TYPE_ID.
const driverPublisherLimitTypeID = 1

// publicationLimit returns the value of the publisher limit counter of the publication, or -1 if not found.
func (archive *Archive) publicationLimit(publication aeron.Publisher) int64 {
	reader := archive.aeron.CounterReader()
	limit := int64(-1)
	reader.ScanForType(driverPublisherLimitTypeID, func(counterID int32, _ *atomic.Buffer) bool {
		if reader.GetCounterRegistrationId(counterID) == publication.RegistrationID() {
			limit = reader.GetCounterValue(counterID)
			return false
		}
		return true
	})
	return limit
}

// RequestReplayToken requests a token authorising a replay of recordingID over a response channel. The
// token is used in ReplayParams.ReplayToken of a replay request sent on a different publication.
func (archive *Archive) RequestReplayToken(recordingID int64) (int64, error) {
	logger.Debugf("RequestReplayToken(%d)", recordingID)
	return archive.request(func(correlationID int64) error {
		return archive.Proxy.ReplayTokenRequest(correlationID, recordingID)
	})
}

// GetMaxRecordedPosition returns the position recorded so far for recordingID. For an active recording this
// can be ahead of GetRecordingPosition as it includes data written but not yet reported via the counter.
// The recording should be stopped (or the publication closed) for the value to be final.
func (archive *Archive) GetMaxRecordedPosition(recordingID int64) (int64, error) {
	logger.Debugf("GetMaxRecordedPosition(%d)", recordingID)
	return archive.request(func(correlationID int64) error {
		return archive.Proxy.MaxRecordedPositionRequest(correlationID, recordingID)
	})
}
