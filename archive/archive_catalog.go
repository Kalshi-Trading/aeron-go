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
	"bytes"
	"fmt"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
)

// Archive error codes carried in ControlResponse.RelevantId when Code is ERROR (Java ArchiveException).
const (
	archiveErrorUnknownReplication = 12
)

// RecordingDescriptorConsumer receives each recording descriptor from ListRecordings, ListRecordingsForUri
// and ListRecording, mirroring Java's RecordingDescriptorConsumer. The descriptor is freshly decoded
// per call and may be retained.
type RecordingDescriptorConsumer func(descriptor *codecs.RecordingDescriptor)

// RecordingSubscriptionDescriptorConsumer receives each descriptor from ListRecordingSubscriptions, mirroring
// Java's RecordingSubscriptionDescriptorConsumer.
type RecordingSubscriptionDescriptorConsumer func(descriptor *codecs.RecordingSubscriptionDescriptor)

// ReplicationParams holds the optional parameters of a replicate request, mirroring Java's
// io.aeron.archive.client.ReplicationParams. Use NewReplicationParams to get Java defaults
// (all numeric fields NULL_VALUE / NULL_POSITION, strings empty).
type ReplicationParams struct {
	// StopPosition at which to stop the replication; RecordingPositionNull to replicate the whole recording.
	StopPosition int64
	// DstRecordingID to extend; RecordingIdNullValue to create a new recording.
	DstRecordingID int64
	// LiveDestination to add to the replication subscription to merge with a live stream (empty = none).
	LiveDestination string
	// ReplicationChannel used by the destination archive; empty uses the archive's configured default.
	ReplicationChannel string
	// ChannelTagID used to tag the replication subscription channel; NULL (-1) for none.
	ChannelTagID int64
	// SubscriptionTagID used to tag the replication subscription; NULL (-1) for none.
	SubscriptionTagID int64
	// FileIoMaxLength bounds the replay buffer used by the source archive; NULL (-1) for the default.
	FileIoMaxLength int32
	// ReplicationSessionID to use for the replay of the source recording; NULL (-1) to let the archive pick.
	ReplicationSessionID int32
	// EncodedCredentials passed to the source archive when connecting to it.
	EncodedCredentials []byte
	// SrcResponseChannel used by the source archive to respond to the destination archive (response channels).
	SrcResponseChannel string
}

// NewReplicationParams returns ReplicationParams with Java defaults.
func NewReplicationParams() *ReplicationParams {
	return new(ReplicationParams).Reset()
}

// Reset restores all fields to Java defaults and returns the receiver for chaining.
func (p *ReplicationParams) Reset() *ReplicationParams {
	*p = ReplicationParams{
		StopPosition:         RecordingPositionNull,
		DstRecordingID:       RecordingIdNullValue,
		ChannelTagID:         aeronNullValue,
		SubscriptionTagID:    aeronNullValue,
		FileIoMaxLength:      aeronNullValue,
		ReplicationSessionID: aeronNullValue,
	}
	return p
}

const aeronNullValue = -1

func (p *ReplicationParams) toCodec(srcRecordingID int64, srcControlStreamID int32, srcControlChannel string) *codecs.ReplicateRequest2Params {
	return &codecs.ReplicateRequest2Params{
		SrcRecordingId:       srcRecordingID,
		DstRecordingId:       p.DstRecordingID,
		StopPosition:         p.StopPosition,
		ChannelTagId:         p.ChannelTagID,
		SubscriptionTagId:    p.SubscriptionTagID,
		SrcControlStreamId:   srcControlStreamID,
		FileIoMaxLength:      p.FileIoMaxLength,
		ReplicationSessionId: p.ReplicationSessionID,
		SrcControlChannel:    srcControlChannel,
		LiveDestination:      p.LiveDestination,
		ReplicationChannel:   p.ReplicationChannel,
		EncodedCredentials:   p.EncodedCredentials,
		SrcResponseChannel:   p.SrcResponseChannel,
	}
}

// SegmentFileBasePosition returns the base position of the segment file containing position, for a
// recording that started at startPosition. Mirrors Java AeronArchive.segmentFileBasePosition.
// termBufferLength and segmentFileLength must be powers of two.
func SegmentFileBasePosition(startPosition int64, position int64, termBufferLength int32, segmentFileLength int32) int64 {
	startTermBasePosition := startPosition - (startPosition & int64(termBufferLength-1))
	lengthFromBasePosition := position - startTermBasePosition
	segments := lengthFromBasePosition - (lengthFromBasePosition & int64(segmentFileLength-1))
	return startTermBasePosition + segments
}

// ListRecordingsWithConsumer lists up to recordCount recordings starting at fromRecordingID, dispatching
// each descriptor to consumer. Returns the number of descriptors dispatched, which may be less than
// recordCount if the end of the catalog is reached. Mirrors Java AeronArchive.listRecordings.
func (archive *Archive) ListRecordingsWithConsumer(fromRecordingID int64, recordCount int32, consumer RecordingDescriptorConsumer) (int32, error) {
	return archive.listDescriptors(recordCount, consumer, nil, func(correlationID int64) error {
		return archive.Proxy.ListRecordingsRequest(correlationID, fromRecordingID, recordCount)
	})
}

// ListRecordingsForUriWithConsumer lists up to recordCount recordings, starting at fromRecordingID, whose
// stream matches stream and whose original channel contains channelFragment. Returns the number of
// descriptors dispatched. Mirrors Java AeronArchive.listRecordingsForUri.
func (archive *Archive) ListRecordingsForUriWithConsumer(fromRecordingID int64, recordCount int32, channelFragment string, stream int32, consumer RecordingDescriptorConsumer) (int32, error) {
	return archive.listDescriptors(recordCount, consumer, nil, func(correlationID int64) error {
		return archive.Proxy.ListRecordingsForUriRequest(correlationID, fromRecordingID, recordCount, stream, channelFragment)
	})
}

// ListRecordingWithConsumer dispatches the descriptor for recordingID to consumer. Returns 1 if found,
// 0 if the recording is unknown. Mirrors Java AeronArchive.listRecording.
func (archive *Archive) ListRecordingWithConsumer(recordingID int64, consumer RecordingDescriptorConsumer) (int32, error) {
	return archive.listDescriptors(1, consumer, nil, func(correlationID int64) error {
		return archive.Proxy.ListRecordingRequest(correlationID, recordingID)
	})
}

// ListRecordingSubscriptionsWithConsumer lists up to subscriptionCount active recording subscriptions
// starting at pseudoIndex whose channel contains channelFragment (and whose stream is stream when
// applyStreamID is true). Returns the number dispatched. Mirrors Java AeronArchive.listRecordingSubscriptions.
func (archive *Archive) ListRecordingSubscriptionsWithConsumer(pseudoIndex int32, subscriptionCount int32, applyStreamID bool, stream int32, channelFragment string, consumer RecordingSubscriptionDescriptorConsumer) (int32, error) {
	return archive.listDescriptors(subscriptionCount, nil, consumer, func(correlationID int64) error {
		return archive.Proxy.ListRecordingSubscriptionsRequest(correlationID, pseudoIndex, subscriptionCount, applyStreamID, stream, channelFragment)
	})
}

// GetMaxRecordedPosition returns the position recorded so far for an active recording, or the stop
// position of a stopped one, without the active-recording requirement of GetRecordingPosition.
// Mirrors Java AeronArchive.getMaxRecordedPosition.
func (archive *Archive) GetMaxRecordedPosition(recordingID int64) (int64, error) {
	return archive.simpleRequest(func(correlationID int64) error {
		return archive.Proxy.MaxRecordedPositionRequest(correlationID, recordingID)
	})
}

// ReplicateWithParams replicates srcRecordingID from the archive at srcControlChannel/srcControlStreamID
// into this archive using every field of params. A nil params uses NewReplicationParams().
// Returns the replication id, which can be passed to StopReplication / TryStopReplication.
// Progress is reported via RecordingSignalListener (REPLICATE, EXTEND, SYNC, MERGE, REPLICATE_END, STOP).
// Mirrors Java AeronArchive.replicate(long, int, String, ReplicationParams).
func (archive *Archive) ReplicateWithParams(srcRecordingID int64, srcControlStreamID int32, srcControlChannel string, params *ReplicationParams) (int64, error) {
	return archive.simpleRequest(func(correlationID int64) error {
		return archive.Proxy.ReplicateRequestWithParams(correlationID, srcRecordingID, srcControlStreamID, srcControlChannel, params)
	})
}

// TryStopReplication stops a replication session, returning false rather than an error if the
// replication is unknown (e.g. already finished). Mirrors Java AeronArchive.tryStopReplication.
func (archive *Archive) TryStopReplication(replicationID int64) (bool, error) {
	_, err := archive.simpleRequest(func(correlationID int64) error {
		return archive.Proxy.StopReplicationRequest(correlationID, replicationID)
	})
	if err != nil {
		if r := archive.Control.Results.ControlResponse; r != nil &&
			r.Code == codecs.ControlResponseCode.ERROR && r.RelevantId == archiveErrorUnknownReplication {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (archive *Archive) simpleRequest(send func(correlationID int64) error) (int64, error) {
	correlationID := nextCorrelationID()
	correlations.Store(correlationID, archive.Control)
	defer correlations.Delete(correlationID)

	archive.mtx.Lock()
	defer archive.mtx.Unlock()
	if err := send(correlationID); err != nil {
		return 0, err
	}
	return archive.Control.PollForResponse(correlationID, archive.SessionID)
}

// descriptorPoller mirrors Java RecordingDescriptorPoller / RecordingSubscriptionDescriptorPoller.
type descriptorPoller struct {
	archive              *Archive
	correlationID        int64
	remaining            int32
	isDispatchComplete   bool
	err                  error
	recordingConsumer    RecordingDescriptorConsumer
	subscriptionConsumer RecordingSubscriptionDescriptorConsumer
	unknownTerminalCode  codecs.ControlResponseCodeEnum
}

func (archive *Archive) listDescriptors(count int32, rc RecordingDescriptorConsumer, sc RecordingSubscriptionDescriptorConsumer, send func(correlationID int64) error) (int32, error) {
	correlationID := nextCorrelationID()
	correlations.Store(correlationID, archive.Control)
	defer correlations.Delete(correlationID)

	archive.mtx.Lock()
	defer archive.mtx.Unlock()
	if err := send(correlationID); err != nil {
		return 0, err
	}

	poller := &descriptorPoller{
		archive:              archive,
		correlationID:        correlationID,
		remaining:            count,
		recordingConsumer:    rc,
		subscriptionConsumer: sc,
		unknownTerminalCode:  codecs.ControlResponseCode.RECORDING_UNKNOWN,
	}
	if sc != nil {
		poller.unknownTerminalCode = codecs.ControlResponseCode.SUBSCRIPTION_UNKNOWN
	}
	if count <= 0 {
		// The archive replies with nothing for a zero count listing (Java returns 0 immediately too).
		return 0, nil
	}
	return poller.pollAll()
}

func (p *descriptorPoller) pollAll() (int32, error) {
	control := p.archive.Control
	initial := p.remaining
	assembler := aeron.NewControlledFragmentAssembler(p.onFragment, aeron.DefaultFragmentAssemblyBufferLength)
	deadline := time.Now().Add(p.archive.Options.Timeout)
	lastRemaining := p.remaining
	for {
		fragments := control.Subscription.ControlledPoll(assembler.OnFragment, controlFragmentLimit)
		if p.err != nil {
			return initial - p.remaining, p.err
		}
		if p.isDispatchComplete {
			return initial - p.remaining, nil
		}
		if p.remaining != lastRemaining {
			lastRemaining = p.remaining
			deadline = time.Now().Add(p.archive.Options.Timeout)
		}
		if fragments == 0 {
			if control.Subscription.IsClosed() || !control.Subscription.IsConnected() {
				return initial - p.remaining, fmt.Errorf("response channel from archive is not connected")
			}
			if time.Now().After(deadline) {
				return initial - p.remaining, fmt.Errorf("timeout waiting for descriptors for correlationID %d", p.correlationID)
			}
			p.archive.Options.IdleStrategy.Idle(0)
		}
	}
}

func (p *descriptorPoller) onFragment(buffer *atomic.Buffer, offset int32, length int32, _ *logbuffer.Header) term.ControlledPollAction {
	if p.isDispatchComplete || p.err != nil {
		return term.ControlledPollActionAbort
	}

	buf := new(bytes.Buffer)
	buffer.WriteBytes(buf, offset, length)
	marshaller := codecs.NewSbeGoMarshaller()
	var hdr codecs.SbeGoMessageHeader
	if err := hdr.Decode(marshaller, buf); err != nil {
		p.listenerError(fmt.Errorf("descriptor poller failed to decode message header: %w", err))
		return term.ControlledPollActionContinue
	}
	sessionID := p.archive.SessionID
	rangeCheck := p.archive.Options.RangeChecking

	switch hdr.TemplateId {
	case codecIds.controlResponse:
		resp := new(codecs.ControlResponse)
		if err := resp.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeCheck); err != nil {
			p.listenerError(fmt.Errorf("descriptor poller failed to decode control response: %w", err))
			return term.ControlledPollActionContinue
		}
		if resp.ControlSessionId != sessionID {
			return term.ControlledPollActionContinue
		}
		if resp.Code == codecs.ControlResponseCode.ERROR {
			err := fmt.Errorf("response for correlationID %d (relevantId %d) failed %s", resp.CorrelationId, resp.RelevantId, resp.ErrorMessage)
			if resp.CorrelationId == p.correlationID {
				p.archive.Control.Results.ControlResponse = resp
				p.err = err
				return term.ControlledPollActionBreak
			}
			p.listenerError(err)
		} else if resp.CorrelationId == p.correlationID && resp.Code == p.unknownTerminalCode {
			p.isDispatchComplete = true
			return term.ControlledPollActionBreak
		}

	case codecIds.recordingDescriptor:
		if p.recordingConsumer == nil {
			return term.ControlledPollActionContinue
		}
		rd := new(codecs.RecordingDescriptor)
		if err := rd.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeCheck); err != nil {
			p.listenerError(fmt.Errorf("descriptor poller failed to decode recording descriptor: %w", err))
			return term.ControlledPollActionContinue
		}
		if rd.ControlSessionId == sessionID && rd.CorrelationId == p.correlationID {
			p.recordingConsumer(rd)
			return p.dispatched()
		}

	case codecIds.recordingSubscriptionDescriptor:
		if p.subscriptionConsumer == nil {
			return term.ControlledPollActionContinue
		}
		rsd := new(codecs.RecordingSubscriptionDescriptor)
		if err := rsd.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeCheck); err != nil {
			p.listenerError(fmt.Errorf("descriptor poller failed to decode recording subscription descriptor: %w", err))
			return term.ControlledPollActionContinue
		}
		if rsd.ControlSessionId == sessionID && rsd.CorrelationId == p.correlationID {
			p.subscriptionConsumer(rsd)
			return p.dispatched()
		}

	case codecIds.recordingSignalEvent:
		rse := new(codecs.RecordingSignalEvent)
		if err := rse.Decode(marshaller, buf, hdr.Version, hdr.BlockLength, rangeCheck); err != nil {
			p.listenerError(fmt.Errorf("descriptor poller failed to decode recording signal: %w", err))
			return term.ControlledPollActionContinue
		}
		dispatchRecordingSignal(p.archive, rse)
	}
	return term.ControlledPollActionContinue
}

func (p *descriptorPoller) dispatched() term.ControlledPollAction {
	p.remaining--
	if p.remaining <= 0 {
		p.isDispatchComplete = true
		return term.ControlledPollActionBreak
	}
	return term.ControlledPollActionContinue
}

func (p *descriptorPoller) listenerError(err error) {
	if p.archive.Listeners.ErrorListener != nil {
		p.archive.Listeners.ErrorListener(err)
	}
}

// dispatchRecordingSignal delivers a recording signal to the RecordingSignalListener only if it belongs
// to this archive client's control session (as Java's ControlEventListener / RecordingSignalConsumer do).
func dispatchRecordingSignal(archive *Archive, rse *codecs.RecordingSignalEvent) {
	if rse.ControlSessionId == archive.SessionID && archive.Listeners.RecordingSignalListener != nil {
		archive.Listeners.RecordingSignalListener(rse)
	}
}
