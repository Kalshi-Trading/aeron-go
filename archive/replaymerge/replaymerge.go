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
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
)

// LiveAddMaxWindow is the maximum window at which a live destination should be added when trying to merge.
const LiveAddMaxWindow = int32(32 * 1024 * 1024)

// ReplayRemoveThreshold is the distance from the live recorded position at which the replay is stopped and removed.
const ReplayRemoveThreshold = int64(0)

// MergeProgressTimeoutDefaultMs is the default timeout in milliseconds for the merge to make progress.
const MergeProgressTimeoutDefaultMs = int64(5 * time.Second / time.Millisecond)

const (
	initialGetMaxRecordedPositionBackoffMs = int64(8)
	getMaxRecordedPositionBackoffMaxMs     = int64(500)
	archivePollIntervalMs                  = int64(100)
)

// replayLength follows the live recording, like Java's Long.MAX_VALUE.
const replayLength = int64(math.MaxInt64)

// EpochClock returns the current time in milliseconds since the epoch.
type EpochClock func() int64

// SystemEpochClock is the EpochClock backed by time.Now.
func SystemEpochClock() int64 {
	return time.Now().UnixMilli()
}

// State of the ReplayMerge state machine.
type State int

const (
	StateResolveReplayPort State = iota
	StateGetRecordingPosition
	StateReplay
	StateCatchup
	StateAttemptLiveJoin
	StateMerged
	StateFailed
	StateClosed
)

var stateNames = [...]string{
	"RESOLVE_REPLAY_PORT", "GET_RECORDING_POSITION", "REPLAY", "CATCHUP", "ATTEMPT_LIVE_JOIN", "MERGED", "FAILED",
	"CLOSED",
}

func (s State) String() string {
	if s < 0 || int(s) >= len(stateNames) {
		return fmt.Sprintf("State(%d)", int(s))
	}
	return stateNames[s]
}

// ErrNoProgress is wrapped by the error returned when the merge fails to make progress within the merge progress
// timeout (Java's TimeoutException).
var ErrNoProgress = errors.New("ReplayMerge no progress")

// ErrImageClosed is returned when the replay image closes before the merge completes.
var ErrImageClosed = errors.New("ReplayMerge Image closed unexpectedly")

// ErrArchiveNotConnected is returned when the archive control response stream is no longer connected.
var ErrArchiveNotConnected = errors.New("archive is not connected")

// ArchiveError is the error returned for an ERROR control response received for this control session. It is the
// archive client's ArchiveException, as Java's ReplayMerge throws ArchiveException(message, errorCode, correlationId).
type ArchiveError = archive.ArchiveException

// ReplayMerge replays a recorded stream from a starting position and merges with the live stream for a full history
// of a stream.
//
// Once constructed either of Poll, ControlledPoll or DoWork, interleaved with consumption of the Image, should be
// called in a duty cycle loop until IsMerged is true. After which the ReplayMerge can be closed and continued usage
// can be made of the Image or its parent Subscription. If an error occurs or progress stops, the merge will fail and
// HasFailed will be true.
//
// If the endpoint on the replay destination uses a port of 0, then the OS will assign a port from the ephemeral
// range and this will be added to the replay channel for instructing the archive.
//
// NOTE: Merging is only supported with UDP streams.
//
// NOTE: ReplayMerge is not goroutine safe and should not be used with an Archive client that is used concurrently
// by other goroutines, as it polls the archive control response stream directly.
type ReplayMerge struct {
	recordingID                     int64
	startPosition                   int64
	mergeProgressTimeoutMs          int64
	replaySessionID                 int64
	activeCorrelationID             int64
	nextTargetPosition              int64
	positionOfLastProgress          int64
	timeOfLastProgressMs            int64
	timeOfNextGetMaxRecordedPosMs   int64
	getMaxRecordedPositionBackoffMs int64
	timeOfLastScheduledArchivePoll  int64
	isLiveAdded                     bool
	isReplayActive                  bool
	state                           State
	image                           aeron.Image
	polledResponse                  *codecs.ControlResponse

	archive           mergeArchive
	subscription      mergeSubscription
	aeronSubscription *aeron.Subscription
	epochClock        EpochClock
	replayDestination string
	liveDestination   string
	replayChannelURI  aeron.ChannelUri
}

// NewReplayMerge creates a ReplayMerge to manage the merging of a replayed stream and switching over to the live
// stream as appropriate, using the system clock for progress checks.
//
// subscription           to use for the replay and live stream. Must be a multi-destination subscription.
// archive                to use for the replay.
// replayChannel          to use as a template for what the archive will use.
// replayDestination      to send the replay to and the destination added by the Subscription.
// liveDestination        for the live stream and the destination added by the Subscription.
// recordingID            for the replay.
// startPosition          for the replay.
// mergeProgressTimeoutMs to use for progress checks.
func NewReplayMerge(
	subscription *aeron.Subscription,
	archive *archive.Archive,
	replayChannel string,
	replayDestination string,
	liveDestination string,
	recordingID int64,
	startPosition int64,
	mergeProgressTimeoutMs int64) (*ReplayMerge, error) {
	return NewReplayMergeWithClock(subscription, archive, replayChannel, replayDestination, liveDestination,
		recordingID, startPosition, SystemEpochClock, mergeProgressTimeoutMs)
}

// NewReplayMergeWithDefaults creates a ReplayMerge using the system clock and MergeProgressTimeoutDefaultMs.
func NewReplayMergeWithDefaults(
	subscription *aeron.Subscription,
	archive *archive.Archive,
	replayChannel string,
	replayDestination string,
	liveDestination string,
	recordingID int64,
	startPosition int64) (*ReplayMerge, error) {
	return NewReplayMergeWithClock(subscription, archive, replayChannel, replayDestination, liveDestination,
		recordingID, startPosition, SystemEpochClock, MergeProgressTimeoutDefaultMs)
}

// NewReplayMergeWithClock creates a ReplayMerge with an explicit epochClock used for progress checks.
func NewReplayMergeWithClock(
	subscription *aeron.Subscription,
	archive *archive.Archive,
	replayChannel string,
	replayDestination string,
	liveDestination string,
	recordingID int64,
	startPosition int64,
	epochClock EpochClock,
	mergeProgressTimeoutMs int64) (*ReplayMerge, error) {
	if subscription == nil {
		return nil, errors.New("subscription is nil")
	}
	if archive == nil {
		return nil, errors.New("archive is nil")
	}
	rm, err := newReplayMerge(subscription, newArchiveClient(archive), replayChannel, replayDestination,
		liveDestination, recordingID, startPosition, epochClock, mergeProgressTimeoutMs)
	if err != nil {
		return nil, err
	}
	rm.aeronSubscription = subscription
	return rm, nil
}

func newReplayMerge(
	subscription mergeSubscription,
	archive mergeArchive,
	replayChannel string,
	replayDestination string,
	liveDestination string,
	recordingID int64,
	startPosition int64,
	epochClock EpochClock,
	mergeProgressTimeoutMs int64) (*ReplayMerge, error) {
	if strings.HasPrefix(subscription.Channel(), aeron.IpcChannel) ||
		strings.HasPrefix(replayChannel, aeron.IpcChannel) ||
		strings.HasPrefix(replayDestination, aeron.IpcChannel) ||
		strings.HasPrefix(liveDestination, aeron.IpcChannel) {
		return nil, errors.New("IPC merging is not supported")
	}

	if !strings.Contains(subscription.Channel(), "control-mode=manual") {
		return nil, fmt.Errorf("Subscription URI must have 'control-mode=manual' uri=%s", subscription.Channel())
	}

	if epochClock == nil {
		epochClock = SystemEpochClock
	}

	replayChannelURI, err := aeron.ParseChannelUri(replayChannel)
	if err != nil {
		return nil, fmt.Errorf("invalid replay channel '%s': %w", replayChannel, err)
	}
	replayChannelURI.Set(aeron.LingerParamName, "0")
	replayChannelURI.Set(aeron.EosParamName, "false")

	replayDestinationURI, err := aeron.ParseChannelUri(replayDestination)
	if err != nil {
		return nil, fmt.Errorf("invalid replay destination '%s': %w", replayDestination, err)
	}
	replayEndpoint := replayDestinationURI.Get(aeron.EndpointParamName)
	if replayEndpoint == "" {
		return nil, fmt.Errorf("replay destination must have an endpoint: %s", replayDestination)
	}

	rm := &ReplayMerge{
		archive:                         archive,
		subscription:                    subscription,
		epochClock:                      epochClock,
		replayDestination:               replayDestination,
		liveDestination:                 liveDestination,
		recordingID:                     recordingID,
		startPosition:                   startPosition,
		mergeProgressTimeoutMs:          mergeProgressTimeoutMs,
		replaySessionID:                 aeron.NullValue,
		activeCorrelationID:             aeron.NullValue,
		nextTargetPosition:              aeron.NullValue,
		positionOfLastProgress:          aeron.NullValue,
		getMaxRecordedPositionBackoffMs: initialGetMaxRecordedPositionBackoffMs,
		replayChannelURI:                replayChannelURI,
	}

	if strings.HasSuffix(replayEndpoint, ":0") {
		rm.state = StateResolveReplayPort
	} else {
		rm.replayChannelURI.Set(aeron.EndpointParamName, replayEndpoint)
		rm.state = StateGetRecordingPosition
	}

	if err := subscription.AddDestination(replayDestination); err != nil {
		return nil, fmt.Errorf("failed to add replay destination '%s': %w", replayDestination, err)
	}
	nowMs := epochClock()
	rm.timeOfLastProgressMs = nowMs
	rm.timeOfNextGetMaxRecordedPosMs = nowMs
	return rm, nil
}

// Close stops any active replay and removes the replay destination from the subscription unless already merged.
// This operation will NOT remove the live destination if it has been added, so it can be used for live consumption.
// Like Java, the replay destination is removed asynchronously, so Close does not wait on the driver. Close is
// idempotent; errors removing the destination or stopping the replay are returned but the ReplayMerge is closed
// regardless.
func (rm *ReplayMerge) Close() error {
	state := rm.state
	if StateClosed == state {
		return nil
	}

	var errs []error
	if !rm.archive.isAeronClosed() {
		if StateMerged != state {
			if _, err := rm.subscription.AsyncRemoveDestination(rm.replayDestination); err != nil {
				errs = append(errs, err)
			}
		}

		if rm.isReplayActive && rm.archive.isRequestConnected() {
			if err := rm.stopReplay(); err != nil {
				errs = append(errs, err)
			}
		}
	}

	rm.setState(StateClosed)
	return errors.Join(errs...)
}

// Subscription returns the Subscription used to consume the replayed and merged stream.
func (rm *ReplayMerge) Subscription() *aeron.Subscription {
	return rm.aeronSubscription
}

// DoWork performs the work of replaying and merging. Should only be used if polling the underlying Image directly;
// otherwise call Poll or ControlledPoll on this type.
//
// Returns an indication of work done processing the merge. If an error is returned the merge has transitioned to
// StateFailed.
func (rm *ReplayMerge) DoWork() (workCount int, err error) {
	nowMs := rm.epochClock()

	switch rm.state {
	case StateResolveReplayPort:
		workCount, err = rm.resolveReplayPort(nowMs)
	case StateGetRecordingPosition:
		workCount, err = rm.getRecordingPosition(nowMs)
	case StateReplay:
		workCount, err = rm.replay(nowMs)
	case StateCatchup:
		workCount, err = rm.catchup(nowMs)
	case StateAttemptLiveJoin:
		workCount, err = rm.attemptLiveJoin(nowMs)
	default:
		return 0, nil
	}

	if err == nil {
		err = rm.checkProgress(nowMs)
	}
	if err != nil {
		rm.setState(StateFailed)
	}
	return workCount, err
}

// Poll polls the Image used for replay and merging and live stream. DoWork is called before the poll so that
// processing of the merge can be done.
//
// Returns the number of fragments processed.
func (rm *ReplayMerge) Poll(fragmentHandler term.FragmentHandler, fragmentLimit int) (int, error) {
	if _, err := rm.DoWork(); err != nil {
		return 0, err
	}
	if rm.image == nil {
		return 0, nil
	}
	return rm.image.Poll(fragmentHandler, fragmentLimit), nil
}

// ControlledPoll is the controlled fragment handler variant of Poll. DoWork is called before the poll so that
// processing of the merge can be done.
//
// Returns the number of fragments processed.
func (rm *ReplayMerge) ControlledPoll(fragmentHandler term.ControlledFragmentHandler, fragmentLimit int) (int, error) {
	if _, err := rm.DoWork(); err != nil {
		return 0, err
	}
	if rm.image == nil {
		return 0, nil
	}
	return rm.image.ControlledPoll(fragmentHandler, fragmentLimit), nil
}

// IsMerged returns true if the live stream is merged and the replay stopped.
func (rm *ReplayMerge) IsMerged() bool {
	return rm.state == StateMerged
}

// HasFailed returns true if the replay merge has failed due to an error.
func (rm *ReplayMerge) HasFailed() bool {
	return rm.state == StateFailed
}

// IsClosed returns true if Close has been called.
func (rm *ReplayMerge) IsClosed() bool {
	return rm.state == StateClosed
}

// State returns the current state of the merge.
func (rm *ReplayMerge) State() State {
	return rm.state
}

// Image returns the Image which is a merge of the replay and live stream, or nil if not yet available.
func (rm *ReplayMerge) Image() aeron.Image {
	return rm.image
}

// IsLiveAdded returns true if the live destination has been added to the Subscription.
func (rm *ReplayMerge) IsLiveAdded() bool {
	return rm.isLiveAdded
}

// ReplayChannel returns the channel the archive is (or will be) instructed to replay to, including any resolved
// ephemeral port.
func (rm *ReplayMerge) ReplayChannel() string {
	return rm.replayChannelURI.String()
}

func (rm *ReplayMerge) String() string {
	transportCount := int32(0)
	if rm.image != nil {
		transportCount = rm.image.ActiveTransportCount()
	}
	return fmt.Sprintf("ReplayMerge{state=%s, nextTargetPosition=%d, timeOfLastProgressMs=%d, "+
		"positionOfLastProgress=%d, isLiveAdded=%t, isReplayActive=%t, replayChannelUri=%s, "+
		"activeTransportCount=%d}",
		rm.state, rm.nextTargetPosition, rm.timeOfLastProgressMs, rm.positionOfLastProgress, rm.isLiveAdded,
		rm.isReplayActive, rm.replayChannelURI.String(), transportCount)
}

func (rm *ReplayMerge) resolveReplayPort(nowMs int64) (int, error) {
	resolvedEndpoint := rm.subscription.ResolvedEndpoint()
	if resolvedEndpoint == "" {
		return 0, nil
	}
	if err := replaceEndpointWildcardPort(rm.replayChannelURI, resolvedEndpoint); err != nil {
		return 0, err
	}
	rm.timeOfLastProgressMs = nowMs
	rm.setState(StateGetRecordingPosition)
	return 1, nil
}

func (rm *ReplayMerge) getRecordingPosition(nowMs int64) (int, error) {
	workCount := 0

	if aeron.NullValue == rm.activeCorrelationID {
		sent, err := rm.callGetMaxRecordedPosition(nowMs)
		if err != nil {
			return 0, err
		}
		if sent {
			rm.timeOfLastProgressMs = nowMs
			workCount++
		}
	} else {
		ok, err := rm.pollForResponse(rm.activeCorrelationID)
		if err != nil {
			return 0, err
		}
		if ok {
			rm.nextTargetPosition = rm.polledRelevantID()
			rm.activeCorrelationID = aeron.NullValue

			if archive.RecordingPositionNull != rm.nextTargetPosition {
				rm.timeOfLastProgressMs = nowMs
				rm.setState(StateReplay)
			}
			workCount++
		}
	}

	return workCount, nil
}

func (rm *ReplayMerge) replay(nowMs int64) (int, error) {
	workCount := 0

	if aeron.NullValue == rm.activeCorrelationID {
		correlationID := rm.archive.nextCorrelationID()
		sent, err := rm.archive.replay(correlationID, rm.recordingID, rm.startPosition, replayLength,
			rm.replayChannelURI.String(), rm.subscription.StreamID())
		if err != nil {
			return 0, err
		}
		if sent {
			rm.activeCorrelationID = correlationID
			rm.timeOfLastProgressMs = nowMs
			workCount++
		}
	} else {
		ok, err := rm.pollForResponse(rm.activeCorrelationID)
		if err != nil {
			return 0, err
		}
		if ok {
			rm.isReplayActive = true
			rm.replaySessionID = rm.polledRelevantID()
			rm.timeOfLastProgressMs = nowMs
			rm.activeCorrelationID = aeron.NullValue

			rm.getMaxRecordedPositionBackoffMs = initialGetMaxRecordedPositionBackoffMs
			rm.timeOfNextGetMaxRecordedPosMs = nowMs

			rm.setState(StateCatchup)
			workCount++
		}
	}

	return workCount, nil
}

func (rm *ReplayMerge) catchup(nowMs int64) (int, error) {
	workCount := 0

	if rm.image == nil && rm.subscription.IsConnected() {
		rm.timeOfLastProgressMs = nowMs
		// The replay session id is the low 32 bits of the replay session id returned by the archive.
		image := rm.subscription.ImageBySessionID(int32(rm.replaySessionID))
		if image != nil {
			rm.image = image
			rm.positionOfLastProgress = image.Position()
		} else {
			rm.positionOfLastProgress = aeron.NullValue
		}
	}

	if rm.image != nil {
		position := rm.image.Position()
		if position >= rm.nextTargetPosition {
			rm.timeOfLastProgressMs = nowMs
			rm.positionOfLastProgress = position
			rm.setState(StateAttemptLiveJoin)
			workCount++
		} else if position > rm.positionOfLastProgress {
			rm.timeOfLastProgressMs = nowMs
			rm.positionOfLastProgress = position
		} else if rm.image.IsClosed() {
			return 0, ErrImageClosed
		}
	}

	return workCount, nil
}

func (rm *ReplayMerge) attemptLiveJoin(nowMs int64) (int, error) {
	workCount := 0

	if aeron.NullValue == rm.activeCorrelationID {
		sent, err := rm.callGetMaxRecordedPosition(nowMs)
		if err != nil {
			return 0, err
		}
		if sent {
			rm.timeOfLastProgressMs = nowMs
			workCount++
		}
	} else {
		ok, err := rm.pollForResponse(rm.activeCorrelationID)
		if err != nil {
			return 0, err
		}
		if ok {
			rm.nextTargetPosition = rm.polledRelevantID()
			rm.activeCorrelationID = aeron.NullValue

			if archive.RecordingPositionNull != rm.nextTargetPosition {
				nextState := StateCatchup

				if rm.image != nil {
					position := rm.image.Position()
					if rm.shouldAddLiveDestination(position) {
						if _, err := rm.subscription.AsyncAddDestination(rm.liveDestination); err != nil {
							return 0, fmt.Errorf("failed to add live destination '%s': %w", rm.liveDestination, err)
						}
						rm.timeOfLastProgressMs = nowMs
						rm.positionOfLastProgress = position
						rm.isLiveAdded = true
					} else if rm.shouldStopAndRemoveReplay(position) {
						if _, err := rm.subscription.AsyncRemoveDestination(rm.replayDestination); err != nil {
							return 0, fmt.Errorf(
								"failed to remove replay destination '%s': %w", rm.replayDestination, err)
						}
						if err := rm.stopReplay(); err != nil {
							return 0, err
						}
						rm.timeOfLastProgressMs = nowMs
						rm.positionOfLastProgress = position
						nextState = StateMerged
					}
				}

				rm.setState(nextState)
			}

			workCount++
		}
	}

	return workCount, nil
}

func (rm *ReplayMerge) callGetMaxRecordedPosition(nowMs int64) (bool, error) {
	if nowMs < rm.timeOfNextGetMaxRecordedPosMs {
		return false, nil
	}

	correlationID := rm.archive.nextCorrelationID()
	sent, err := rm.archive.maxRecordedPosition(correlationID, rm.recordingID)
	if err != nil {
		return false, err
	}
	if sent {
		rm.activeCorrelationID = correlationID
	}

	// increase backoff regardless of result
	rm.getMaxRecordedPositionBackoffMs = minInt64(rm.getMaxRecordedPositionBackoffMs*2, getMaxRecordedPositionBackoffMaxMs)
	rm.timeOfNextGetMaxRecordedPosMs = nowMs + rm.getMaxRecordedPositionBackoffMs

	return sent, nil
}

func (rm *ReplayMerge) stopReplay() error {
	correlationID := rm.archive.nextCorrelationID()
	sent, err := rm.archive.stopReplay(correlationID, rm.replaySessionID)
	if err != nil {
		return err
	}
	if sent {
		rm.isReplayActive = false
	}
	return nil
}

func (rm *ReplayMerge) setState(newState State) {
	rm.state = newState
	rm.activeCorrelationID = aeron.NullValue
}

func (rm *ReplayMerge) shouldAddLiveDestination(position int64) bool {
	return !rm.isLiveAdded &&
		(rm.nextTargetPosition-position) <= minInt64(int64(rm.image.TermBufferLength()>>2), int64(LiveAddMaxWindow))
}

func (rm *ReplayMerge) shouldStopAndRemoveReplay(position int64) bool {
	return rm.isLiveAdded &&
		(rm.nextTargetPosition-position) <= ReplayRemoveThreshold &&
		rm.image.ActiveTransportCount() >= 2
}

func (rm *ReplayMerge) checkProgress(nowMs int64) error {
	if nowMs > rm.timeOfLastProgressMs+rm.mergeProgressTimeoutMs {
		transportCount := int32(0)
		if rm.image != nil {
			transportCount = rm.image.ActiveTransportCount()
		}
		return fmt.Errorf("%w: state=%s, activeTransportCount=%d", ErrNoProgress, rm.state, transportCount)
	}

	if aeron.NullValue == rm.activeCorrelationID && nowMs > rm.timeOfLastScheduledArchivePoll+archivePollIntervalMs {
		rm.timeOfLastScheduledArchivePoll = nowMs
		if _, err := rm.pollForResponse(aeron.NullValue); err != nil {
			return err
		}
	}
	return nil
}

// pollForResponse polls the archive control response stream once and returns true if a response for correlationID
// was received. An ERROR response for this control session is returned as an *ArchiveError.
func (rm *ReplayMerge) pollForResponse(correlationID int64) (bool, error) {
	pollCount, response := rm.archive.pollResponse()
	if response != nil {
		rm.polledResponse = response
		if response.ControlSessionId == rm.archive.controlSessionID() {
			if response.Code == codecs.ControlResponseCode.ERROR {
				return false, archive.NewArchiveException(
					fmt.Sprintf("archive response for correlationId=%d, error: %s",
						response.CorrelationId, response.ErrorMessage),
					archive.ArchiveErrorCode(response.RelevantId),
					response.CorrelationId)
			}
			return response.CorrelationId == correlationID, nil
		}
	} else if pollCount == 0 && !rm.archive.isResponseConnected() {
		return false, ErrArchiveNotConnected
	}
	return false, nil
}

func (rm *ReplayMerge) polledRelevantID() int64 {
	return rm.polledResponse.RelevantId
}

// replaceEndpointWildcardPort mirrors Java's ChannelUri.replaceEndpointWildcardPort.
func replaceEndpointWildcardPort(uri aeron.ChannelUri, resolvedEndpoint string) error {
	portSeparatorIndex := strings.LastIndex(resolvedEndpoint, ":")
	if portSeparatorIndex == -1 {
		return fmt.Errorf("no port specified on resolvedEndpoint=%s", resolvedEndpoint)
	}
	if strings.HasSuffix(resolvedEndpoint, ":0") {
		return fmt.Errorf("wildcard port specified on resolvedEndpoint=%s", resolvedEndpoint)
	}

	existingEndpoint := uri.Get(aeron.EndpointParamName)
	if existingEndpoint == "" {
		uri.Set(aeron.EndpointParamName, resolvedEndpoint)
	} else if strings.HasSuffix(existingEndpoint, ":0") {
		uri.Set(aeron.EndpointParamName,
			existingEndpoint[:len(existingEndpoint)-2]+resolvedEndpoint[portSeparatorIndex:])
	}
	return nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
