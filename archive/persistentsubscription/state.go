// Copyright 2026 Kalshi Trading.
// Copyright 2026 Adaptive Financial Consulting Limited (Java reference implementation).
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

package persistentsubscription

// State of a PersistentSubscription. The numeric values match the Java client's State codes and are the values
// published in the state counter (type id StateCounterTypeID).
type State int32

const (
	StateAwaitArchiveConnection State = iota
	StateSendListRecordingRequest
	StateAwaitListRecordingResponse
	StateSendReplayRequest
	StateAwaitReplayResponse
	StateAddReplaySubscription
	StateAwaitReplaySubscription
	StateAwaitReplayChannelEndpoint
	StateAddRequestPublication
	StateAwaitRequestPublication
	StateSendReplayTokenRequest
	StateAwaitReplayToken
	StateReplay
	StateAttemptSwitch
	StateAddLiveSubscription
	StateAwaitLive
	StateLive
	StateFailed
)

var stateNames = [...]string{
	"AWAIT_ARCHIVE_CONNECTION",
	"SEND_LIST_RECORDING_REQUEST",
	"AWAIT_LIST_RECORDING_RESPONSE",
	"SEND_REPLAY_REQUEST",
	"AWAIT_REPLAY_RESPONSE",
	"ADD_REPLAY_SUBSCRIPTION",
	"AWAIT_REPLAY_SUBSCRIPTION",
	"AWAIT_REPLAY_CHANNEL_ENDPOINT",
	"ADD_REQUEST_PUBLICATION",
	"AWAIT_REQUEST_PUBLICATION",
	"SEND_REPLAY_TOKEN_REQUEST",
	"AWAIT_REPLAY_TOKEN",
	"REPLAY",
	"ATTEMPT_SWITCH",
	"ADD_LIVE_SUBSCRIPTION",
	"AWAIT_LIVE",
	"LIVE",
	"FAILED",
}

// String returns the Java-compatible state name.
func (s State) String() string {
	if s >= 0 && int(s) < len(stateNames) {
		return stateNames[s]
	}
	return "UNKNOWN"
}

// Counter type ids, matching io.aeron.AeronCounters.
const (
	StateCounterTypeID           int32 = 114
	JoinDifferenceCounterTypeID  int32 = 115
	LiveLeftCountCounterTypeID   int32 = 116
	LiveJoinedCountCounterTypeID int32 = 117
)

type replayChannelType int

const (
	replayChannelSessionSpecific replayChannelType = iota
	replayChannelDynamicPort
	replayChannelResponse
)
