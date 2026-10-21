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
	"errors"
	"fmt"

	"github.com/lirm/aeron-go/aeron"
)

// tryStop sends a stop request and maps an UNKNOWN_SUBSCRIPTION error response to (false, nil), as
// AeronArchive.tryStopRecording* does.
func (archive *Archive) tryStop(send func(correlationID int64) error) (bool, error) {
	_, err := archive.request(send)
	if err == nil {
		return true, nil
	}
	var archiveErr *ArchiveError
	if errors.As(err, &archiveErr) && archiveErr.ErrorCode == ErrorCodeUnknownSubscription {
		return false, nil
	}
	return false, err
}

// TryStopRecording stops recording channel/stream if a matching recording subscription exists.
//
// Returns true if stopped, false if no recording subscription matched, or an error.
func (archive *Archive) TryStopRecording(channel string, stream int32) (bool, error) {
	logger.Debugf("TryStopRecording(%s:%d)", channel, stream)
	return archive.tryStop(func(correlationID int64) error {
		return archive.Proxy.StopRecordingRequest(correlationID, stream, channel)
	})
}

// TryStopRecordingBySubscriptionId stops the recording subscription returned by StartRecording.
//
// Returns true if stopped, false if the subscription is not known to the archive, or an error.
func (archive *Archive) TryStopRecordingBySubscriptionId(subscriptionID int64) (bool, error) {
	logger.Debugf("TryStopRecordingBySubscriptionId(%d)", subscriptionID)
	return archive.tryStop(func(correlationID int64) error {
		return archive.Proxy.StopRecordingSubscriptionRequest(correlationID, subscriptionID)
	})
}

// TryStopRecordingByIdentity stops an active recording by recording id.
//
// Returns true if the recording was active and has been stopped, false if it was not active, or an error
// (e.g. *ArchiveError with ErrorCodeUnknownRecording).
func (archive *Archive) TryStopRecordingByIdentity(recordingID int64) (bool, error) {
	logger.Debugf("TryStopRecordingByIdentity(%d)", recordingID)
	relevantID, err := archive.request(func(correlationID int64) error {
		return archive.Proxy.StopRecordingByIdentityRequest(correlationID, recordingID)
	})
	if err != nil {
		return false, err
	}
	return relevantID != 0, nil
}

// AddRecordedExclusivePublication adds an exclusive publication and starts a local recording of its
// session, equivalent to AeronArchive.addRecordedExclusivePublication.
func (archive *Archive) AddRecordedExclusivePublication(channel string, stream int32) (*aeron.Publication, error) {
	publication, err := archive.aeron.AddExclusivePublication(channel, stream)
	if err != nil {
		return nil, err
	}
	sessionChannel, err := AddSessionIdToChannel(publication.Channel(), publication.SessionID())
	if err != nil {
		publication.Close()
		return nil, err
	}
	if _, err := archive.request(func(correlationID int64) error {
		return archive.Proxy.StartRecordingRequest(correlationID, stream, true, false, sessionChannel)
	}); err != nil {
		publication.Close()
		return nil, fmt.Errorf("AddRecordedExclusivePublication: %w", err)
	}
	return publication, nil
}
