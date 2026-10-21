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

package examples

import (
	"fmt"
	"math"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
)

// FindLatestRecording returns the most recent recording whose channel contains channelFragment on streamID, or nil
// if there is none. Mirrors Java SamplesUtil.findLatestRecording: every recording is listed, not just the first page.
func FindLatestRecording(arch *archive.Archive, channelFragment string, streamID int32) (*codecs.RecordingDescriptor, error) {
	var latest *codecs.RecordingDescriptor
	_, err := arch.ListRecordingsForUriWithConsumer(0, math.MaxInt32, channelFragment, streamID,
		func(descriptor *codecs.RecordingDescriptor) {
			d := *descriptor
			latest = &d
		})
	if err != nil {
		return nil, err
	}
	return latest, nil
}

// PrintAvailableImage mirrors Java SamplesUtil.printAvailableImage.
func PrintAvailableImage(image aeron.Image) {
	channel, streamID := imageSubscription(image)
	fmt.Printf("Available image on %s streamId=%d sessionId=%d mtu=%d term-length=%d from %s\n",
		channel, streamID, image.SessionID(), image.MtuLength(), image.TermBufferLength(), image.SourceIdentity())
}

// PrintUnavailableImage mirrors Java SamplesUtil.printUnavailableImage.
func PrintUnavailableImage(image aeron.Image) {
	channel, streamID := imageSubscription(image)
	fmt.Printf("Unavailable image on %s streamId=%d sessionId=%d\n", channel, streamID, image.SessionID())
}

func imageSubscription(image aeron.Image) (string, int32) {
	if sub := image.Subscription(); sub != nil {
		return sub.Channel(), sub.StreamID()
	}
	return "", aeron.NullValue
}
