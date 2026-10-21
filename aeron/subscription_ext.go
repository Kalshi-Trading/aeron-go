/*
Copyright 2016 Stanislav Liberman
Copyright (C) 2022 Talos, Inc.

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
	"errors"
	"fmt"
	"strings"

	"github.com/lirm/aeron-go/aeron/logbuffer/term"
)

// ErrSpyRejectImage is returned when attempting to reject an image of a spy subscription.
var ErrSpyRejectImage = errors.New("unable to reject an image when using a spy channel")

// BlockPoll polls the images under the subscription for available message fragments in blocks, delivering at most
// one block of up to blockLengthLimit bytes per image to the handler. Returns the number of bytes consumed.
func (sub *Subscription) BlockPoll(handler term.BlockHandler, blockLengthLimit int32) int64 {
	var bytesConsumed int64
	for _, image := range sub.images.Get() {
		bytesConsumed += int64(image.BlockPoll(handler, blockLengthLimit))
	}
	return bytesConsumed
}

// RawPoll polls the images under the subscription for available message fragments in raw blocks, delivering at
// most one block of up to blockLengthLimit bytes per image to the handler with the file name and file offset of the
// block in the log buffer. Returns the number of bytes consumed.
func (sub *Subscription) RawPoll(handler term.RawBlockHandler, blockLengthLimit int32) int64 {
	var bytesConsumed int64
	for _, image := range sub.images.Get() {
		bytesConsumed += int64(image.RawPoll(handler, blockLengthLimit))
	}
	return bytesConsumed
}

// HasNoImages returns true if the subscription has no images associated with it.
func (sub *Subscription) HasNoImages() bool {
	return len(sub.images.Get()) == 0
}

// ImageAtIndex returns the image at the given index of the current image list, or nil if out of range.
func (sub *Subscription) ImageAtIndex(index int) Image {
	images := sub.images.Get()
	if index < 0 || index >= len(images) {
		return nil
	}
	return images[index]
}

// Images returns a snapshot copy of the images currently associated with this subscription.
func (sub *Subscription) Images() []Image {
	images := sub.images.Get()
	result := make([]Image, len(images))
	copy(result, images)
	return result
}

// ForEachImage iterates over the current snapshot of images without allocating.
func (sub *Subscription) ForEachImage(fn func(image Image)) {
	for _, image := range sub.images.Get() {
		fn(image)
	}
}

// RejectImage asks the driver to reject the image with the given correlation id at the given position, passing the
// reason to the publisher (Aeron 1.47+). Not valid for spy subscriptions.
func (sub *Subscription) RejectImage(imageCorrelationID int64, position int64, reason string) error {
	if strings.HasPrefix(sub.channel, SpyPrefix) {
		return ErrSpyRejectImage
	}
	if sub.IsClosed() {
		return errors.New("subscription is closed")
	}
	return sub.conductor.rejectImage(imageCorrelationID, position, reason)
}

// String returns a description of the subscription.
func (sub *Subscription) String() string {
	return fmt.Sprintf("Subscription{registrationId=%d, isClosed=%t, streamId=%d, channel='%s', imageCount=%d}",
		sub.registrationID, sub.IsClosed(), sub.streamID, sub.channel, sub.ImageCount())
}
