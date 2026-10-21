/*
Copyright 2026 Kalshi Trading

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
	"github.com/lirm/aeron-go/aeron/status"
)

// PublicationErrorFrameHandler is called on the conductor goroutine when the driver propagates an error frame
// received by a publication this client added (e.g. a subscriber invalidated its image). The frame is only valid for
// the duration of the callback; copy the value to retain it. Mirrors io.aeron.PublicationErrorFrameHandler.
type PublicationErrorFrameHandler func(frame *status.PublicationErrorFrame)

// PublicationErrorFrameHandler sets the handler for publication error frames. Default is no-op.
func (ctx *Context) PublicationErrorFrameHandler(handler PublicationErrorFrameHandler) *Context {
	ctx.publicationErrorFrameHandler = handler
	return ctx
}

// OnPublicationError implements driver.PublicationErrorListener. As in Java, the handler is called once per
// publication instance whose original registration id matches the frame.
func (cc *ClientConductor) OnPublicationError(frame *status.PublicationErrorFrame) {
	handler := cc.publicationErrorFrameHandler
	if handler == nil {
		return
	}
	cc.adminLock.Lock()
	matches := 0
	for _, pub := range cc.pubs {
		if pub.origRegID == frame.RegistrationID && pub.publication != nil {
			matches++
		}
	}
	cc.adminLock.Unlock()
	for i := 0; i < matches; i++ {
		handler(frame)
	}
}
