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
	"time"

	"github.com/lirm/aeron-go/aeron/driver"
)

// PublicationErrorFrame is an error frame received by a publication from a remote receiver.
type PublicationErrorFrame = driver.PublicationErrorFrame

// PublicationErrorFrameHandler is notified of ON_PUBLICATION_ERROR events for publications of this client.
// The frame must not be retained by the handler beyond the call unless copied.
type PublicationErrorFrameHandler func(frame *PublicationErrorFrame)

type conductorOptions struct {
	closeHandler                 func()
	keepAliveInterval            time.Duration
	closeLinger                  time.Duration
	publicationErrorFrameHandler PublicationErrorFrameHandler
}

// CloseHandler sets a handler invoked once when the client closes (explicitly or due to a fatal timeout).
// Equivalent to Aeron.AddCloseHandler before connect.
func (ctx *Context) CloseHandler(handler func()) *Context {
	ctx.conductorOpts.closeHandler = handler
	return ctx
}

// KeepAliveInterval sets the interval at which the client checks driver liveness and updates its heartbeat
// (default 500ms, matching Java aeron.client.keepalive.interval).
func (ctx *Context) KeepAliveInterval(interval time.Duration) *Context {
	ctx.conductorOpts.keepAliveInterval = interval
	return ctx
}

// CloseLingerDuration sets how long Close lingers before unmapping remaining log buffers (default 0, matching Java
// aeron.client.close.linger.duration).
func (ctx *Context) CloseLingerDuration(d time.Duration) *Context {
	ctx.conductorOpts.closeLinger = d
	return ctx
}

// PublicationErrorFrameHandler sets the handler for publication error frames.
func (ctx *Context) PublicationErrorFrameHandler(handler PublicationErrorFrameHandler) *Context {
	ctx.conductorOpts.publicationErrorFrameHandler = handler
	return ctx
}
