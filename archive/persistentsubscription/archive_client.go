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

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive"
	"github.com/lirm/aeron-go/archive/codecs"
)

// archiveListener mirrors Java's AsyncAeronArchiveListener.
type archiveListener interface {
	onConnected()
	onDisconnected()
	onControlResponse(correlationID int64, relevantID int64, code codecs.ControlResponseCodeEnum, errorMessage string)
	onRecordingDescriptor(descriptor *codecs.RecordingDescriptor)
	onArchiveError(err error)
}

// archiveClient mirrors Java's AsyncAeronArchive: a non-blocking archive client driven by poll().
type archiveClient interface {
	poll() int
	isConnected() bool
	isClosed() bool
	trySendListRecordingRequest(correlationID int64, recordingID int64) bool
	trySendMaxRecordedPositionRequest(correlationID int64, recordingID int64) bool
	trySendReplayTokenRequest(correlationID int64, recordingID int64) bool
	trySendReplayRequest(correlationID int64, recordingID int64, position int64, length int64,
		replayChannel string, replayStreamID int32) bool
	// trySendReplayRequestOn sends a response-channel replay request on a dedicated request publication.
	trySendReplayRequestOn(publication requestPublication, correlationID int64, recordingID int64,
		position int64, length int64, replayChannel string, replayStreamID int32, replayToken int64) bool
	trySendStopReplayRequest(correlationID int64, replaySessionID int64) bool
	close()
}

type asyncArchiveState int

const (
	asyncArchiveConnecting asyncArchiveState = iota
	asyncArchiveConnected
	asyncArchiveDisconnected
	asyncArchiveClosed
)

type connectResult struct {
	archive *archive.Archive
	err     error
}

// connectRetryInterval paces reconnection attempts after a failed connect so an absent driver/archive does not
// produce an error storm.
const connectRetryInterval = 100 * time.Millisecond

// asyncArchive implements archiveClient over the (blocking) archive.Archive. Connection establishment runs on a
// background goroutine; everything else is non-blocking and runs on the polling goroutine.
type asyncArchive struct {
	options      archive.Options
	aeronContext func() *aeron.Context
	listener     archiveListener

	state         asyncArchiveState
	archive       *archive.Archive
	connecting    chan connectResult
	nextConnectNs int64
	nanoClock     func() int64

	proxies       map[*aeron.ExclusivePublication]*archive.Proxy
	assembler     *aeron.ControlledFragmentAssembler
	marshaller    *codecs.SbeGoMarshaller
	rangeChecking bool

	connectArchive func(options *archive.Options, ctx *aeron.Context) (*archive.Archive, error)
	closeOnce      sync.Once
}

func newAsyncArchive(options *archive.Options, aeronContext func() *aeron.Context, listener archiveListener,
	nanoClock func() int64) *asyncArchive {
	a := &asyncArchive{
		options:        *options,
		aeronContext:   aeronContext,
		listener:       listener,
		state:          asyncArchiveConnecting,
		nanoClock:      nanoClock,
		proxies:        map[*aeron.ExclusivePublication]*archive.Proxy{},
		marshaller:     codecs.NewSbeGoMarshaller(),
		rangeChecking:  options.RangeChecking,
		connectArchive: archive.NewArchive,
	}
	a.assembler = aeron.NewControlledFragmentAssembler(a.onFragment, aeron.DefaultFragmentAssemblyBufferLength)
	return a
}

func (a *asyncArchive) poll() int {
	switch a.state {
	case asyncArchiveConnecting:
		return a.pollConnecting()
	case asyncArchiveConnected:
		if !a.archive.Control.Subscription.IsConnected() {
			a.state = asyncArchiveDisconnected
			return 1
		}
		return a.archive.Control.Subscription.ControlledPoll(a.assembler.OnFragment, 10)
	case asyncArchiveDisconnected:
		a.closeArchiveAsync()
		a.state = asyncArchiveConnecting
		a.listener.onDisconnected()
		return 1
	}
	return 0
}

func (a *asyncArchive) pollConnecting() int {
	if a.connecting == nil {
		if a.nanoClock()-a.nextConnectNs < 0 {
			return 0
		}
		ch := make(chan connectResult, 1)
		a.connecting = ch
		options := a.options
		ctx := a.aeronContext()
		connect := a.connectArchive
		go func() {
			arch, err := connect(&options, ctx)
			ch <- connectResult{archive: arch, err: err}
		}()
		return 1
	}
	select {
	case result := <-a.connecting:
		a.connecting = nil
		if result.err != nil {
			a.nextConnectNs = a.nanoClock() + int64(connectRetryInterval)
			a.listener.onArchiveError(fmt.Errorf("archive connect failed: %w", result.err))
			return 1
		}
		a.archive = result.archive
		a.proxies = map[*aeron.ExclusivePublication]*archive.Proxy{}
		a.state = asyncArchiveConnected
		a.listener.onConnected()
		return 1
	default:
		return 0
	}
}

// closeArchiveAsync closes the current archive session without blocking the duty cycle (archive.Close blocks
// while offering the close session request).
func (a *asyncArchive) closeArchiveAsync() {
	if arch := a.archive; arch != nil {
		a.archive = nil
		go func() { _ = arch.Close() }()
	}
}

func (a *asyncArchive) isConnected() bool {
	return a.state == asyncArchiveConnected
}

func (a *asyncArchive) isClosed() bool {
	return a.state == asyncArchiveClosed
}

func (a *asyncArchive) close() {
	a.closeOnce.Do(func() {
		a.state = asyncArchiveClosed
		if arch := a.archive; arch != nil {
			a.archive = nil
			_ = arch.Close()
		}
		if ch := a.connecting; ch != nil {
			a.connecting = nil
			go func() {
				if result := <-ch; result.archive != nil {
					_ = result.archive.Close()
				}
			}()
		}
	})
}

func (a *asyncArchive) send(f func(proxy *archive.Proxy) (bool, error)) bool {
	if a.state != asyncArchiveConnected {
		return false
	}
	return a.sendWith(a.archive.Proxy, f)
}

func (a *asyncArchive) sendWith(proxy *archive.Proxy, f func(proxy *archive.Proxy) (bool, error)) bool {
	sent, err := f(proxy)
	if err != nil {
		if errors.Is(err, archive.ErrProxyNotConnected) {
			if proxy == a.archive.Proxy {
				a.state = asyncArchiveDisconnected
			}
		} else {
			a.listener.onArchiveError(err)
		}
		return false
	}
	return sent
}

func (a *asyncArchive) trySendListRecordingRequest(correlationID int64, recordingID int64) bool {
	return a.send(func(p *archive.Proxy) (bool, error) { return p.TryListRecordingRequest(correlationID, recordingID) })
}

func (a *asyncArchive) trySendMaxRecordedPositionRequest(correlationID int64, recordingID int64) bool {
	return a.send(func(p *archive.Proxy) (bool, error) {
		return p.TryMaxRecordedPositionRequest(correlationID, recordingID)
	})
}

func (a *asyncArchive) trySendReplayTokenRequest(correlationID int64, recordingID int64) bool {
	return a.send(func(p *archive.Proxy) (bool, error) { return p.TryReplayTokenRequest(correlationID, recordingID) })
}

func (a *asyncArchive) trySendReplayRequest(correlationID int64, recordingID int64, position int64, length int64,
	replayChannel string, replayStreamID int32) bool {
	return a.send(func(p *archive.Proxy) (bool, error) {
		return p.TryReplayRequestWithToken(correlationID, recordingID, position, length, replayStreamID,
			replayChannel, aeron.NullValue, aeron.NullValue)
	})
}

func (a *asyncArchive) trySendReplayRequestOn(publication requestPublication, correlationID int64, recordingID int64,
	position int64, length int64, replayChannel string, replayStreamID int32, replayToken int64) bool {
	pub := publicationOf(publication)
	if a.state != asyncArchiveConnected || pub == nil {
		return false
	}
	proxy, ok := a.proxies[pub]
	if !ok {
		proxy = a.archive.NewProxyForPublication(pub)
		a.proxies[pub] = proxy
	}
	return a.sendWith(proxy, func(p *archive.Proxy) (bool, error) {
		return p.TryReplayRequestWithToken(correlationID, recordingID, position, length, replayStreamID,
			replayChannel, aeron.NullValue, replayToken)
	})
}

func (a *asyncArchive) trySendStopReplayRequest(correlationID int64, replaySessionID int64) bool {
	return a.send(func(p *archive.Proxy) (bool, error) {
		return p.TryStopReplayRequest(correlationID, replaySessionID)
	})
}

var (
	controlResponseTemplateID     = (&codecs.ControlResponse{}).SbeTemplateId()
	recordingDescriptorTemplateID = (&codecs.RecordingDescriptor{}).SbeTemplateId()
	archiveSchemaID               = (&codecs.ControlResponse{}).SbeSchemaId()
)

func (a *asyncArchive) onFragment(buffer *atomic.Buffer, offset int32, length int32,
	_ *logbuffer.Header) term.ControlledPollAction {
	if a.state != asyncArchiveConnected {
		return term.ControlledPollActionAbort
	}
	buf := new(bytes.Buffer)
	buffer.WriteBytes(buf, offset, length)

	var hdr codecs.SbeGoMessageHeader
	if err := hdr.Decode(a.marshaller, buf); err != nil {
		a.listener.onArchiveError(fmt.Errorf("failed to decode archive control message header: %w", err))
		return term.ControlledPollActionContinue
	}
	if hdr.SchemaId != archiveSchemaID {
		a.listener.onArchiveError(fmt.Errorf("expected archive schemaId=%d, actual=%d", archiveSchemaID, hdr.SchemaId))
		return term.ControlledPollActionContinue
	}

	switch hdr.TemplateId {
	case controlResponseTemplateID:
		var response codecs.ControlResponse
		if err := response.Decode(a.marshaller, buf, hdr.Version, hdr.BlockLength, a.rangeChecking); err != nil {
			a.listener.onArchiveError(fmt.Errorf("failed to decode control response: %w", err))
			return term.ControlledPollActionContinue
		}
		if response.ControlSessionId == a.archive.SessionID {
			a.listener.onControlResponse(response.CorrelationId, response.RelevantId, response.Code,
				string(response.ErrorMessage))
		}
	case recordingDescriptorTemplateID:
		var descriptor codecs.RecordingDescriptor
		if err := descriptor.Decode(a.marshaller, buf, hdr.Version, hdr.BlockLength, a.rangeChecking); err != nil {
			a.listener.onArchiveError(fmt.Errorf("failed to decode recording descriptor: %w", err))
			return term.ControlledPollActionContinue
		}
		if descriptor.ControlSessionId == a.archive.SessionID {
			a.listener.onRecordingDescriptor(&descriptor)
		}
	}
	return term.ControlledPollActionContinue
}
