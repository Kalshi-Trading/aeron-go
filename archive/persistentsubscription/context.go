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
	"errors"
	"fmt"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/archive"
)

const (
	// FromStart starts the persistent subscription from the start of the recording (Java's FROM_START).
	FromStart int64 = -1
	// FromLive starts the persistent subscription from the live stream (Java's FROM_LIVE).
	FromLive int64 = -2
)

// counterAllocationTimeout bounds how long Context.conclude waits for the driver to allocate counters.
const counterAllocationTimeout = 10 * time.Second

// ErrConfiguration is wrapped by all Context validation errors (Java's ConfigurationException).
var ErrConfiguration = errors.New("invalid persistent subscription configuration")

// Context configures a PersistentSubscription. Mirrors PersistentSubscription.Context. Create with NewContext,
// set the required fields (RecordingID, LiveChannel, LiveStreamID, ReplayChannel, ReplayStreamID) and pass to New.
// A Context may only be used for a single PersistentSubscription.
type Context struct {
	// Aeron client used for the live and replay subscriptions and counters. If nil, a client is created during
	// New using AeronDir and OwnsAeronClient is set to true.
	Aeron *aeron.Aeron
	// OwnsAeronClient closes Aeron when the PersistentSubscription is closed.
	OwnsAeronClient bool
	// AeronDir is the media driver directory for the Aeron client(s) this package creates. Empty uses the default.
	AeronDir string

	// RecordingID of the recording of the live stream. Required.
	RecordingID int64
	// StartPosition in the recording to start from, or FromStart / FromLive. Defaults to FromLive.
	StartPosition int64

	// LiveChannel and LiveStreamID of the live stream (the stream being recorded). Required.
	LiveChannel  string
	LiveStreamID int32
	// ReplayChannel and ReplayStreamID to receive replays on. Required. The channel may be session specific
	// (e.g. "aeron:udp?endpoint=localhost:0" is a dynamic port, "aeron:ipc" or a fixed endpoint get the replay
	// session id appended) or a response channel ("control-mode=response").
	ReplayChannel  string
	ReplayStreamID int32

	// Listener for live joined/left and error notifications. Defaults to a no-op listener.
	Listener Listener

	// ArchiveOptions for the archive control session (channels, streams, message timeout). The message timeout
	// (Options.Timeout) bounds archive request/response round trips and image availability. Defaults to
	// archive.DefaultOptions().
	ArchiveOptions *archive.Options
	// ArchiveAeronContext is the context for the archive client's own Aeron client. If nil, a fresh context using
	// AeronDir is created for every connection attempt.
	ArchiveAeronContext *aeron.Context

	// Counters reporting state (StateCounterTypeID), join difference, live left and live joined counts. If nil they
	// are allocated during New. They are closed when the PersistentSubscription is closed.
	StateCounter          *aeron.Counter
	JoinDifferenceCounter *aeron.Counter
	LiveLeftCounter       *aeron.Counter
	LiveJoinedCounter     *aeron.Counter

	// NanoClock is the monotonic clock in nanoseconds used for deadlines. Defaults to time since creation.
	NanoClock func() int64

	concluded bool
}

// NewContext returns a Context with Java-equivalent defaults (unset ids are aeron.NullValue).
func NewContext() *Context {
	return &Context{
		RecordingID:    aeron.NullValue,
		StartPosition:  FromLive,
		LiveStreamID:   aeron.NullValue,
		ReplayStreamID: aeron.NullValue,
	}
}

func configError(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s", ErrConfiguration, fmt.Sprintf(format, args...))
}

// validate checks the configuration without allocating any resources.
func (ctx *Context) validate() error {
	if ctx.concluded {
		return configError("context has already been concluded")
	}
	if ctx.RecordingID == aeron.NullValue {
		return configError("recordingId must be set")
	}
	if ctx.LiveStreamID == aeron.NullValue {
		return configError("liveStreamId must be set")
	}
	if ctx.LiveChannel == "" {
		return configError("liveChannel must be set")
	}
	if ctx.ReplayChannel == "" {
		return configError("replayChannel must be set")
	}
	if ctx.ReplayStreamID == aeron.NullValue {
		return configError("replayStreamId must be set")
	}
	if ctx.RecordingID < 0 {
		return configError("invalid recordingId %d", ctx.RecordingID)
	}
	if ctx.StartPosition < FromLive {
		return configError("invalid startPosition %d", ctx.StartPosition)
	}
	if ctx.StartPosition >= 0 && ctx.StartPosition&(int64(logbuffer.FrameAlignment)-1) != 0 {
		return configError("startPosition %d is not frame aligned", ctx.StartPosition)
	}
	replayURI, err := aeron.ParseChannelUri(ctx.ReplayChannel)
	if err != nil {
		return configError("invalid replayChannel %q: %v", ctx.ReplayChannel, err)
	}
	if _, err := aeron.ParseChannelUri(ctx.LiveChannel); err != nil {
		return configError("invalid liveChannel %q: %v", ctx.LiveChannel, err)
	}
	if replayURI.Get("control-mode") == "response" {
		requestChannel := ctx.archiveOptions().RequestChannel
		if requestChannel != "" {
			requestURI, err := aeron.ParseChannelUri(requestChannel)
			if err != nil {
				return configError("invalid archive request channel %q: %v", requestChannel, err)
			}
			if replayURI.IsIpc() != requestURI.IsIpc() {
				return configError("Channel media type mismatch. When using `control-mode=response`, the " +
					"`replayChannel` media type must match the media type for the archive control channel.")
			}
		}
	}
	return nil
}

func (ctx *Context) archiveOptions() *archive.Options {
	if ctx.ArchiveOptions == nil {
		ctx.ArchiveOptions = archive.DefaultOptions()
	}
	return ctx.ArchiveOptions
}

// archiveConnectOptions returns a copy of the archive options used for each archive connection. Like Java's
// Context.conclude (aeronArchiveContext.aeron(aeron)), the archive shares the persistent subscription's Aeron
// client unless ArchiveOptions.Aeron or ArchiveAeronContext asks for a dedicated one.
func (ctx *Context) archiveConnectOptions() *archive.Options {
	options := *ctx.archiveOptions()
	if options.Aeron == nil && ctx.ArchiveAeronContext == nil && ctx.Aeron != nil {
		options.Aeron = ctx.Aeron
		options.OwnsAeronClient = false
	}
	return &options
}

func (ctx *Context) newArchiveAeronContext() *aeron.Context {
	if ctx.ArchiveAeronContext != nil {
		return ctx.ArchiveAeronContext
	}
	c := aeron.NewContext()
	if ctx.AeronDir != "" {
		c.AeronDir(ctx.AeronDir)
	}
	return c
}

// conclude validates the context and allocates the Aeron client and counters if not supplied.
func (ctx *Context) conclude() error {
	if err := ctx.validate(); err != nil {
		return err
	}
	ctx.concluded = true

	if ctx.Listener == nil {
		ctx.Listener = ListenerFuncs{}
	}
	replayURI, _ := aeron.ParseChannelUri(ctx.ReplayChannel)
	replayURI.Set("rejoin", "false")
	ctx.ReplayChannel = replayURI.String()
	ctx.archiveOptions()

	if ctx.NanoClock == nil {
		start := time.Now()
		ctx.NanoClock = func() int64 { return int64(time.Since(start)) }
	}

	if ctx.Aeron == nil {
		aeronCtx := aeron.NewContext()
		if ctx.AeronDir != "" {
			aeronCtx.AeronDir(ctx.AeronDir)
		}
		a, err := aeron.Connect(aeronCtx)
		if err != nil {
			return fmt.Errorf("persistent subscription failed to connect aeron client: %w", err)
		}
		ctx.Aeron = a
		ctx.OwnsAeronClient = true
	}

	counters := []struct {
		counter **aeron.Counter
		name    string
		typeID  int32
	}{
		{&ctx.StateCounter, "Persistent Subscription State", StateCounterTypeID},
		{&ctx.JoinDifferenceCounter, "Persistent Subscription Join Difference", JoinDifferenceCounterTypeID},
		{&ctx.LiveLeftCounter, "Persistent Subscription Live Left Count", LiveLeftCountCounterTypeID},
		{&ctx.LiveJoinedCounter, "Persistent Subscription Live Joined Count", LiveJoinedCountCounterTypeID},
	}
	for _, c := range counters {
		if *c.counter != nil {
			continue
		}
		counter, err := ctx.allocateCounter(c.name, c.typeID)
		if err != nil {
			ctx.close()
			return err
		}
		*c.counter = counter
	}
	return nil
}

func (ctx *Context) allocateCounter(name string, typeID int32) (*aeron.Counter, error) {
	label := fmt.Sprintf("%s: %d %s %d %s", name, ctx.ReplayStreamID, ctx.ReplayChannel, ctx.LiveStreamID,
		ctx.LiveChannel)
	id, err := ctx.Aeron.AsyncAddCounterByLabel(typeID, label)
	if err != nil {
		return nil, fmt.Errorf("failed to add counter %q: %w", label, err)
	}
	deadline := time.Now().Add(counterAllocationTimeout)
	for {
		counter, err := ctx.Aeron.GetCounter(id)
		if err != nil {
			return nil, fmt.Errorf("failed to add counter %q: %w", label, err)
		}
		if counter != nil {
			return counter, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out adding counter %q", label)
		}
		time.Sleep(time.Millisecond)
	}
}

// close releases the resources owned by the context: the Aeron client if owned, otherwise the counters.
func (ctx *Context) close() {
	if ctx.OwnsAeronClient && ctx.Aeron != nil {
		_ = ctx.Aeron.Close()
		return
	}
	for _, c := range []*aeron.Counter{ctx.StateCounter, ctx.JoinDifferenceCounter, ctx.LiveLeftCounter,
		ctx.LiveJoinedCounter} {
		if c != nil && !c.IsClosed() {
			_ = c.Close()
		}
	}
}
