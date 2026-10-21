// Copyright (C) 2026 Kalshi Trading.
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

	"github.com/lirm/aeron-go/aeron"
)

// ArchiveState is the lifecycle state of an Archive, equivalent to Java AeronArchive.State.
type ArchiveState int32

const (
	ArchiveStateConnected ArchiveState = iota
	ArchiveStateDisconnected
	ArchiveStateClosed
)

func (s ArchiveState) String() string {
	switch s {
	case ArchiveStateConnected:
		return "CONNECTED"
	case ArchiveStateDisconnected:
		return "DISCONNECTED"
	case ArchiveStateClosed:
		return "CLOSED"
	}
	return "UNKNOWN"
}

func (archive *Archive) setState(s ArchiveState) {
	for {
		old := archive.state.Load()
		if ArchiveState(old) == ArchiveStateClosed {
			return
		}
		if archive.state.CompareAndSwap(old, int32(s)) {
			return
		}
	}
}

// State returns the lifecycle state. The state becomes DISCONNECTED when the control response image goes away.
func (archive *Archive) State() ArchiveState { return ArchiveState(archive.state.Load()) }

// IsClosed reports whether Close has been called.
func (archive *Archive) IsClosed() bool { return archive.State() == ArchiveStateClosed }

// ControlSessionID returns the control session id allocated by the archive (same as SessionID).
func (archive *Archive) ControlSessionID() int64 { return archive.SessionID }

// ArchiveID returns the archive id negotiated on connect, or aeron.NullValue if the archive predates protocol 1.11.
func (archive *Archive) ArchiveID() int64 { return archive.archiveID }

// Close sends a CloseSessionRequest to the archive and releases resources, equivalent to Java AeronArchive.close().
// When Options.OwnsAeronClient is true the Aeron client is closed, otherwise only the control publication,
// control subscription and any recording events subscription are closed. Errors are passed to the
// ErrorListener and returned. Close is idempotent.
func (archive *Archive) Close() error {
	archive.mtx.Lock()
	defer archive.mtx.Unlock()

	if archive.State() == ArchiveStateClosed {
		return nil
	}

	var errs []error
	if pub := archive.Proxy.Publication; pub != nil && pub.IsConnected() && archive.SessionID != aeron.NullValue {
		if err := archive.Proxy.CloseSessionRequest(); err != nil {
			errs = append(errs, err)
		}
	}
	// Set after the close session request, which is refused once the client is closed.
	archive.state.Store(int32(ArchiveStateClosed))

	if archive.Options.OwnsAeronClient {
		if archive.aeron != nil {
			errs = append(errs, archive.aeron.Close())
		}
	} else {
		if archive.Events != nil && archive.Events.Enabled && archive.Events.Subscription != nil {
			errs = append(errs, archive.Events.Subscription.Close())
		}
		if archive.Proxy.Publication != nil {
			errs = append(errs, archive.Proxy.Publication.Close())
		}
		if archive.Control.Subscription != nil {
			errs = append(errs, archive.Control.Subscription.Close())
		}
	}

	err := errors.Join(errs...)
	if err != nil && archive.Listeners.ErrorListener != nil {
		archive.Listeners.ErrorListener(err)
	}
	return err
}
