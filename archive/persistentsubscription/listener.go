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

// Listener receives PersistentSubscription notifications. Mirrors io.aeron.archive.client.PersistentSubscriptionListener.
// Callbacks are invoked from Poll/ControlledPoll on the polling goroutine.
type Listener interface {
	// OnLiveJoined is called when the PersistentSubscription transitions to consuming from the live channel.
	OnLiveJoined()
	// OnLiveLeft is called when the PersistentSubscription stops consuming from the live channel.
	OnLiveLeft()
	// OnError is called for both terminal and non-terminal errors. See PersistentSubscription.HasFailed.
	OnError(err error)
}

// ListenerFuncs adapts optional functions to the Listener interface. Nil functions are ignored.
type ListenerFuncs struct {
	LiveJoined func()
	LiveLeft   func()
	Error      func(err error)
}

// OnLiveJoined implements Listener.
func (l ListenerFuncs) OnLiveJoined() {
	if l.LiveJoined != nil {
		l.LiveJoined()
	}
}

// OnLiveLeft implements Listener.
func (l ListenerFuncs) OnLiveLeft() {
	if l.LiveLeft != nil {
		l.LiveLeft()
	}
}

// OnError implements Listener.
func (l ListenerFuncs) OnError(err error) {
	if l.Error != nil {
		l.Error(err)
	}
}
