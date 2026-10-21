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
	"sync"

	"github.com/lirm/aeron-go/aeron"
)

// subscriptionHandle is the view of a subscription needed by the state machine. Images are reported as closed as
// soon as the driver signals them unavailable (Go's client keeps unavailable images lingering).
type subscriptionHandle interface {
	registrationID() int64
	imageCount() int
	imageAtIndex(index int) aeron.Image
	imageBySessionID(sessionID int32) aeron.Image
	resolvedEndpoint() string
}

// requestPublication is an exclusive publication used to send replay requests for response channels.
type requestPublication interface {
	isConnected() bool
	publisher() aeron.Publisher
}

// aeronClient is the subset of the aeron client used by PersistentSubscription. Abstracted for unit testing.
type aeronClient interface {
	nextCorrelationID() int64
	asyncAddSubscription(channel string, streamID int32) (int64, error)
	// getSubscription returns (nil, nil) while pending; errors are *RegistrationError.
	getSubscription(registrationID int64) (subscriptionHandle, error)
	asyncAddExclusivePublication(channel string, streamID int32) (int64, error)
	getExclusivePublication(registrationID int64) (requestPublication, error)
	// asyncRemoveSubscription / asyncRemovePublication release a resource that may still be pending.
	asyncRemoveSubscription(registrationID int64)
	asyncRemovePublication(registrationID int64)
	// doWork releases resources whose registrations completed after they were removed.
	doWork() int
	isClosed() bool
}

type trackedImage struct {
	aeron.Image
	mu          *sync.Mutex
	unavailable bool
}

func (t *trackedImage) IsClosed() bool {
	t.mu.Lock()
	unavailable := t.unavailable
	t.mu.Unlock()
	return unavailable || t.Image.IsClosed()
}

type trackedSubscription struct {
	id     int64
	sub    *aeron.Subscription
	mu     sync.Mutex
	images []*trackedImage
}

func (s *trackedSubscription) onAvailable(image aeron.Image) {
	s.mu.Lock()
	s.images = append(s.images, &trackedImage{Image: image, mu: &s.mu})
	s.mu.Unlock()
}

func (s *trackedSubscription) onUnavailable(image aeron.Image) {
	s.mu.Lock()
	for i, t := range s.images {
		if t.Image.CorrelationID() == image.CorrelationID() {
			t.unavailable = true
			s.images = append(s.images[:i:i], s.images[i+1:]...)
			break
		}
	}
	s.mu.Unlock()
}

func (s *trackedSubscription) registrationID() int64 {
	return s.id
}

func (s *trackedSubscription) imageCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.images)
}

func (s *trackedSubscription) imageAtIndex(index int) aeron.Image {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < 0 || index >= len(s.images) {
		return nil
	}
	return s.images[index]
}

func (s *trackedSubscription) imageBySessionID(sessionID int32) aeron.Image {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.images {
		if t.Image.SessionID() == sessionID {
			return t
		}
	}
	return nil
}

func (s *trackedSubscription) resolvedEndpoint() string {
	return s.sub.TryResolveChannelEndpointPort()
}

type exclusivePublication struct {
	pub *aeron.ExclusivePublication
}

func (p *exclusivePublication) isConnected() bool {
	return p.pub.IsConnected()
}

func (p *exclusivePublication) publisher() aeron.Publisher {
	if p.pub == nil {
		return nil
	}
	return p.pub
}

// realAeronClient adapts *aeron.Aeron. It is only used from the polling goroutine except for image handlers.
type realAeronClient struct {
	aeron         *aeron.Aeron
	subscriptions map[int64]*trackedSubscription
	publications  map[int64]*exclusivePublication
	orphanSubs    []int64
	orphanPubs    []int64
}

func newRealAeronClient(a *aeron.Aeron) *realAeronClient {
	return &realAeronClient{
		aeron:         a,
		subscriptions: map[int64]*trackedSubscription{},
		publications:  map[int64]*exclusivePublication{},
	}
}

func (c *realAeronClient) asyncAddSubscription(channel string, streamID int32) (int64, error) {
	tracked := &trackedSubscription{}
	id, err := c.aeron.AsyncAddSubscriptionWithHandlers(channel, streamID, tracked.onAvailable, tracked.onUnavailable)
	if err != nil {
		return 0, err
	}
	tracked.id = id
	c.subscriptions[id] = tracked
	return id, nil
}

func (c *realAeronClient) getSubscription(registrationID int64) (subscriptionHandle, error) {
	tracked, ok := c.subscriptions[registrationID]
	if !ok {
		return nil, toRegistrationError(registrationID, errUnknownRegistration)
	}
	if tracked.sub != nil {
		return tracked, nil
	}
	sub, err := c.aeron.GetSubscription(registrationID)
	if err != nil {
		delete(c.subscriptions, registrationID)
		return nil, toRegistrationError(registrationID, err)
	}
	if sub == nil {
		return nil, nil
	}
	tracked.sub = sub
	return tracked, nil
}

func (c *realAeronClient) asyncAddExclusivePublication(channel string, streamID int32) (int64, error) {
	id, err := c.aeron.AsyncAddExclusivePublication(channel, streamID)
	if err != nil {
		return 0, err
	}
	c.publications[id] = &exclusivePublication{}
	return id, nil
}

func (c *realAeronClient) getExclusivePublication(registrationID int64) (requestPublication, error) {
	tracked, ok := c.publications[registrationID]
	if !ok {
		return nil, toRegistrationError(registrationID, errUnknownRegistration)
	}
	if tracked.pub != nil {
		return tracked, nil
	}
	pub, err := c.aeron.GetExclusivePublication(registrationID)
	if err != nil {
		delete(c.publications, registrationID)
		return nil, toRegistrationError(registrationID, err)
	}
	if pub == nil {
		return nil, nil
	}
	tracked.pub = pub
	return tracked, nil
}

func (c *realAeronClient) asyncRemoveSubscription(registrationID int64) {
	tracked, ok := c.subscriptions[registrationID]
	if !ok {
		return
	}
	if tracked.sub != nil {
		delete(c.subscriptions, registrationID)
		_ = tracked.sub.Close()
		return
	}
	c.orphanSubs = append(c.orphanSubs, registrationID)
}

func (c *realAeronClient) asyncRemovePublication(registrationID int64) {
	tracked, ok := c.publications[registrationID]
	if !ok {
		return
	}
	if tracked.pub != nil {
		delete(c.publications, registrationID)
		_ = tracked.pub.Close()
		return
	}
	c.orphanPubs = append(c.orphanPubs, registrationID)
}

func (c *realAeronClient) doWork() int {
	work := 0
	if len(c.orphanSubs) > 0 {
		remaining := c.orphanSubs[:0]
		for _, id := range c.orphanSubs {
			if sub, err := c.getSubscription(id); err != nil {
				work++
			} else if sub != nil {
				c.asyncRemoveSubscription(id)
				work++
			} else {
				remaining = append(remaining, id)
			}
		}
		c.orphanSubs = remaining
	}
	if len(c.orphanPubs) > 0 {
		remaining := c.orphanPubs[:0]
		for _, id := range c.orphanPubs {
			if pub, err := c.getExclusivePublication(id); err != nil {
				work++
			} else if pub != nil {
				c.asyncRemovePublication(id)
				work++
			} else {
				remaining = append(remaining, id)
			}
		}
		c.orphanPubs = remaining
	}
	return work
}

func (c *realAeronClient) nextCorrelationID() int64 {
	return c.aeron.NextCorrelationID()
}

// hasPending reports whether removed registrations are still awaiting completion.
func (c *realAeronClient) hasPending() bool {
	return len(c.orphanSubs) > 0 || len(c.orphanPubs) > 0
}

func (c *realAeronClient) isClosed() bool {
	return c.aeron.IsClosed()
}

// publication returns the underlying aeron publication for a resolved request publication.
