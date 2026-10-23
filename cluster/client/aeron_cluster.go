// Copyright 2022 Steven Stern
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

// Package client is a Go port of io.aeron.cluster.client.AeronCluster (Aeron 1.53.3, cluster schema v17).
package client

import (
	"fmt"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/aeron/logging"
	"github.com/lirm/aeron-go/cluster/codecs"
)

var logger = logging.MustGetLogger("cluster-client")
var marshaller = codecs.NewSbeGoMarshaller()

const (
	sendAttempts  = 3
	fragmentLimit = 10
)

type clientState int8

const (
	stateConnected clientState = iota
	stateAwaitNewLeader
	stateAwaitNewLeaderConnection
	statePendingClose
	stateClosed
)

func (s clientState) String() string {
	switch s {
	case stateConnected:
		return "CONNECTED"
	case stateAwaitNewLeader:
		return "AWAIT_NEW_LEADER"
	case stateAwaitNewLeaderConnection:
		return "AWAIT_NEW_LEADER_CONNECTION"
	case statePendingClose:
		return "PENDING_CLOSE"
	case stateClosed:
		return "CLOSED"
	}
	return "UNKNOWN"
}

// AeronCluster is a client for interacting with an Aeron Cluster, mirroring io.aeron.cluster.client.AeronCluster.
// Create with Connect or NewAsyncConnect. Not safe for concurrent use.
type AeronCluster struct {
	ctx                         *Context
	clusterSessionId            int64
	leadershipTermId            int64
	leaderMemberId              int32
	subscription                *aeron.Subscription
	egressImage                 aeron.Image
	publication                 aeron.Publisher
	state                       clientState
	stateDeadline               time.Time
	bufferClaim                 logbuffer.Claim
	headerBuffer                *atomic.Buffer
	fragmentAssembler           *aeron.FragmentAssembler
	controlledFragmentAssembler *aeron.ControlledFragmentAssembler
	egressListener              EgressListener
	controlledEgressListener    ControlledEgressListener
	egressExtension             EgressListenerExtension
	controlledEgressExtension   ControlledEgressListenerExtension
	endpointByIdMap             map[int32]*memberIngress
}

func newAeronCluster(
	ctx *Context,
	publication aeron.Publisher,
	subscription *aeron.Subscription,
	egressImage aeron.Image,
	clusterSessionId int64,
	leadershipTermId int64,
	leaderMemberId int32,
) *AeronCluster {
	ac := &AeronCluster{
		ctx:                      ctx,
		clusterSessionId:         clusterSessionId,
		leadershipTermId:         leadershipTermId,
		leaderMemberId:           leaderMemberId,
		subscription:             subscription,
		egressImage:              egressImage,
		publication:              publication,
		headerBuffer:             atomic.MakeBuffer(make([]byte, SessionHeaderLength)),
		egressListener:           ctx.EgressListener,
		controlledEgressListener: ctx.ControlledEgressListener,
		endpointByIdMap:          make(map[int32]*memberIngress),
	}
	ac.fragmentAssembler = aeron.NewFragmentAssembler(ac.onFragment, 0)
	ac.controlledFragmentAssembler = aeron.NewControlledFragmentAssembler(ac.onControlledFragment, 0)
	writeMessageHeader(ac.headerBuffer, 0, sessionMessageHeaderBlockLength, sessionMessageHeaderTemplateId)
	ac.headerBuffer.PutInt64(MessageHeaderLength, leadershipTermId)
	ac.headerBuffer.PutInt64(MessageHeaderLength+8, clusterSessionId)
	ac.setState(stateConnected, time.Time{})
	return ac
}

// SetEgressListenerExtension sets a handler for egress messages with a non-cluster schema (PollEgress).
func (ac *AeronCluster) SetEgressListenerExtension(extension EgressListenerExtension) {
	ac.egressExtension = extension
}

// SetControlledEgressListenerExtension sets a handler for egress messages with a non-cluster schema
// (ControlledPollEgress).
func (ac *AeronCluster) SetControlledEgressListenerExtension(extension ControlledEgressListenerExtension) {
	ac.controlledEgressExtension = extension
}

// Close the session (sending a SessionCloseRequest if connected) and release resources. Idempotent.
func (ac *AeronCluster) Close() error {
	if ac.state == stateClosed {
		return nil
	}
	if ac.state == stateConnected && ac.publication != nil && ac.publication.IsConnected() {
		ac.closeSession()
	}
	if !ac.ctx.OwnsAeronClient {
		if ac.subscription != nil {
			if err := ac.subscription.Close(); err != nil {
				ac.ctx.ErrorHandler(err)
			}
		}
		if ac.publication != nil {
			if err := ac.publication.Close(); err != nil {
				ac.ctx.ErrorHandler(err)
			}
		}
	}
	ac.setState(stateClosed, time.Time{})
	return ac.ctx.Close()
}

// IsClosed reports whether the client has been closed (explicitly, by the cluster, or after a new leader timeout).
func (ac *AeronCluster) IsClosed() bool { return ac.state == stateClosed }

// IsConnected reports whether the client is connected to a leader and may send ingress.
func (ac *AeronCluster) IsConnected() bool { return ac.state == stateConnected }

// Context returns the concluded context.
func (ac *AeronCluster) Context() *Context { return ac.ctx }

// ClusterSessionId allocated by the cluster for this session.
func (ac *AeronCluster) ClusterSessionId() int64 { return ac.clusterSessionId }

// LeadershipTermId of the current leader.
func (ac *AeronCluster) LeadershipTermId() int64 { return ac.leadershipTermId }

// LeaderMemberId of the current leader.
func (ac *AeronCluster) LeaderMemberId() int32 { return ac.leaderMemberId }

// IngressPublication to the current leader. Prefer Offer/TryClaim which prepend the session header.
func (ac *AeronCluster) IngressPublication() aeron.Publisher { return ac.publication }

// EgressSubscription from the cluster.
func (ac *AeronCluster) EgressSubscription() *aeron.Subscription { return ac.subscription }

// TryClaim claims length bytes plus the session header. The session header is already written; write the
// message at bufferClaim.Offset()+SessionHeaderLength then Commit.
func (ac *AeronCluster) TryClaim(length int32, bufferClaim *logbuffer.Claim) int64 {
	result := ac.publication.TryClaim(length+SessionHeaderLength, bufferClaim)
	ac.TrackIngressPublicationResult(result)
	if result > 0 {
		bufferClaim.Buffer().PutBytes(bufferClaim.Offset(), ac.headerBuffer, 0, SessionHeaderLength)
	}
	return result
}

// Offer a message to the cluster with the session header prepended.
func (ac *AeronCluster) Offer(buffer *atomic.Buffer, offset int32, length int32) int64 {
	result := ac.publication.Offer2(ac.headerBuffer, 0, SessionHeaderLength, buffer, offset, length, nil)
	ac.TrackIngressPublicationResult(result)
	return result
}

// SendKeepAlive sends a SessionKeepAlive, retrying up to 3 times on back pressure.
func (ac *AeronCluster) SendKeepAlive() (bool, error) {
	length := MessageHeaderLength + int32(sessionKeepAliveBlockLength)
	ac.ctx.resetIdle()
	for attempts := sendAttempts; ; {
		position := ac.publication.TryClaim(length, &ac.bufferClaim)
		ac.TrackIngressPublicationResult(position)
		if position > 0 {
			buf, offset := ac.bufferClaim.Buffer(), ac.bufferClaim.Offset()
			writeMessageHeader(buf, offset, sessionKeepAliveBlockLength, sessionKeepAliveTemplateId)
			buf.PutInt64(offset+MessageHeaderLength, ac.leadershipTermId)
			buf.PutInt64(offset+MessageHeaderLength+8, ac.clusterSessionId)
			ac.bufferClaim.Commit()
			return true, nil
		}
		if position == aeron.MaxPositionExceeded {
			return false, fmt.Errorf("%w: max position exceeded: channel=%s", ErrCluster,
				ac.publication.Channel())
		}
		if attempts--; attempts <= 0 {
			return false, nil
		}
		ac.ctx.idle(0)
	}
}

// SendAdminRequestToTakeASnapshot asks the cluster to take a snapshot. The result arrives via OnAdminResponse
// with the given correlationId. Requires the cluster's AuthorisationService to allow it.
func (ac *AeronCluster) SendAdminRequestToTakeASnapshot(correlationId int64) (bool, error) {
	length := MessageHeaderLength + int32(adminRequestBlockLength) + 4
	ac.ctx.resetIdle()
	for attempts := sendAttempts; ; {
		position := ac.publication.TryClaim(length, &ac.bufferClaim)
		ac.TrackIngressPublicationResult(position)
		if position > 0 {
			buf, offset := ac.bufferClaim.Buffer(), ac.bufferClaim.Offset()
			writeMessageHeader(buf, offset, adminRequestBlockLength, adminRequestTemplateId)
			body := offset + MessageHeaderLength
			buf.PutInt64(body, ac.leadershipTermId)
			buf.PutInt64(body+8, ac.clusterSessionId)
			buf.PutInt64(body+16, correlationId)
			buf.PutInt32(body+24, int32(codecs.AdminRequestType.SNAPSHOT))
			buf.PutInt32(body+int32(adminRequestBlockLength), 0)
			ac.bufferClaim.Commit()
			return true, nil
		}
		if position == aeron.PublicationClosed {
			return false, fmt.Errorf("%w: ingress publication is closed", ErrCluster)
		}
		if position == aeron.MaxPositionExceeded {
			return false, fmt.Errorf("%w: max position exceeded: channel=%s", ErrCluster,
				ac.publication.Channel())
		}
		if attempts--; attempts <= 0 {
			return false, nil
		}
		ac.ctx.idle(0)
	}
}

// PollEgress polls the egress subscription dispatching to Context.EgressListener, then handles state changes.
func (ac *AeronCluster) PollEgress() int {
	if ac.state == stateClosed {
		return 0
	}
	workCount := ac.subscription.Poll(ac.fragmentAssembler.OnFragment, fragmentLimit)
	return workCount + ac.checkEgressImage() + ac.PollStateChanges()
}

// ControlledPollEgress polls the egress subscription dispatching to Context.ControlledEgressListener.
func (ac *AeronCluster) ControlledPollEgress() int {
	if ac.state == stateClosed {
		return 0
	}
	workCount := ac.subscription.ControlledPoll(ac.controlledFragmentAssembler.OnFragment, fragmentLimit)
	return workCount + ac.checkEgressImage() + ac.PollStateChanges()
}

func (ac *AeronCluster) checkEgressImage() int {
	if (ac.state == stateConnected || ac.state == stateAwaitNewLeaderConnection) &&
		isImageClosed(ac.subscription, ac.egressImage) {
		ac.onDisconnected()
		return 1
	}
	return 0
}

// PollStateChanges closes the client if the session was closed or no new leader arrived within
// Context.NewLeaderTimeout. Called by PollEgress; call directly when polling egress via an EgressAdapter.
func (ac *AeronCluster) PollStateChanges() int {
	if ac.state == statePendingClose ||
		((ac.state == stateAwaitNewLeader || ac.state == stateAwaitNewLeaderConnection) &&
			!time.Now().Before(ac.stateDeadline)) {
		if err := ac.Close(); err != nil {
			ac.ctx.ErrorHandler(err)
		}
		return 1
	}
	return 0
}

// OnNewLeader updates the client for a new leader, connecting an ingress publication to it. Called
// automatically by PollEgress; call when handling NewLeaderEvent via an EgressAdapter.
func (ac *AeronCluster) OnNewLeader(clusterSessionId int64, leadershipTermId int64, leaderMemberId int32,
	ingressEndpoints string) error {
	if clusterSessionId != ac.clusterSessionId {
		return fmt.Errorf("%w: invalid clusterSessionId=%d expected=%d", ErrCluster, clusterSessionId,
			ac.clusterSessionId)
	}
	ac.setState(stateAwaitNewLeaderConnection, time.Now().Add(ac.ctx.MessageTimeout))
	ac.leadershipTermId = leadershipTermId
	ac.leaderMemberId = leaderMemberId
	ac.headerBuffer.PutInt64(MessageHeaderLength, leadershipTermId)
	if ac.publication != nil {
		_ = ac.publication.Close()
	}

	if ac.ctx.IngressEndpoints == "" {
		pub, err := ac.addNewLeaderIngressPublication(ac.ctx.IngressChannel)
		if err != nil {
			return err
		}
		ac.publication = pub
	} else {
		ac.ctx.IngressEndpoints = ingressEndpoints
		if err := ac.updateMemberEndpoints(ingressEndpoints, leaderMemberId); err != nil {
			return err
		}
	}
	ac.fragmentAssembler.Clear()
	ac.controlledFragmentAssembler.Clear()
	ac.egressListener.OnNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints)
	ac.controlledEgressListener.OnNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints)
	return nil
}

// TrackIngressPublicationResult updates connection state from an ingress publication result. Call after
// offering directly to IngressPublication.
func (ac *AeronCluster) TrackIngressPublicationResult(result int64) {
	switch ac.state {
	case stateConnected:
		if result == aeron.NotConnected || result == aeron.PublicationClosed {
			ac.onDisconnected()
		} else if result == aeron.MaxPositionExceeded {
			_ = ac.publication.Close()
			ac.setState(statePendingClose, time.Time{})
		}
	case stateAwaitNewLeaderConnection:
		if result > 0 {
			ac.setState(stateConnected, time.Time{})
		}
	}
}

func (ac *AeronCluster) addNewLeaderIngressPublication(channel string) (aeron.Publisher, error) {
	streamId := ac.ctx.IngressStreamId
	deadline := time.Now().Add(ac.ctx.MessageTimeout)
	registrationId := int64(nullValue)
	var lastErr error
	for {
		if registrationId == nullValue {
			id, err := asyncAddIngressPublication(ac.ctx, channel, streamId)
			if err != nil {
				return nil, err
			}
			registrationId = id
		}
		pub, err := getIngressPublication(ac.ctx, registrationId)
		if pub != nil {
			return pub, nil
		}
		if err != nil {
			registrationId = nullValue
			if !isResourceTemporarilyUnavailable(err) {
				return nil, err
			}
			lastErr = err
		}
		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf("%w: failed to add new leader ingress publication (leaderMemberId=%d, "+
				"leadershipTermId=%d, channel=%s, streamId=%d) within %v: lastErr=%v", ErrTimeout,
				ac.leaderMemberId, ac.leadershipTermId, channel, streamId, ac.ctx.MessageTimeout, lastErr)
		}
		ac.ctx.idle(ac.ctx.runAgentInvokers())
	}
}

func (ac *AeronCluster) updateMemberEndpoints(ingressEndpoints string, leaderMemberId int32) error {
	closeAllMembers(ac.endpointByIdMap)
	members, err := parseMemberIngress(ac.ctx, ingressEndpoints)
	if err != nil {
		return err
	}
	ac.endpointByIdMap = members
	newLeader := members[leaderMemberId]
	if newLeader == nil {
		return fmt.Errorf("%w: new leaderMemberId=%d not in ingressEndpoints=%s", ErrCluster, leaderMemberId,
			ingressEndpoints)
	}
	channel, err := ingressChannelForEndpoint(ac.ctx, newLeader.endpoint)
	if err != nil {
		return err
	}
	pub, err := ac.addNewLeaderIngressPublication(channel)
	if err != nil {
		return err
	}
	newLeader.publication = pub
	ac.publication = pub
	return nil
}

func (ac *AeronCluster) onDisconnected() {
	if ac.publication != nil {
		_ = ac.publication.Close()
	}
	ac.setState(stateAwaitNewLeader, time.Now().Add(ac.ctx.newLeaderTimeout()))
}

func (ac *AeronCluster) onSessionEvent(e *codecs.SessionEvent) {
	if e.Code == codecs.EventCode.CLOSED {
		ac.setState(statePendingClose, time.Time{})
	}
}

func (ac *AeronCluster) onNewLeaderEvent(header *logbuffer.Header, e *codecs.NewLeaderEvent) term.ControlledPollAction {
	ac.egressImage = imageForHeader(ac.subscription, header)
	if err := ac.OnNewLeader(e.ClusterSessionId, e.LeadershipTermId, e.LeaderMemberId,
		string(e.IngressEndpoints)); err != nil {
		ac.ctx.ErrorHandler(err)
	}
	return term.ControlledPollActionCommit
}

func (ac *AeronCluster) onFragment(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
	var ext ControlledEgressListenerExtension
	if ac.egressExtension != nil {
		ext = uncontrolledExtension{ac.egressExtension}
	}
	dispatchEgress(ac.clusterSessionId, buffer, offset, length, header, uncontrolled{ac.egressListener}, ext,
		ac.onNewLeaderEvent, ac.onSessionEvent, ac.ctx.ErrorHandler)
}

func (ac *AeronCluster) onControlledFragment(buffer *atomic.Buffer, offset int32, length int32,
	header *logbuffer.Header) term.ControlledPollAction {
	return dispatchEgress(ac.clusterSessionId, buffer, offset, length, header, ac.controlledEgressListener,
		ac.controlledEgressExtension, ac.onNewLeaderEvent, ac.onSessionEvent, ac.ctx.ErrorHandler)
}

func (ac *AeronCluster) closeSession() {
	length := MessageHeaderLength + int32(sessionCloseRequestBlockLength)
	ac.ctx.resetIdle()
	for attempts := sendAttempts; ; {
		position := ac.publication.TryClaim(length, &ac.bufferClaim)
		ac.TrackIngressPublicationResult(position)
		if position > 0 {
			buf, offset := ac.bufferClaim.Buffer(), ac.bufferClaim.Offset()
			writeMessageHeader(buf, offset, sessionCloseRequestBlockLength, sessionCloseRequestTemplateId)
			buf.PutInt64(offset+MessageHeaderLength, ac.leadershipTermId)
			buf.PutInt64(offset+MessageHeaderLength+8, ac.clusterSessionId)
			ac.bufferClaim.Commit()
			return
		}
		if attempts--; attempts <= 0 {
			return
		}
		ac.ctx.idle(0)
	}
}

func (ac *AeronCluster) setState(newState clientState, deadline time.Time) {
	logger.Debugf("AeronCluster %v -> %v", ac.state, newState)
	ac.state = newState
	ac.stateDeadline = deadline
}

func atomicBufferOf(data []byte) *atomic.Buffer {
	return atomic.MakeBuffer(data)
}
