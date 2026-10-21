package client

import (
	"fmt"
	"strings"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/cluster/codecs"
)

// AsyncConnectState is the step of an AsyncConnect, mirroring AeronCluster.AsyncConnect.State.
type AsyncConnectState int

const (
	AsyncConnectCreateEgressSubscription AsyncConnectState = iota - 1
	AsyncConnectCreateIngressPublications
	AsyncConnectAwaitPublicationConnected
	AsyncConnectSendMessage
	AsyncConnectPollResponse
	AsyncConnectConcludeConnect
	AsyncConnectDone
)

func (s AsyncConnectState) String() string {
	switch s {
	case AsyncConnectCreateEgressSubscription:
		return "CREATE_EGRESS_SUBSCRIPTION"
	case AsyncConnectCreateIngressPublications:
		return "CREATE_INGRESS_PUBLICATIONS"
	case AsyncConnectAwaitPublicationConnected:
		return "AWAIT_PUBLICATION_CONNECTED"
	case AsyncConnectSendMessage:
		return "SEND_MESSAGE"
	case AsyncConnectPollResponse:
		return "POLL_RESPONSE"
	case AsyncConnectConcludeConnect:
		return "CONCLUDE_CONNECT"
	case AsyncConnectDone:
		return "DONE"
	}
	return "<unknown>"
}

// StepName returns the name of the AsyncConnect step, or "<unknown>".
func StepName(step int) string {
	return AsyncConnectState(step).String()
}

// AsyncConnect is a non-blocking connection to the cluster, mirroring AeronCluster.AsyncConnect. Call Poll
// until it returns a client or an error. On error (or abandoning the attempt) call Close.
type AsyncConnect struct {
	ctx                      *Context
	deadline                 time.Time
	leaderHeartbeatTimeoutNs int64
	correlationId            int64
	clusterSessionId         int64
	leadershipTermId         int64
	leaderMemberId           int32
	state                    AsyncConnectState
	message                  []byte
	egressSubscription       *aeron.Subscription
	egressPoller             *EgressPoller
	egressRegistrationId     int64
	egressImage              aeron.Image
	memberByIdMap            map[int32]*memberIngress
	ingressRegistrationId    int64
	ingressPublication       aeron.Publisher
}

// Connect concludes the context and blocks until connected to the cluster or MessageTimeout elapses.
func Connect(ctx *Context) (*AeronCluster, error) {
	async, err := NewAsyncConnect(ctx)
	if err != nil {
		return nil, err
	}
	state := async.State()
	for {
		cluster, err := async.Poll()
		if err != nil {
			async.Close()
			return nil, err
		}
		if cluster != nil {
			return cluster, nil
		}
		if state != async.State() {
			state = async.State()
			ctx.idle(1)
		} else {
			ctx.idle(0)
		}
	}
}

// NewAsyncConnect concludes the context and begins an asynchronous connect.
func NewAsyncConnect(ctx *Context) (*AsyncConnect, error) {
	if err := ctx.Conclude(); err != nil {
		_ = ctx.Close()
		return nil, err
	}
	members, err := parseMemberIngress(ctx, ctx.IngressEndpoints)
	if err != nil {
		_ = ctx.Close()
		return nil, err
	}
	return &AsyncConnect{
		ctx:                      ctx,
		deadline:                 time.Now().Add(ctx.MessageTimeout),
		leaderHeartbeatTimeoutNs: nullValue,
		correlationId:            nullValue,
		clusterSessionId:         nullValue,
		leadershipTermId:         nullValue,
		leaderMemberId:           nullValue,
		state:                    AsyncConnectCreateEgressSubscription,
		egressRegistrationId:     nullValue,
		memberByIdMap:            members,
		ingressRegistrationId:    nullValue,
	}, nil
}

// Step returns the integer step of the current state.
func (c *AsyncConnect) Step() int { return int(c.state) }

// State returns the current state.
func (c *AsyncConnect) State() AsyncConnectState { return c.state }

// Close releases resources if the connect did not complete.
func (c *AsyncConnect) Close() {
	if c.state == AsyncConnectDone {
		return
	}
	if c.ingressPublication != nil {
		_ = c.ingressPublication.Close()
	} else if c.ingressRegistrationId != nullValue {
		closePendingPublication(c.ctx, c.ingressRegistrationId)
	}
	if c.egressSubscription != nil {
		_ = c.egressSubscription.Close()
	} else if c.egressRegistrationId != nullValue && c.ctx.Aeron != nil && !c.ctx.Aeron.IsClosed() {
		if sub, err := c.ctx.Aeron.GetSubscription(c.egressRegistrationId); err == nil && sub != nil {
			_ = sub.Close()
		}
	}
	closeAllMembers(c.memberByIdMap)
	_ = c.ctx.Close()
	c.state = AsyncConnectDone
}

// Poll advances the connect. Returns the connected client once complete, or an error.
func (c *AsyncConnect) Poll() (*AeronCluster, error) {
	if err := c.checkDeadline(); err != nil {
		return nil, err
	}
	c.ctx.runAgentInvokers()
	var err error
	switch c.state {
	case AsyncConnectCreateEgressSubscription:
		err = c.createEgressSubscription()
	case AsyncConnectCreateIngressPublications:
		err = c.createIngressPublications()
	case AsyncConnectAwaitPublicationConnected:
		c.awaitPublicationConnected()
	case AsyncConnectSendMessage:
		err = c.sendMessage()
	case AsyncConnectPollResponse:
		err = c.pollResponse()
	case AsyncConnectConcludeConnect:
		return c.concludeConnect(), nil
	}
	return nil, err
}

func (c *AsyncConnect) checkDeadline() error {
	if time.Now().Before(c.deadline) {
		return nil
	}
	isConnected := false
	egressChannel := "<unknown>"
	if c.egressSubscription != nil {
		isConnected = c.egressSubscription.IsConnected()
		egressChannel = c.egressSubscription.TryResolveChannelEndpointPort()
	}
	var memberErrs []string
	for _, m := range c.memberByIdMap {
		if m.publicationErr != nil {
			memberErrs = append(memberErrs, fmt.Sprintf("memberId=%d: %v", m.memberId, m.publicationErr))
		}
	}
	return fmt.Errorf("%w: cluster connect timeout: state=%v messageTimeout=%v ingressChannel=%s "+
		"ingressEndpoints=%s ingressPublication=%v egress.isConnected=%v responseChannel=%s memberErrors=[%s]",
		ErrTimeout, c.state, c.ctx.MessageTimeout, c.ctx.IngressChannel, c.ctx.IngressEndpoints,
		c.ingressPublication != nil, isConnected, egressChannel, strings.Join(memberErrs, "; "))
}

func (c *AsyncConnect) createEgressSubscription() error {
	if c.egressRegistrationId == nullValue {
		id, err := c.ctx.Aeron.AsyncAddSubscription(c.ctx.EgressChannel, c.ctx.EgressStreamId)
		if err != nil {
			return err
		}
		c.egressRegistrationId = id
	}
	sub, err := c.ctx.Aeron.GetSubscription(c.egressRegistrationId)
	if err != nil {
		c.egressRegistrationId = nullValue
		return err
	}
	if sub != nil {
		c.egressSubscription = sub
		c.egressPoller = NewEgressPoller(sub, fragmentLimit)
		c.egressRegistrationId = nullValue
		c.state = AsyncConnectCreateIngressPublications
	}
	return nil
}

func (c *AsyncConnect) createIngressPublications() error {
	if c.ctx.IngressEndpoints != "" {
		c.state = AsyncConnectAwaitPublicationConnected
		return nil
	}
	if c.ingressPublication == nil {
		if c.ingressRegistrationId == nullValue {
			id, err := asyncAddIngressPublication(c.ctx, c.ctx.IngressChannel, c.ctx.IngressStreamId)
			if err != nil {
				return err
			}
			c.ingressRegistrationId = id
		}
		pub, err := getIngressPublication(c.ctx, c.ingressRegistrationId)
		if err != nil {
			c.ingressRegistrationId = nullValue
			return err
		}
		c.ingressPublication = pub
	}
	if c.ingressPublication != nil {
		c.ingressRegistrationId = nullValue
		c.state = AsyncConnectAwaitPublicationConnected
	}
	return nil
}

func (c *AsyncConnect) awaitPublicationConnected() {
	responseChannel := c.egressSubscription.TryResolveChannelEndpointPort()
	if responseChannel == "" {
		return
	}
	if c.ingressPublication == nil {
		for _, member := range c.memberByIdMap {
			if member.publication == nil && member.publicationErr == nil {
				if member.registrationId == nullValue {
					member.asyncAddPublication()
				}
				member.asyncGetPublication()
			}
			if member.publication != nil && member.publication.IsConnected() {
				c.ingressPublication = member.publication
				c.prepareConnectRequest(responseChannel)
				break
			}
		}
	} else if c.ingressPublication.IsConnected() {
		c.prepareConnectRequest(responseChannel)
	}
}

func (c *AsyncConnect) prepareConnectRequest(responseChannel string) {
	c.correlationId = c.ctx.Aeron.NextCorrelationID()
	req := codecs.SessionConnectRequest{
		CorrelationId:      c.correlationId,
		ResponseStreamId:   c.ctx.EgressStreamId,
		Version:            ProtocolSemanticVersion,
		ResponseChannel:    []byte(responseChannel),
		EncodedCredentials: c.ctx.CredentialsSupplier.EncodedCredentials(),
		ClientInfo:         []byte(fmt.Sprintf("name=%s version=aeron-go", c.ctx.ClientName)),
	}
	c.encode(&req)
}

func (c *AsyncConnect) prepareChallengeResponse(encodedCredentials []byte) {
	c.correlationId = c.ctx.Aeron.NextCorrelationID()
	resp := codecs.ChallengeResponse{
		CorrelationId:      c.correlationId,
		ClusterSessionId:   c.clusterSessionId,
		EncodedCredentials: encodedCredentials,
	}
	c.encode(&resp)
}

func (c *AsyncConnect) encode(msg sbeEncoder) {
	data, err := encodeMessage(msg)
	if err != nil {
		// Encoding into a bytes.Buffer only fails on programming errors.
		panic(err)
	}
	c.message = data
	c.state = AsyncConnectSendMessage
}

func (c *AsyncConnect) sendMessage() error {
	buffer := atomicBufferOf(c.message)
	position := c.ingressPublication.Offer(buffer, 0, buffer.Capacity(), nil)
	if position > 0 {
		c.state = AsyncConnectPollResponse
	} else if position == aeron.PublicationClosed || position == aeron.NotConnected {
		return fmt.Errorf("%w: unexpected loss of connection to cluster", ErrCluster)
	}
	return nil
}

func (c *AsyncConnect) pollResponse() error {
	p := c.egressPoller
	if p.Poll() > 0 && p.IsPollComplete() && p.CorrelationId() == c.correlationId {
		if p.IsChallenged() {
			c.correlationId = nullValue
			c.clusterSessionId = p.ClusterSessionId()
			c.prepareChallengeResponse(c.ctx.CredentialsSupplier.OnChallenge(p.EncodedChallenge()))
			return nil
		}
		switch p.EventCode() {
		case codecs.EventCode.OK:
			c.leadershipTermId = p.LeadershipTermId()
			c.leaderMemberId = p.LeaderMemberId()
			c.clusterSessionId = p.ClusterSessionId()
			c.leaderHeartbeatTimeoutNs = p.LeaderHeartbeatTimeoutNs()
			c.egressImage = p.EgressImage()
			c.state = AsyncConnectConcludeConnect
		case codecs.EventCode.ERROR:
			return fmt.Errorf("%w: %s", ErrCluster, p.Detail())
		case codecs.EventCode.REDIRECT:
			return c.updateMembers()
		case codecs.EventCode.AUTHENTICATION_REJECTED:
			return fmt.Errorf("%w: %s", ErrAuthenticationRejected, p.Detail())
		}
	}
	return nil
}

func (c *AsyncConnect) updateMembers() error {
	c.leaderMemberId = c.egressPoller.LeaderMemberId()
	oldLeader := c.memberByIdMap[c.leaderMemberId]
	delete(c.memberByIdMap, c.leaderMemberId)
	if c.ingressPublication != nil && (oldLeader == nil || c.ingressPublication != oldLeader.publication) {
		_ = c.ingressPublication.Close()
	}
	c.ingressPublication = nil
	closeAllMembers(c.memberByIdMap)

	members, err := parseMemberIngress(c.ctx, c.egressPoller.Detail())
	if err != nil {
		if oldLeader != nil {
			oldLeader.close()
		}
		return err
	}
	c.memberByIdMap = members
	newLeader := members[c.leaderMemberId]
	if newLeader == nil {
		if oldLeader != nil {
			oldLeader.close()
		}
		return fmt.Errorf("%w: redirect to unknown leaderMemberId=%d endpoints=%s", ErrCluster,
			c.leaderMemberId, c.egressPoller.Detail())
	}
	if oldLeader != nil && oldLeader.publicationErr == nil && newLeader.endpoint == oldLeader.endpoint {
		newLeader.publication = oldLeader.publication
		newLeader.registrationId = oldLeader.registrationId
		c.ingressPublication = oldLeader.publication
	} else {
		if oldLeader != nil {
			oldLeader.close()
		}
		newLeader.asyncAddPublication()
	}
	c.state = AsyncConnectAwaitPublicationConnected
	return nil
}

func (c *AsyncConnect) concludeConnect() *AeronCluster {
	if c.ctx.NewLeaderTimeout == 0 {
		hb := DefaultLeaderHeartbeatTimeout
		if c.leaderHeartbeatTimeoutNs != nullValue {
			hb = time.Duration(c.leaderHeartbeatTimeoutNs)
		}
		c.ctx.NewLeaderTimeout = 2 * hb
	}
	cluster := newAeronCluster(c.ctx, c.ingressPublication, c.egressSubscription, c.egressImage,
		c.clusterSessionId, c.leadershipTermId, c.leaderMemberId)
	c.ingressPublication = nil
	delete(c.memberByIdMap, c.leaderMemberId)
	closeAllMembers(c.memberByIdMap)
	c.state = AsyncConnectDone
	return cluster
}
