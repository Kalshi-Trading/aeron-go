package client

import (
	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/cluster/codecs"
)

// EgressPoller polls the egress subscription for single connect/session protocol events, mirroring
// io.aeron.cluster.client.EgressPoller. Each Poll stops after the first complete cluster event.
type EgressPoller struct {
	fragmentLimit            int
	subscription             *aeron.Subscription
	fragmentAssembler        *aeron.ControlledFragmentAssembler
	egressImage              aeron.Image
	clusterSessionId         int64
	correlationId            int64
	leadershipTermId         int64
	leaderMemberId           int32
	templateId               int32
	version                  int32
	leaderHeartbeatTimeoutNs int64
	isPollComplete           bool
	eventCode                codecs.EventCodeEnum
	detail                   string
	encodedChallenge         []byte
}

// NewEgressPoller creates a poller for the egress subscription.
func NewEgressPoller(subscription *aeron.Subscription, fragmentLimit int) *EgressPoller {
	p := &EgressPoller{subscription: subscription, fragmentLimit: fragmentLimit}
	p.reset()
	p.fragmentAssembler = aeron.NewControlledFragmentAssembler(p.onFragment, 0)
	return p
}

func (p *EgressPoller) reset() {
	p.isPollComplete = false
	p.clusterSessionId = nullValue
	p.correlationId = nullValue
	p.leadershipTermId = nullValue
	p.leaderMemberId = nullValue
	p.templateId = nullValue
	p.version = 0
	p.leaderHeartbeatTimeoutNs = nullValue
	p.eventCode = codecs.EventCode.NullValue
	p.detail = ""
	p.encodedChallenge = nil
}

func (p *EgressPoller) Subscription() *aeron.Subscription { return p.subscription }

// EgressImage is the image the last session or new leader event arrived on.
func (p *EgressPoller) EgressImage() aeron.Image { return p.egressImage }

// TemplateId of the last polled message or -1.
func (p *EgressPoller) TemplateId() int32               { return p.templateId }
func (p *EgressPoller) ClusterSessionId() int64         { return p.clusterSessionId }
func (p *EgressPoller) CorrelationId() int64            { return p.correlationId }
func (p *EgressPoller) LeadershipTermId() int64         { return p.leadershipTermId }
func (p *EgressPoller) LeaderMemberId() int32           { return p.leaderMemberId }
func (p *EgressPoller) EventCode() codecs.EventCodeEnum { return p.eventCode }
func (p *EgressPoller) Version() int32                  { return p.version }
func (p *EgressPoller) LeaderHeartbeatTimeoutNs() int64 { return p.leaderHeartbeatTimeoutNs }
func (p *EgressPoller) Detail() string                  { return p.detail }
func (p *EgressPoller) EncodedChallenge() []byte        { return p.encodedChallenge }
func (p *EgressPoller) IsPollComplete() bool            { return p.isPollComplete }
func (p *EgressPoller) IsChallenged() bool              { return p.templateId == int32(challengeTemplateId) }

// Poll the egress subscription; resets the last result if the previous poll was complete.
func (p *EgressPoller) Poll() int {
	if p.isPollComplete {
		p.reset()
	}
	return p.subscription.ControlledPoll(p.fragmentAssembler.OnFragment, p.fragmentLimit)
}

func (p *EgressPoller) onFragment(buffer *atomic.Buffer, offset int32, length int32,
	header *logbuffer.Header) term.ControlledPollAction {
	if p.isPollComplete {
		return term.ControlledPollActionAbort
	}
	if length < MessageHeaderLength {
		return term.ControlledPollActionContinue
	}
	hdr := readMessageHeader(buffer, offset)
	if hdr.schemaId != schemaId {
		return term.ControlledPollActionContinue
	}
	p.templateId = int32(hdr.templateId)
	switch hdr.templateId {
	case sessionMessageHeaderTemplateId:
		if length < SessionHeaderLength {
			return term.ControlledPollActionContinue
		}
		body := offset + MessageHeaderLength
		p.leadershipTermId = buffer.GetInt64(body)
		p.clusterSessionId = buffer.GetInt64(body + 8)
		p.isPollComplete = true
		return term.ControlledPollActionBreak

	case sessionEventTemplateId:
		var e codecs.SessionEvent
		if err := decodeBody(buffer, offset, length, hdr, &e); err != nil {
			logError(err)
			return term.ControlledPollActionContinue
		}
		p.clusterSessionId = e.ClusterSessionId
		p.correlationId = e.CorrelationId
		p.leadershipTermId = e.LeadershipTermId
		p.leaderMemberId = e.LeaderMemberId
		p.eventCode = e.Code
		p.version = e.Version
		p.leaderHeartbeatTimeoutNs = e.LeaderHeartbeatTimeoutNs
		if e.LeaderHeartbeatTimeoutNs == e.LeaderHeartbeatTimeoutNsNullValue() {
			p.leaderHeartbeatTimeoutNs = nullValue
		}
		p.detail = string(e.Detail)
		p.isPollComplete = true
		p.egressImage = imageForHeader(p.subscription, header)
		return term.ControlledPollActionBreak

	case newLeaderEventTemplateId:
		var e codecs.NewLeaderEvent
		if err := decodeBody(buffer, offset, length, hdr, &e); err != nil {
			logError(err)
			return term.ControlledPollActionContinue
		}
		p.clusterSessionId = e.ClusterSessionId
		p.leadershipTermId = e.LeadershipTermId
		p.leaderMemberId = e.LeaderMemberId
		p.detail = string(e.IngressEndpoints)
		p.isPollComplete = true
		p.egressImage = imageForHeader(p.subscription, header)
		return term.ControlledPollActionBreak

	case challengeTemplateId:
		var e codecs.Challenge
		if err := decodeBody(buffer, offset, length, hdr, &e); err != nil {
			logError(err)
			return term.ControlledPollActionContinue
		}
		p.encodedChallenge = append([]byte{}, e.EncodedChallenge...)
		p.clusterSessionId = e.ClusterSessionId
		p.correlationId = e.CorrelationId
		p.isPollComplete = true
		return term.ControlledPollActionBreak
	}
	return term.ControlledPollActionContinue
}

func imageForHeader(subscription *aeron.Subscription, header *logbuffer.Header) aeron.Image {
	if subscription == nil || header == nil {
		return nil
	}
	return subscription.ImageBySessionID(header.SessionID())
}

// isImageClosed reports whether img has been closed or removed from the subscription. The Go Header carries no
// reference to its Image, so we compare against the subscription's current image list.
func isImageClosed(subscription *aeron.Subscription, img aeron.Image) bool {
	if img == nil {
		return false
	}
	if img.IsClosed() {
		return true
	}
	current := subscription.ImageBySessionID(img.SessionID())
	return current == nil || current.CorrelationID() != img.CorrelationID()
}
