package client

import (
	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
)

// IngressSessionDecorator prepends a SessionMessageHeader to messages offered on an ingress publication,
// mirroring io.aeron.cluster.client.IngressSessionDecorator.
type IngressSessionDecorator struct {
	headerBuffer *atomic.Buffer
}

// IngressSessionDecoratorHeaderLength is IngressSessionDecorator.HEADER_LENGTH.
const IngressSessionDecoratorHeaderLength = SessionHeaderLength

// NewIngressSessionDecorator creates a decorator for the session and leadership term (use -1 for unknown).
func NewIngressSessionDecorator(clusterSessionId int64, leadershipTermId int64) *IngressSessionDecorator {
	d := &IngressSessionDecorator{headerBuffer: atomic.MakeBuffer(make([]byte, SessionHeaderLength))}
	writeMessageHeader(d.headerBuffer, 0, sessionMessageHeaderBlockLength, sessionMessageHeaderTemplateId)
	d.SetLeadershipTermId(leadershipTermId)
	d.SetClusterSessionId(clusterSessionId)
	d.headerBuffer.PutInt64(MessageHeaderLength+16, nullValue)
	return d
}

// SetClusterSessionId sets the cluster session id written into the header.
func (d *IngressSessionDecorator) SetClusterSessionId(clusterSessionId int64) *IngressSessionDecorator {
	d.headerBuffer.PutInt64(MessageHeaderLength+8, clusterSessionId)
	return d
}

// SetLeadershipTermId sets the leadership term id written into the header.
func (d *IngressSessionDecorator) SetLeadershipTermId(leadershipTermId int64) *IngressSessionDecorator {
	d.headerBuffer.PutInt64(MessageHeaderLength, leadershipTermId)
	return d
}

// Offer the message with the session header prepended.
func (d *IngressSessionDecorator) Offer(publication *aeron.Publication, buffer *atomic.Buffer, offset int32,
	length int32) int64 {
	return publication.Offer2(d.headerBuffer, 0, SessionHeaderLength, buffer, offset, length, nil)
}
