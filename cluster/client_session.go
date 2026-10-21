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
package cluster

import (
	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
)

const (
	// ClientSessionMockedOffer is returned when offering to a session on a non-leader member (Java
	// ClientSession.MOCKED_OFFER).
	ClientSessionMockedOffer = 1
)

// ClientSession is a session from a client to the cluster as seen by a clustered service.
// Mirrors io.aeron.cluster.service.ClientSession.
type ClientSession interface {
	Id() int64
	ResponseStreamId() int32
	ResponseChannel() string
	EncodedPrincipal() []byte

	// Close requests the session be closed via the consensus module. Duplicate requests are suppressed.
	Close()

	// IsClosing indicates that a request to close this session has been made.
	IsClosing() bool

	// Offer a message to the client's egress. Returns ClientSessionMockedOffer when not the leader.
	Offer(buffer *atomic.Buffer, offset, length int32, reservedValueSupplier term.ReservedValueSupplier) int64

	// TryClaim a range of the egress publication for zero copy publication. The length excludes the
	// SessionMessageHeader, which occupies the first SessionMessageHeaderLength bytes of the claim payload.
	// When not leader a scratch buffer is claimed and ClientSessionMockedOffer returned.
	TryClaim(length int32, claim *logbuffer.Claim) int64
}

type containerClientSession struct {
	id               int64
	responseStreamId int32
	responseChannel  string
	encodedPrincipal []byte
	agent            *ClusteredServiceAgent
	response         *aeron.Publication
	isClosing        bool
}

func newContainerClientSession(
	id int64,
	responseStreamId int32,
	responseChannel string,
	encodedPrincipal []byte,
	agent *ClusteredServiceAgent,
) *containerClientSession {
	return &containerClientSession{
		id:               id,
		responseStreamId: responseStreamId,
		responseChannel:  responseChannel,
		encodedPrincipal: encodedPrincipal,
		agent:            agent,
	}
}

func (s *containerClientSession) Id() int64 {
	return s.id
}

func (s *containerClientSession) ResponseStreamId() int32 {
	return s.responseStreamId
}

func (s *containerClientSession) ResponseChannel() string {
	return s.responseChannel
}

func (s *containerClientSession) EncodedPrincipal() []byte {
	return s.encodedPrincipal
}

func (s *containerClientSession) Close() {
	if s.agent.GetClientSession(s.id) != nil {
		s.agent.CloseClientSession(s.id)
	}
}

func (s *containerClientSession) IsClosing() bool {
	return s.isClosing
}

func (s *containerClientSession) markClosing() {
	s.isClosing = true
}

func (s *containerClientSession) resetClosing() {
	s.isClosing = false
}

func (s *containerClientSession) Offer(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	reservedValueSupplier term.ReservedValueSupplier,
) int64 {
	return s.agent.offerToSession(s.id, s.response, buffer, offset, length, reservedValueSupplier)
}

func (s *containerClientSession) TryClaim(length int32, claim *logbuffer.Claim) int64 {
	return s.agent.tryClaimToSession(s.id, s.response, length, claim)
}

// connect lazily creates the egress publication. Errors are logged rather than failing the service as the client
// may have gone away (Java: ContainerClientSession.connect).
func (s *containerClientSession) connect(aeronClient *aeron.Aeron) {
	if s.response != nil {
		return
	}
	pub, err := aeronClient.AddPublication(s.responseChannel, s.responseStreamId)
	if err != nil {
		logger.Errorf("failed to connect session response publication - id=%d channel=%s: %v",
			s.id, s.responseChannel, err)
		return
	}
	s.response = pub
}

func (s *containerClientSession) disconnect() {
	if s.response != nil {
		closePublication(s.response)
		s.response = nil
	}
}
