package cluster

import (
	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster/codecs"
)

// Cluster is the interface a ClusteredService uses to interact with the cluster it is hosted in.
// Mirrors io.aeron.cluster.service.Cluster.
type Cluster interface {

	// MemberId returns the unique id for the hosting member of the cluster.
	MemberId() int32

	// Role returns the role the cluster node is playing
	Role() Role

	// LogPosition returns the position the log has reached in bytes as of the current message
	LogPosition() int64

	// Aeron returns the Aeron client used by the service container.
	Aeron() *aeron.Aeron

	// Options returns the configuration of the service container (Java: context()).
	Options() *Options

	// GetClientSession returns the session for the given cluster session id, or nil if not found.
	GetClientSession(clusterSessionId int64) ClientSession

	// ClientSessions returns the currently open client sessions ordered by cluster session id.
	ClientSessions() []ClientSession

	// ForEachClientSession invokes the action for every open client session ordered by cluster session id.
	ForEachClientSession(action func(ClientSession))

	// CloseClientSession requests the consensus module to close the session. Returns false if the session is
	// already closing or the request could not be sent; panics/errors for unknown sessions are returned as false
	// with an error logged. Must not be called from OnSessionClose.
	CloseClientSession(clusterSessionId int64) bool

	// Time returns the cluster time as time units since 1 Jan 1970 UTC
	Time() int64

	// TimeUnit returns the unit of time applied when timestamping and time operations
	TimeUnit() codecs.ClusterTimeUnitEnum

	// ScheduleTimer schedules a timer for a given deadline and provide a correlation id to identify the timer when it
	// expires or for cancellation. Scheduling with an existing correlationId replaces the old timer.
	ScheduleTimer(correlationId int64, deadline int64) bool

	// CancelTimer cancels a previously scheduled timer.
	CancelTimer(correlationId int64) bool

	// Offer a message as ingress to the cluster for sequencing (service to service). The message will be appended to
	// the log with the cluster session id set to the service id.
	Offer(buffer *atomic.Buffer, offset, length int32) int64

	// TryClaim a range in the publication to the consensus module for service ingress. The length excludes the
	// SessionMessageHeader which is written into the first SessionMessageHeaderLength bytes of the claim payload.
	// The application writes at claim.Offset()+SessionMessageHeaderLength and then calls claim.Commit().
	TryClaim(length int32, claim *logbuffer.Claim) int64

	// IdleStrategy returns the IdleStrategy which should be used by the service when it experiences back-pressure
	// on egress, closing sessions, making timer requests, or any long-running actions.
	IdleStrategy() idlestrategy.Idler
}
