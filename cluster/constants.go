package cluster

import "fmt"

const (
	SBEHeaderLength            = 8
	SessionMessageHeaderLength = 24
)

// Role of the cluster member hosting the service, matching io.aeron.cluster.service.Cluster.Role.
type Role int32

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return fmt.Sprintf("Role(%d)", int32(r))
	}
}

const (
	ClusterSchemaId                 = 111
	ClusterSchemaVersion            = 17
	SessionMessageHeaderTemplateId  = 1
	SessionEventTemplateId          = 2
	SessionCloseRequestTemplateId   = 4
	SessionKeepAliveTemplateId      = 5
	NewLeaderEventTemlateId         = 6
	ChallengeTemplateId             = 7
	timerEventTemplateId            = 20
	sessionOpenTemplateId           = 21
	sessionCloseTemplateId          = 22
	clusterActionReqTemplateId      = 23
	newLeadershipTermTemplateId     = 24
	membershipChangeTemplateId      = 25
	closeSessionTemplateId          = 30
	scheduleTimerTemplateId         = 31
	cancelTimerTemplateId           = 32
	serviceAckTemplateId            = 33
	clusterMembersQueryTemplateId   = 34
	joinLogTemplateId               = 40
	serviceTerminationPosTemplateId = 42
	snapshotMarkerTemplateId        = 100
	clientSessionTemplateId         = 102
	requestServiceAckTemplateId     = 108
)

const SessionMessageHdrBlockLength = 24

// Cluster action flags carried on ClusterActionRequest (ConsensusModule.CLUSTER_ACTION_FLAGS_*).
const (
	ClusterActionFlagsDefault         int32 = 0
	ClusterActionFlagsStandbySnapshot int32 = 1
)
