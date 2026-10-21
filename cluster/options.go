package cluster

import (
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/util"

	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/archive"
	"go.uber.org/zap/zapcore"
)

type Options struct {
	Timeout                 time.Duration      // [runtime] How long to try sending/receiving control messages
	IdleStrategy            idlestrategy.Idler // [runtime] Idlestrategy for sending/receiving control messagesA
	RangeChecking           bool               // [runtime] archive protocol marshalling checks
	Loglevel                zapcore.Level      // [runtime] via logging.SetLevel()
	ClusterDir              string
	ClusterId               int32
	ServiceId               int32
	AppVersion              int32
	ControlChannel          string
	ConsensusModuleStreamId int32
	ServiceStreamId         int32
	SnapshotChannel         string
	SnapshotStreamId        int32
	ReplayChannel           string
	ReplayStreamId          int32
	ArchiveOptions          *archive.Options
	LogFragmentLimit        int
	// IsRespondingService controls whether egress publications are created for client sessions when leader
	// (Java: ClusteredServiceContainer.Context.isRespondingService). Default true.
	IsRespondingService bool
	// AppVersionValidator checks the configured AppVersion against the version found in the log / snapshot.
	// Defaults to SemanticVersionMajorValidator (Java: AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR).
	AppVersionValidator func(configured, logOrSnapshot int32) bool
	// ServiceName is recorded in the service mark file.
	ServiceName string
	// AeronClient optionally supplies an existing client; otherwise the agent connects using the aeron.Context
	// and closes the client on termination.
	AeronClient *aeron.Aeron
}

// SemanticVersionMajorValidator accepts versions with the same semantic major version.
func SemanticVersionMajorValidator(configured, logOrSnapshot int32) bool {
	return util.SemanticVersionMajor(uint32(configured)) == util.SemanticVersionMajor(uint32(logOrSnapshot))
}

func NewOptions() *Options {
	archiveOpts := archive.DefaultOptions()
	archiveOpts.RequestChannel = "aeron:ipc?alias=cluster-service-archive-ctrl-req|term-length=128k"
	archiveOpts.ResponseChannel = "aeron:ipc?alias=cluster-service-archive-ctrl-resp|term-length=128k"
	return &Options{
		Timeout:                 time.Second * 5,
		IdleStrategy:            idlestrategy.NewDefaultBackoffIdleStrategy(),
		RangeChecking:           true,
		Loglevel:                zapcore.WarnLevel,
		ClusterDir:              "/tmp/aeron-cluster",
		ControlChannel:          "aeron:ipc?term-length=128k|alias=service-control",
		ConsensusModuleStreamId: 105,
		ServiceStreamId:         104,
		SnapshotChannel:         "aeron:ipc?alias=snapshot",
		SnapshotStreamId:        106,
		ReplayChannel:           "aeron:ipc?alias=service-replay",
		ReplayStreamId:          103,
		ArchiveOptions:          archiveOpts,
		LogFragmentLimit:        50,
		IsRespondingService:     true,
		AppVersionValidator:     SemanticVersionMajorValidator,
		ServiceName:             "clustered-service",
	}
}
