package cluster

import (
	"fmt"
	"os"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
	"github.com/lirm/aeron-go/aeron/util/memmap"
)

const (
	HeaderLength      = 8 * 1024
	ErrorBufferLength = 1024 * 1024

	// MarkFileSchemaId, MarkFileSchemaVersion, MarkFileHeaderTemplateId and MarkFileHeaderBlockLength describe the
	// SBE encoding of the mark file header (aeron-cluster-mark-codecs.xml, 1.53.3).
	MarkFileSchemaId          = 110
	MarkFileSchemaVersion     = 2
	MarkFileHeaderTemplateId  = 200
	MarkFileHeaderBlockLength = 128
	// MarkFileHeaderOffset is the offset of the MarkFileHeader body after the SBE message header.
	MarkFileHeaderOffset = SBEHeaderLength

	markFileVersionFailed = -1
)

// MarkFileSemanticVersion is ClusterMarkFile.SEMANTIC_VERSION.
var MarkFileSemanticVersion = int32(util.SemanticVersionCompose(0, 3, 0))

// ClusterComponentType of the process owning a mark file.
type ClusterComponentType int32

const (
	ClusterComponentUnknown         ClusterComponentType = 0
	ClusterComponentConsensusModule ClusterComponentType = 1
	ClusterComponentContainer       ClusterComponentType = 2
	ClusterComponentBackup          ClusterComponentType = 3
	ClusterComponentStandby         ClusterComponentType = 4
)

// ClusterMarkFile is the mark file used by a clustered service container, binary compatible with
// io.aeron.cluster.service.ClusterMarkFile so Java tooling (ClusterTool) can read it.
type ClusterMarkFile struct {
	file      *memmap.File
	buffer    *atomic.Buffer
	flyweight *MarkFileHeaderFlyweight
}

// NewClusterMarkFile creates (or overwrites) a mark file of the given component type.
func NewClusterMarkFile(filename string, componentType ClusterComponentType, errorBufferLength int32) (
	*ClusterMarkFile, error) {
	if errorBufferLength < ErrorBufferLength {
		return nil, fmt.Errorf("invalid errorBufferLength=%d, min=%d", errorBufferLength, ErrorBufferLength)
	}
	_ = os.Remove(filename)
	f, err := memmap.NewFile(filename, 0, int(HeaderLength+errorBufferLength))
	if err != nil {
		return nil, err
	}

	b := atomic.MakeBuffer(f.GetMemoryPtr(), f.GetMemorySize())
	b.PutUInt16(0, MarkFileHeaderBlockLength)
	b.PutUInt16(2, MarkFileHeaderTemplateId)
	b.PutUInt16(4, MarkFileSchemaId)
	b.PutUInt16(6, MarkFileSchemaVersion)

	fly := &MarkFileHeaderFlyweight{}
	fly.Wrap(b, MarkFileHeaderOffset)

	fly.ComponentType.Set(int32(componentType))
	fly.CandidateTermId.Set(NullValue)
	fly.HeaderLength.Set(HeaderLength)
	fly.ErrorBufferLength.Set(errorBufferLength)
	fly.Pid.Set(int64(os.Getpid()))
	fly.StartTimestamp.Set(time.Now().UnixMilli())

	cmf := &ClusterMarkFile{file: f, buffer: b, flyweight: fly}
	cmf.EncodeStrings("", "", "", "", "", "")
	return cmf, nil
}

// Buffer returns the mapped mark file.
func (cmf *ClusterMarkFile) Buffer() *atomic.Buffer {
	return cmf.buffer
}

// Header returns the header flyweight.
func (cmf *ClusterMarkFile) Header() *MarkFileHeaderFlyweight {
	return cmf.flyweight
}

// EncodeStrings writes the variable length data fields in schema order.
func (cmf *ClusterMarkFile) EncodeStrings(aeronDirectory, controlChannel, ingressChannel, serviceName,
	authenticator, servicesClusterDir string) {
	offset := int32(MarkFileHeaderOffset + MarkFileHeaderBlockLength)
	for _, s := range []string{aeronDirectory, controlChannel, ingressChannel, serviceName, authenticator,
		servicesClusterDir} {
		b := []byte(s)
		if offset+4+int32(len(b)) > HeaderLength {
			b = b[:0]
		}
		cmf.buffer.PutInt32(offset, int32(len(b)))
		if len(b) > 0 {
			cmf.buffer.PutBytesArray(offset+4, &b, 0, int32(len(b)))
		}
		offset += 4 + int32(len(b))
	}
}

// DecodeStrings reads the variable length data fields (aeronDirectory, controlChannel, ingressChannel,
// serviceName, authenticator, servicesClusterDir).
func (cmf *ClusterMarkFile) DecodeStrings() []string {
	offset := int32(MarkFileHeaderOffset + MarkFileHeaderBlockLength)
	result := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		l := cmf.buffer.GetInt32(offset)
		result = append(result, string(cmf.buffer.GetBytesArray(offset+4, l)))
		offset += 4 + l
	}
	return result
}

func (cmf *ClusterMarkFile) SetArchiveStreamId(v int32)         { cmf.flyweight.ArchiveStreamId.Set(v) }
func (cmf *ClusterMarkFile) SetServiceStreamId(v int32)         { cmf.flyweight.ServiceStreamId.Set(v) }
func (cmf *ClusterMarkFile) SetConsensusModuleStreamId(v int32) { cmf.flyweight.ConsensusModuleStreamId.Set(v) }
func (cmf *ClusterMarkFile) SetIngressStreamId(v int32)         { cmf.flyweight.IngressStreamId.Set(v) }
func (cmf *ClusterMarkFile) SetMemberId(v int32)                { cmf.flyweight.MemberId.Set(v) }
func (cmf *ClusterMarkFile) SetServiceId(v int32)               { cmf.flyweight.ServiceId.Set(v) }
func (cmf *ClusterMarkFile) SetClusterId(v int32)               { cmf.flyweight.ClusterId.Set(v) }
func (cmf *ClusterMarkFile) SetCandidateTermId(v int64)         { cmf.flyweight.CandidateTermId.Set(v) }

// SignalReady stores the activity timestamp then publishes the semantic version with release semantics.
func (cmf *ClusterMarkFile) SignalReady(activityTimestampMs int64) {
	cmf.UpdateActivityTimestamp(activityTimestampMs)
	cmf.buffer.PutInt32Ordered(MarkFileHeaderOffset, MarkFileSemanticVersion)
}

// SignalFailedStart marks the file version as failed (Java VERSION_FAILED).
func (cmf *ClusterMarkFile) SignalFailedStart() {
	cmf.UpdateActivityTimestamp(NullValue)
	cmf.buffer.PutInt32Ordered(MarkFileHeaderOffset, markFileVersionFailed)
}

// SignalTerminated sets the activity timestamp to NullValue to indicate a clean shutdown.
func (cmf *ClusterMarkFile) SignalTerminated() {
	if cmf.file != nil {
		cmf.UpdateActivityTimestamp(NullValue)
	}
}

// UpdateActivityTimestamp updates the activity timestamp as proof of life (ordered write).
func (cmf *ClusterMarkFile) UpdateActivityTimestamp(timestampMs int64) {
	if cmf.file != nil {
		cmf.buffer.PutInt64Ordered(MarkFileHeaderOffset+8, timestampMs)
	}
}

// ActivityTimestampVolatile reads the activity timestamp.
func (cmf *ClusterMarkFile) ActivityTimestampVolatile() int64 {
	return cmf.buffer.GetInt64Volatile(MarkFileHeaderOffset + 8)
}

// VersionVolatile reads the mark file version.
func (cmf *ClusterMarkFile) VersionVolatile() int32 {
	return cmf.buffer.GetInt32Volatile(MarkFileHeaderOffset)
}

// Close unmaps the mark file.
func (cmf *ClusterMarkFile) Close() {
	if cmf.file != nil {
		if err := cmf.file.Close(); err != nil {
			logger.Errorf("error closing mark file: %v", err)
		}
		cmf.file = nil
	}
}
