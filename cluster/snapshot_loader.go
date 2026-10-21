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
	"bytes"
	"fmt"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/cluster/codecs"
)

type snapshotLoader struct {
	agent      *ClusteredServiceAgent
	img        aeron.Image
	marshaller *codecs.SbeGoMarshaller
	isDone     bool
	inSnapshot bool
	appVersion int32
	timeUnit   codecs.ClusterTimeUnitEnum
	err        error
	assembler  *aeron.ControlledFragmentAssembler
}

func newSnapshotLoader(agent *ClusteredServiceAgent, img aeron.Image) *snapshotLoader {
	loader := &snapshotLoader{agent: agent, img: img, marshaller: codecs.NewSbeGoMarshaller()}
	loader.assembler = aeron.NewControlledFragmentAssembler(loader.onControlledFragment, 0)
	return loader
}

// onControlledFragment stops the poll once the end marker is reached so the image is positioned at the start of
// the service's own snapshot state for ClusteredService.OnStart (Java ServiceSnapshotLoader returns BREAK).
func (loader *snapshotLoader) onControlledFragment(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) term.ControlledPollAction {
	loader.onFragment(buffer, offset, length, header)
	if loader.isDone {
		return term.ControlledPollActionBreak
	}
	return term.ControlledPollActionContinue
}

func (loader *snapshotLoader) poll() int {
	if loader.isDone {
		return 0
	}
	return loader.img.ControlledPoll(loader.assembler.OnFragment, 1)
}

func (loader *snapshotLoader) onFragment(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) {
	if length < SBEHeaderLength {
		return
	}
	blockLength := buffer.GetUInt16(offset)
	templateId := buffer.GetUInt16(offset + 2)
	schemaId := buffer.GetUInt16(offset + 4)
	version := buffer.GetUInt16(offset + 6)
	if schemaId != ClusterSchemaId {
		loader.err = fmt.Errorf("snapshot: expected schemaId=%d, actual=%d (templateId=%d blockLen=%d version=%d)",
			ClusterSchemaId, schemaId, templateId, blockLength, version)
		loader.isDone = true
		return
	}
	offset += SBEHeaderLength
	length -= SBEHeaderLength

	buf := &bytes.Buffer{}
	buffer.WriteBytes(buf, offset, length)

	switch templateId {
	case snapshotMarkerTemplateId:
		marker := &codecs.SnapshotMarker{}
		if err := marker.Decode(loader.marshaller, buf, version, blockLength, true); err != nil {
			loader.err = err
			loader.isDone = true
		} else if marker.TypeId != snapshotTypeId {
			loader.err = fmt.Errorf("unexpected snapshot type: %d", marker.TypeId)
			loader.isDone = true
		} else if marker.Mark == codecs.SnapshotMark.BEGIN {
			if loader.inSnapshot {
				loader.err = fmt.Errorf("already in snapshot, pos=%d", header.Position())
				loader.isDone = true
			} else {
				loader.inSnapshot = true
				loader.appVersion = marker.AppVersion
				loader.timeUnit = marker.TimeUnit
			}
		} else if marker.Mark == codecs.SnapshotMark.END {
			if !loader.inSnapshot {
				loader.err = fmt.Errorf("missing beging snapshot, pos=%d", header.Position())
				loader.isDone = true
			} else {
				loader.inSnapshot = false
				loader.isDone = true
			}
		} else {
			loader.err = fmt.Errorf("unexpected snapshot mark, pos=%d inSnapshot=%v mark=%v", header.Position(), loader.inSnapshot, marker.Mark)
			loader.isDone = true
		}
	case clientSessionTemplateId:
		s := codecs.ClientSession{}
		if err := s.Decode(loader.marshaller, buf, version, blockLength, true); err != nil {
			loader.err = err
			loader.isDone = true
		} else {
			loader.agent.addSessionFromSnapshot(newContainerClientSession(
				s.ClusterSessionId, s.ResponseStreamId, string(s.ResponseChannel), s.EncodedPrincipal, loader.agent))
		}
	default:
		logger.Debugf("SnapshotLoader: unknown templateId=%d at pos=%d", templateId, header.Position())
	}
}
