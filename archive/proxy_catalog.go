// Copyright (C) 2021-2022 Talos, Inc.
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

package archive

import (
	"github.com/lirm/aeron-go/archive/codecs"
)

// ReplicateRequestWithParams sends a ReplicateRequest2 carrying every ReplicationParams field,
// matching Java ArchiveProxy.replicate(srcRecordingId, srcControlStreamId, srcControlChannel, replicationParams, ...).
// A nil params is treated as NewReplicationParams().
func (proxy *Proxy) ReplicateRequestWithParams(correlationID int64, srcRecordingID int64, srcControlStreamID int32, srcControlChannel string, params *ReplicationParams) error {
	if params == nil {
		params = NewReplicationParams()
	}
	return proxy.offerPacket(codecs.ReplicateRequest2FullPacket(
		proxy.marshaller, proxy.archive.Options.RangeChecking, proxy.archive.SessionID, correlationID,
		params.toCodec(srcRecordingID, srcControlStreamID, srcControlChannel)))
}
