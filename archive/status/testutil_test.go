// Copyright 2026 Kalshi Trading
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

package status

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
)

// fakeCounters lays out counter records the same way the Java CountersManager does.
type fakeCounters struct {
	metaData *atomic.Buffer
	values   *atomic.Buffer
	next     int32
}

func newFakeCounters(numCounters int32) *fakeCounters {
	return &fakeCounters{
		metaData: atomic.MakeBuffer(make([]byte, numCounters*counters.MetadataLength)),
		values:   atomic.MakeBuffer(make([]byte, numCounters*counters.CounterLength)),
	}
}

func (f *fakeCounters) reader() *counters.Reader {
	return counters.NewReader(f.values, f.metaData)
}

func (f *fakeCounters) allocate(typeId int32, key func(key *atomic.Buffer)) int32 {
	id := f.next
	f.next++
	offset := id * counters.MetadataLength
	f.metaData.PutInt32(offset+counters.TypeIdOffset, typeId)
	if key != nil {
		kb := atomic.MakeBuffer(make([]byte, counters.MaxKeyLength))
		key(kb)
		f.metaData.PutBytes(offset+counters.KeyOffset, kb, 0, counters.MaxKeyLength)
	}
	f.metaData.PutInt32Ordered(offset, counters.RecordAllocated)
	return id
}

func (f *fakeCounters) setState(id int32, state int32) {
	f.metaData.PutInt32Ordered(id*counters.MetadataLength, state)
}

func (f *fakeCounters) setValue(id int32, value int64) {
	f.values.PutInt64Ordered(id*counters.CounterLength, value)
}

func recordingPosKey(recordingId int64, sessionId int32, sourceIdentity string, archiveId int64) func(*atomic.Buffer) {
	return func(key *atomic.Buffer) {
		key.PutInt64(RecordingIdOffset, recordingId)
		key.PutInt32(SessionIdOffset, sessionId)
		if int32(len(sourceIdentity)) > MaxSourceIdentityLength {
			sourceIdentity = sourceIdentity[:MaxSourceIdentityLength]
		}
		b := []byte(sourceIdentity)
		key.PutInt32(SourceIdentityLengthOffset, int32(len(b)))
		if len(b) > 0 {
			key.PutBytesArray(SourceIdentityOffset, &b, 0, int32(len(b)))
		}
		key.PutInt64(SourceIdentityOffset+int32(len(b)), archiveId)
	}
}
