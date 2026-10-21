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

package counters

import (
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
)

// WrapKey wraps keyBuf over the MaxKeyLength-byte key region of counterId's metadata record and returns the
// record state (volatile read) and type id. ok is false if counterId is outside [0, maxCounterId], in which case
// keyBuf is left untouched. Callers iterating all counters should stop at the first RecordUnused state, matching
// CountersReader.forEach in Java.
func (reader *Reader) WrapKey(counterId int32, keyBuf *atomic.Buffer) (state int32, typeId int32, ok bool) {
	if counterId < 0 || counterId > int32(reader.maxCounterID) {
		return RecordUnused, 0, false
	}
	metaDataOffset := counterId * MetadataLength
	if metaDataOffset+MetadataLength > reader.metaData.Capacity() {
		return RecordUnused, 0, false
	}
	state = reader.metaData.GetInt32Volatile(metaDataOffset)
	typeId = reader.metaData.GetInt32(metaDataOffset + TypeIdOffset)
	keyBuf.Wrap(unsafe.Pointer(uintptr(reader.metaData.Ptr())+uintptr(metaDataOffset+KeyOffset)), MaxKeyLength)
	return state, typeId, true
}
