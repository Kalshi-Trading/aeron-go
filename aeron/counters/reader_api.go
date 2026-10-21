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
	"github.com/lirm/aeron-go/aeron/atomic"
)

// ForEachLabel calls consumer with the id and label of every allocated counter (CountersReader.forEach(IntObjConsumer)).
func (reader *Reader) ForEachLabel(consumer func(counterId int32, label string)) {
	reader.forEachAllocated(func(counterId int32, metaDataOffset int32) {
		consumer(counterId, reader.labelValue(metaDataOffset))
	})
}

// ForEach calls consumer with the value, id and label of every allocated counter
// (CountersReader.forEach(CounterConsumer)).
func (reader *Reader) ForEach(consumer func(value int64, counterId int32, label string)) {
	reader.forEachAllocated(func(counterId int32, metaDataOffset int32) {
		label := reader.labelValue(metaDataOffset)
		consumer(reader.values.GetInt64Volatile(counterOffset(counterId)), counterId, label)
	})
}

// ForEachMetaData calls consumer with the id, type id, key buffer and label of every allocated counter
// (CountersReader.forEach(MetaData)). The key buffer is MaxKeyLength long and only valid during the callback.
func (reader *Reader) ForEachMetaData(consumer func(counterId int32, typeId int32, keyBuffer *atomic.Buffer, label string)) {
	var keyBuf atomic.Buffer
	reader.forEachAllocated(func(counterId int32, metaDataOffset int32) {
		typeId := reader.metaData.GetInt32(metaDataOffset + TypeIdOffset)
		label := reader.labelValue(metaDataOffset)
		reader.wrapKey(&keyBuf, metaDataOffset)
		consumer(counterId, typeId, &keyBuf, label)
	})
}

func (reader *Reader) forEachAllocated(fn func(counterId int32, metaDataOffset int32)) {
	var counterId int32
	for offset, capacity := int32(0), reader.metaData.Capacity(); offset+MetadataLength <= capacity && int(counterId) <= reader.maxCounterID; offset += MetadataLength {
		recordStatus := reader.metaData.GetInt32Volatile(offset)
		if RecordAllocated == recordStatus {
			fn(counterId, offset)
		} else if RecordUnused == recordStatus {
			break
		}
		counterId++
	}
}

// FindByRegistrationId returns the id of the first allocated counter with the given registration id, or
// NullCounterId.
func (reader *Reader) FindByRegistrationId(registrationId int64) int32 {
	result := NullCounterId
	reader.findAllocated(func(counterId int32, metaDataOffset int32) bool {
		if registrationId == reader.values.GetInt64Volatile(counterOffset(counterId)+RegistrationIdOffset) {
			result = counterId
			return true
		}
		return false
	})
	return result
}

// FindByTypeIdAndRegistrationId returns the id of the first allocated counter with the given type id and registration
// id, or NullCounterId.
func (reader *Reader) FindByTypeIdAndRegistrationId(typeId int32, registrationId int64) int32 {
	result := NullCounterId
	reader.findAllocated(func(counterId int32, metaDataOffset int32) bool {
		if typeId == reader.metaData.GetInt32(metaDataOffset+TypeIdOffset) &&
			registrationId == reader.values.GetInt64Volatile(counterOffset(counterId)+RegistrationIdOffset) {
			result = counterId
			return true
		}
		return false
	})
	return result
}

func (reader *Reader) findAllocated(match func(counterId int32, metaDataOffset int32) bool) {
	var counterId int32
	for offset, capacity := int32(0), reader.metaData.Capacity(); offset+MetadataLength <= capacity && int(counterId) <= reader.maxCounterID; offset += MetadataLength {
		recordStatus := reader.metaData.GetInt32Volatile(offset)
		if RecordAllocated == recordStatus {
			if match(counterId, offset) {
				return
			}
		} else if RecordUnused == recordStatus {
			return
		}
		counterId++
	}
}

// GetCounterReferenceId returns the reference id of the counter, or DefaultReferenceId if out of range.
func (reader *Reader) GetCounterReferenceId(counterId int32) int64 {
	if !reader.isValidCounterId(counterId) {
		return DefaultReferenceId
	}
	return reader.values.GetInt64(counterOffset(counterId) + ReferenceIdOffset)
}

// GetCounterState returns the record state (RecordUnused, RecordAllocated, RecordReclaimed) of the counter, or
// RecordUnused if out of range.
func (reader *Reader) GetCounterState(counterId int32) int32 {
	if !reader.isValidCounterId(counterId) {
		return RecordUnused
	}
	return reader.metaData.GetInt32Volatile(MetaDataOffset(counterId))
}

// GetFreeForReuseDeadline returns the deadline (epoch ms) after which the counter may be reused, NotFreeToReuse if
// it is still in use, or NotFreeToReuse if out of range.
func (reader *Reader) GetFreeForReuseDeadline(counterId int32) int64 {
	if !reader.isValidCounterId(counterId) {
		return NotFreeToReuse
	}
	return reader.metaData.GetInt64(MetaDataOffset(counterId) + FreeForReuseDeadlineOffset)
}

// GetCounterLabel returns the label of the counter, or "" if out of range.
func (reader *Reader) GetCounterLabel(counterId int32) string {
	if !reader.isValidCounterId(counterId) {
		return ""
	}
	return reader.labelValue(MetaDataOffset(counterId))
}

// GetCounterKey returns a copy of the full MaxKeyLength key region of the counter, or nil if out of range.
func (reader *Reader) GetCounterKey(counterId int32) []byte {
	if !reader.isValidCounterId(counterId) {
		return nil
	}
	return reader.metaData.GetBytesArray(MetaDataOffset(counterId)+KeyOffset, MaxKeyLength)
}

// KeyBuffer wraps the key region of the counter's metadata (MaxKeyLength bytes) without copying, or returns nil if
// out of range.
func (reader *Reader) KeyBuffer(counterId int32) *atomic.Buffer {
	if !reader.isValidCounterId(counterId) {
		return nil
	}
	var keyBuf atomic.Buffer
	reader.wrapKey(&keyBuf, MetaDataOffset(counterId))
	return &keyBuf
}
