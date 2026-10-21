/*
Copyright 2016-2018 Stanislav Liberman
Copyright 2022 Talos, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// See  ~agrona/agrona/src/main/java/org/agrona/concurrent/status/CountersReader.java
//
// Values Buffer
//
//   0                   1                   2                   3
//   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                        Counter Value                          |
//  |                                                               |
//  +---------------------------------------------------------------+
//  |                       Registration Id                         |
//  |                                                               |
//  +---------------------------------------------------------------+
//  |                          Owner Id                             |
//  |                                                               |
//  +---------------------------------------------------------------+
//  |                     104 bytes of padding                     ...
// ...                                                              |
//  +---------------------------------------------------------------+
//  |                   Repeats to end of buffer                   ...
//  |                                                               |
// ...                                                              |
//  +---------------------------------------------------------------+
//
// Meta Data Buffer
//
//   0                   1                   2                   3
//   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                        Record State                           |
//  +---------------------------------------------------------------+
//  |                          Type Id                              |
//  +---------------------------------------------------------------+
//  |                  Free-for-reuse Deadline (ms)                 |
//  |                                                               |
//  +---------------------------------------------------------------+
//  |                      112 bytes for key                       ...
// ...                                                              |
//  +-+-------------------------------------------------------------+
//  |R|                      Label Length                           |
//  +-+-------------------------------------------------------------+
//  |                     380 bytes of Label                       ...
// ...                                                              |
//  +---------------------------------------------------------------+
//  |                   Repeats to end of buffer                   ...
//  |                                                               |
// ...                                                              |
//  +---------------------------------------------------------------+

package counters

import (
	"fmt"
	"unsafe"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
)

const CounterLength = util.CacheLineLength * 2

const FullLabelLength = util.CacheLineLength * 6
const LabelOffset = util.CacheLineLength * 2
const MetadataLength = LabelOffset + FullLabelLength
const MaxLabelLength = FullLabelLength - util.SizeOfInt32
const MaxKeyLength = (util.CacheLineLength * 2) - (util.SizeOfInt32 * 2) - util.SizeOfInt64

const TypeIdOffset = util.SizeOfInt32

// FreeForReuseDeadlineOffset is the offset in the record at which the deadline (in milliseconds) for when counter may be reused.
const FreeForReuseDeadlineOffset = TypeIdOffset + util.SizeOfInt32
const KeyOffset = FreeForReuseDeadlineOffset + util.SizeOfInt64

const RecordReclaimed int32 = -1
const RecordUnused int32 = 0
const RecordAllocated int32 = 1

const NullCounterId = int32(-1)

// NotFreeToReuse is the deadline value used to indicate a counter is not free to be reused.
const NotFreeToReuse = int64(^uint64(0) >> 1)

const (
	// DefaultTypeId is the default type id of a counter when none is supplied.
	DefaultTypeId int32 = 0
	// DefaultRegistrationId is the default registration id of a counter when none is set.
	DefaultRegistrationId int64 = 0
	// DefaultOwnerId is the default owner id of a counter when none is set.
	DefaultOwnerId int64 = 0
	// DefaultReferenceId is the default reference id of a counter when none is set.
	DefaultReferenceId int64 = 0
)

const RegistrationIdOffset = util.SizeOfInt64
const OwnerIdOffset = RegistrationIdOffset + util.SizeOfInt64
const ReferenceIdOffset = OwnerIdOffset + util.SizeOfInt64

// Reader is the Go equivalent of Agrona's CountersReader: a read-only view over the counters metadata and values
// buffers of the CnC file.
type Reader struct {
	metaData *atomic.Buffer
	values   *atomic.Buffer

	// maxCounterID is inclusive, i.e. the highest valid counter id (matches Java's maxCounterId).
	maxCounterID int
}

// Counter is a snapshot of an allocated counter produced by Scan.
type Counter struct {
	Id     int32
	TypeId int32
	Value  int64
	Label  string
}

func NewReader(values, metaData *atomic.Buffer) *Reader {
	reader := Reader{metaData: metaData, values: values}
	reader.maxCounterID = int(values.Capacity()/CounterLength) - 1

	return &reader
}

// CounterOffset returns the offset in the values buffer for a given counter id.
func CounterOffset(counterId int32) int32 {
	return counterId * CounterLength
}

// MetaDataOffset returns the offset in the metadata buffer for a given counter id.
func MetaDataOffset(counterId int32) int32 {
	return counterId * MetadataLength
}

func counterOffset(counterId int32) int32 {
	return counterId * CounterLength
}

// MaxCounterId returns the maximum (inclusive) counter id which can be supported given the length of the values
// buffer.
func (reader *Reader) MaxCounterId() int32 {
	return int32(reader.maxCounterID)
}

// MetaDataBuffer returns the underlying metadata buffer.
func (reader *Reader) MetaDataBuffer() *atomic.Buffer {
	return reader.metaData
}

// ValuesBuffer returns the underlying values buffer.
func (reader *Reader) ValuesBuffer() *atomic.Buffer {
	return reader.values
}

func (reader *Reader) Scan(cb func(Counter)) {
	bytesToString := func(labelBytes []byte) string {
		return string(labelBytes)
	}
	reader.scan(cb, bytesToString)
}

// ScanUnsafe callbacks must only consume the label argument within  the callback or must deep copy it
// because the underlying memory be reused for the next counter within scan.
func (reader *Reader) ScanUnsafe(cb func(Counter)) {
	bytesToString := func(labelBytes []byte) string {
		return unsafe.String(unsafe.SliceData(labelBytes), len(labelBytes))
	}
	reader.scan(cb, bytesToString)
}

func (reader *Reader) scan(cb func(Counter), bytesToString func(labelBytes []byte) string) {
	var id int32 = 0
	var i int32 = 0

	label := make([]byte, 0, FullLabelLength)
	for capacity := reader.metaData.Capacity(); i+MetadataLength <= capacity; i += MetadataLength {
		recordStatus := reader.metaData.GetInt32Volatile(i)
		if RecordUnused == recordStatus {
			break
		} else if RecordAllocated == recordStatus {
			typeId := reader.metaData.GetInt32(i + TypeIdOffset)
			label = reader.labelValueUnsafe(i, label[:0])
			value := reader.values.GetInt64Volatile(counterOffset(id))

			cb(Counter{Id: id, TypeId: typeId, Value: value, Label: bytesToString(label)})
		}
		id++
	}
}

// ScanForType calls the callback for every allocated counter of the given type until the callback returns false.
// The key buffer is only valid for the duration of the callback.
func (reader *Reader) ScanForType(typeId int32, callback func(counterId int32, keyBuffer *atomic.Buffer) bool) {
	var keyBuf atomic.Buffer
	for id := 0; id <= reader.maxCounterID; id++ {
		counterId := int32(id)
		metaDataOffset := counterId * MetadataLength
		recordStatus := reader.metaData.GetInt32Volatile(metaDataOffset)
		if recordStatus == RecordUnused {
			break
		} else if RecordAllocated == recordStatus {
			if reader.metaData.GetInt32(metaDataOffset+TypeIdOffset) == typeId {
				reader.wrapKey(&keyBuf, metaDataOffset)
				if !callback(counterId, &keyBuf) {
					break
				}
			}
		}
	}
}

// FindCounter returns the first allocated counter of the given type whose key matches keyFilter (nil matches any),
// or NullCounterId.
func (reader *Reader) FindCounter(typeId int32, keyFilter func(keyBuffer *atomic.Buffer) bool) int32 {
	var keyBuf atomic.Buffer
	for id := 0; id <= reader.maxCounterID; id++ {
		metaDataOffset := int32(id) * MetadataLength
		recordStatus := reader.metaData.GetInt32Volatile(metaDataOffset)
		if recordStatus == RecordUnused {
			break
		} else if RecordAllocated == recordStatus {
			if reader.metaData.GetInt32(metaDataOffset+TypeIdOffset) == typeId {
				reader.wrapKey(&keyBuf, metaDataOffset)
				if keyFilter == nil || keyFilter(&keyBuf) {
					return int32(id)
				}
			}
		}
	}
	return NullCounterId
}

func (reader *Reader) wrapKey(keyBuf *atomic.Buffer, metaDataOffset int32) {
	keyBuf.Wrap(unsafe.Add(reader.metaData.Ptr(), metaDataOffset+KeyOffset), MaxKeyLength)
}

// GetKeyPartInt32 returns an int32 portion of the key at the specified offset
func (reader *Reader) GetKeyPartInt32(counterId int32, offset int32) (int32, error) {
	if err := reader.validateCounterIdAndOffset(counterId, offset, util.SizeOfInt32); err != nil {
		return 0, err
	}
	metaDataOffset := counterId * MetadataLength
	recordStatus := reader.metaData.GetInt32Volatile(metaDataOffset)
	if recordStatus != RecordAllocated {
		return 0, fmt.Errorf("counterId=%d recordStatus=%d", counterId, recordStatus)
	}
	return reader.metaData.GetInt32(metaDataOffset + KeyOffset + offset), nil
}

// GetKeyPartInt64 returns an int64 portion of the key at the specified offset
func (reader *Reader) GetKeyPartInt64(counterId int32, offset int32) (int64, error) {
	if err := reader.validateCounterIdAndOffset(counterId, offset, util.SizeOfInt64); err != nil {
		return 0, err
	}
	metaDataOffset := counterId * MetadataLength
	recordStatus := reader.metaData.GetInt32Volatile(metaDataOffset)
	if recordStatus != RecordAllocated {
		return 0, fmt.Errorf("counterId=%d recordStatus=%d", counterId, recordStatus)
	}
	return reader.metaData.GetInt64(metaDataOffset + KeyOffset + offset), nil
}

// GetKeyPartString returns a string portion of the key, assuming the string is prefixed by its length
// (as an 32-bit int) at the specified offset.
func (reader *Reader) GetKeyPartString(counterId int32, offset int32) (string, error) {
	if err := reader.validateCounterIdAndOffset(counterId, offset, util.SizeOfInt32); err != nil {
		return "", err
	}
	metaDataOffset := counterId * MetadataLength
	recordStatus := reader.metaData.GetInt32Volatile(metaDataOffset)
	if recordStatus != RecordAllocated {
		return "", fmt.Errorf("counterId=%d recordStatus=%d", counterId, recordStatus)
	}
	lengthOffset := metaDataOffset + KeyOffset + offset
	length := reader.metaData.GetInt32(lengthOffset)
	if length < 0 || (offset+util.SizeOfInt32+length) > MaxKeyLength {
		return "", fmt.Errorf("counterId=%d offset=%d length=%d", counterId, offset, length)
	}
	return string(reader.metaData.GetBytesArray(lengthOffset+util.SizeOfInt32, length)), nil
}

// GetCounterValue returns the value of the given counter id (as a volatile read), or 0 if the id is out of range.
func (reader *Reader) GetCounterValue(counterId int32) int64 {
	if !reader.isValidCounterId(counterId) {
		return 0
	}
	return reader.values.GetInt64Volatile(counterOffset(counterId))
}

// GetCounterRegistrationId returns the registration id of the counter, or DefaultRegistrationId if out of range.
func (reader *Reader) GetCounterRegistrationId(counterId int32) int64 {
	if !reader.isValidCounterId(counterId) {
		return DefaultRegistrationId
	}
	return reader.values.GetInt64Volatile(counterOffset(counterId) + RegistrationIdOffset)
}

// GetCounterOwnerId returns the owner (client) id of the counter, or DefaultOwnerId if out of range.
func (reader *Reader) GetCounterOwnerId(counterId int32) int64 {
	if !reader.isValidCounterId(counterId) {
		return DefaultOwnerId
	}
	return reader.values.GetInt64Volatile(counterOffset(counterId) + OwnerIdOffset)
}

// GetCounterTypeId returns the type id for a counter, or -1 if out of range.
func (reader *Reader) GetCounterTypeId(counterId int32) int32 {
	if !reader.isValidCounterId(counterId) {
		return -1
	}
	return reader.metaData.GetInt32(counterId*MetadataLength + TypeIdOffset)
}

func (reader *Reader) IsCounterAllocated(counterId int32) bool {
	return reader.isValidCounterId(counterId) &&
		reader.metaData.GetInt32Volatile(counterId*MetadataLength) == RecordAllocated
}

func (reader *Reader) isValidCounterId(counterId int32) bool {
	return counterId >= 0 && int(counterId) <= reader.maxCounterID
}

// ValidateCounterId returns an error if counterId is outside 0..MaxCounterId.
func (reader *Reader) ValidateCounterId(counterId int32) error {
	if !reader.isValidCounterId(counterId) {
		return fmt.Errorf("counter id %d out of range: 0 - maxCounterId=%d", counterId, reader.maxCounterID)
	}
	return nil
}

func (reader *Reader) validateCounterIdAndOffset(counterId int32, offset int32, size int32) error {
	if err := reader.ValidateCounterId(counterId); err != nil {
		return err
	}
	if offset < 0 || offset+size > MaxKeyLength {
		return fmt.Errorf("counterId=%d offset=%d size=%d maxKeyLength=%d", counterId, offset, size, MaxKeyLength)
	}
	return nil
}

func (reader *Reader) labelLength(metaDataOffset int32) int32 {
	labelSize := reader.metaData.GetInt32Volatile(metaDataOffset + LabelOffset)
	if labelSize < 0 {
		return 0
	}
	if labelSize > MaxLabelLength {
		return MaxLabelLength
	}
	return labelSize
}

func (reader *Reader) labelValue(metaDataOffset int32) string {
	labelSize := reader.labelLength(metaDataOffset)
	return string(reader.metaData.GetBytesArray(metaDataOffset+LabelOffset+util.SizeOfInt32, labelSize))
}

func (reader *Reader) labelValueUnsafe(metaDataOffset int32, label []byte) []byte {
	label = label[:reader.labelLength(metaDataOffset)]
	reader.metaData.GetBytes(metaDataOffset+LabelOffset+util.SizeOfInt32, label)
	return label
}
