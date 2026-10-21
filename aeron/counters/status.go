package counters

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
)

// Channel endpoint status values (io.aeron.status.ChannelEndpointStatus).
const (
	ChannelEndpointStatusInitializing int64 = 0
	ChannelEndpointStatusErrored      int64 = -1
	ChannelEndpointStatusActive       int64 = 1
	ChannelEndpointStatusClosing      int64 = 2
	// ChannelEndpointStatusNoIdAllocated is the counter id used when no status counter is allocated.
	ChannelEndpointStatusNoIdAllocated int32 = -1
	// ChannelEndpointStatusChannelOffset is the key offset of the length-prefixed channel string.
	ChannelEndpointStatusChannelOffset int32 = 0
	// ChannelEndpointStatusMaxChannelLength is the max channel length that fits in the key.
	ChannelEndpointStatusMaxChannelLength = MaxKeyLength - (ChannelEndpointStatusChannelOffset + util.SizeOfInt32)
)

// ChannelEndpointStatusString returns the textual form of a channel endpoint status value.
func ChannelEndpointStatusString(status int64) string {
	switch status {
	case ChannelEndpointStatusInitializing:
		return "INITIALIZING"
	case ChannelEndpointStatusErrored:
		return "ERRORED"
	case ChannelEndpointStatusActive:
		return "ACTIVE"
	case ChannelEndpointStatusClosing:
		return "CLOSING"
	default:
		return fmt.Sprintf("unknown id=%d", status)
	}
}

// HeartbeatTypeId is the type id of client heartbeat counters (io.aeron.status.HeartbeatTimestamp.HEARTBEAT_TYPE_ID).
const HeartbeatTypeId = DriverHeartbeatTypeId

// HeartbeatRegistrationIdOffset is the key offset of the registration id in a heartbeat counter.
const HeartbeatRegistrationIdOffset int32 = 0

// FindHeartbeatCounterIdByRegistrationId finds the heartbeat counter of the client/agent with the given registration
// id (HeartbeatTimestamp.findCounterIdByRegistrationId). Returns NullCounterId if not found.
func FindHeartbeatCounterIdByRegistrationId(reader *Reader, counterTypeId int32, registrationId int64) int32 {
	return reader.FindCounter(counterTypeId, func(keyBuffer *atomic.Buffer) bool {
		return keyBuffer.GetInt64(HeartbeatRegistrationIdOffset) == registrationId
	})
}

// IsHeartbeatActive reports whether the counter is still allocated as a heartbeat of the given type and registration
// id (HeartbeatTimestamp.isActive).
func IsHeartbeatActive(reader *Reader, counterId int32, counterTypeId int32, registrationId int64) bool {
	if !reader.IsCounterAllocated(counterId) || reader.GetCounterTypeId(counterId) != counterTypeId {
		return false
	}
	v, err := reader.GetKeyPartInt64(counterId, HeartbeatRegistrationIdOffset)
	return err == nil && v == registrationId
}

// Key layout shared by the driver's stream position counters (PublisherLimit, PublisherPos, SenderPos, SenderLimit,
// ReceiverHwm, ReceiverPos, SubscriberPos): registrationId, sessionId, streamId, channel (length-prefixed).
const (
	StreamCounterRegistrationIdOffset int32 = 0
	StreamCounterSessionIdOffset            = StreamCounterRegistrationIdOffset + util.SizeOfInt64
	StreamCounterStreamIdOffset             = StreamCounterSessionIdOffset + util.SizeOfInt32
	StreamCounterChannelOffset              = StreamCounterStreamIdOffset + util.SizeOfInt32
	StreamCounterMaxChannelLength           = MaxKeyLength - (StreamCounterChannelOffset + util.SizeOfInt32)
)

// Label prefixes used by the driver for stream position counters.
const (
	PublisherLimitName       = "pub-lmt"
	PublisherPosName         = "pub-pos (sampled)"
	SenderPosName            = "snd-pos"
	SenderLimitName          = "snd-lmt"
	ReceiverHwmName          = "rcv-hwm"
	ReceiverPosName          = "rcv-pos"
	SubscriberPosName        = "sub-pos"
	SendChannelStatusName    = "snd-channel"
	ReceiveChannelStatusName = "rcv-channel"
)

// StreamCounterKey is the decoded key of a stream position counter.
type StreamCounterKey struct {
	RegistrationId int64
	SessionId      int32
	StreamId       int32
	Channel        string
}

// GetStreamCounterKey decodes the key of a stream position counter.
func GetStreamCounterKey(reader *Reader, counterId int32) (StreamCounterKey, error) {
	var k StreamCounterKey
	var err error
	if k.RegistrationId, err = reader.GetKeyPartInt64(counterId, StreamCounterRegistrationIdOffset); err != nil {
		return k, err
	}
	if k.SessionId, err = reader.GetKeyPartInt32(counterId, StreamCounterSessionIdOffset); err != nil {
		return k, err
	}
	if k.StreamId, err = reader.GetKeyPartInt32(counterId, StreamCounterStreamIdOffset); err != nil {
		return k, err
	}
	k.Channel, err = reader.GetKeyPartString(counterId, StreamCounterChannelOffset)
	return k, err
}

// FindStreamCounter finds the first allocated counter of typeId whose key matches the registration id and, when
// sessionId is not nil, the session id. Returns NullCounterId if not found. Useful for the publisher limit
// (DriverPublisherLimitTypeId), publisher position (DriverPublisherPosTypeId), sender limit (DriverSenderLimitTypeId),
// receiver position (DriverReceiverPosTypeId) and subscriber position (DriverSubscriberPositionTypeId) counters.
func FindStreamCounter(reader *Reader, typeId int32, registrationId int64, sessionId *int32) int32 {
	return reader.FindCounter(typeId, func(keyBuffer *atomic.Buffer) bool {
		if keyBuffer.GetInt64(StreamCounterRegistrationIdOffset) != registrationId {
			return false
		}
		return sessionId == nil || keyBuffer.GetInt32(StreamCounterSessionIdOffset) == *sessionId
	})
}

// GetChannelEndpointStatusChannel returns the channel of a send/receive channel status counter.
func GetChannelEndpointStatusChannel(reader *Reader, counterId int32) (string, error) {
	return reader.GetKeyPartString(counterId, ChannelEndpointStatusChannelOffset)
}

// FindChannelEndpointStatus finds the send (DriverSendChannelStatusTypeId) or receive
// (DriverReceiveChannelStatusTypeId) channel status counter for a channel. Returns NullCounterId if not found.
func FindChannelEndpointStatus(reader *Reader, typeId int32, channel string) int32 {
	result := NullCounterId
	reader.ScanForType(typeId, func(counterId int32, _ *atomic.Buffer) bool {
		if c, err := GetChannelEndpointStatusChannel(reader, counterId); err == nil && c == channel {
			result = counterId
			return false
		}
		return true
	})
	return result
}

// ValidateCounterTypeId returns an error if the counter's type id does not match (AeronCounters.validateCounterTypeId).
func ValidateCounterTypeId(reader *Reader, counterId int32, expectedTypeId int32) error {
	if typeId := reader.GetCounterTypeId(counterId); typeId != expectedTypeId {
		return fmt.Errorf("the type for counterId=%d, typeId=%d does not match the expected=%d",
			counterId, typeId, expectedTypeId)
	}
	return nil
}

// AppendToLabel appends value to the label of an allocated counter, truncating to MaxLabelLength. Returns the number
// of bytes written (AeronCounters.appendToLabel).
func AppendToLabel(metaDataBuffer *atomic.Buffer, counterId int32, value string) (int32, error) {
	if err := validateMetaDataCounterId(metaDataBuffer, counterId); err != nil {
		return 0, err
	}
	offset := MetaDataOffset(counterId)
	if state := metaDataBuffer.GetInt32Volatile(offset); state != RecordAllocated {
		return 0, fmt.Errorf("counter id %d is not allocated, state: %d", counterId, state)
	}
	existing := metaDataBuffer.GetInt32(offset + LabelOffset)
	remaining := MaxLabelLength - existing
	n := int32(len(value))
	if n > remaining {
		n = remaining
	}
	if n > 0 {
		b := []byte(value[:n])
		metaDataBuffer.PutBytesArray(offset+LabelOffset+util.SizeOfInt32+existing, &b, 0, n)
		metaDataBuffer.PutInt32Ordered(offset+LabelOffset, existing+n)
	}
	return n, nil
}

// SetReferenceId sets the reference id of a counter (AeronCounters.setReferenceId).
func SetReferenceId(metaDataBuffer, valuesBuffer *atomic.Buffer, counterId int32, referenceId int64) error {
	if err := validateMetaDataCounterId(metaDataBuffer, counterId); err != nil {
		return err
	}
	valuesBuffer.PutInt64Ordered(CounterOffset(counterId)+ReferenceIdOffset, referenceId)
	return nil
}

func validateMetaDataCounterId(metaDataBuffer *atomic.Buffer, counterId int32) error {
	if counterId < 0 {
		return fmt.Errorf("counter id %d is negative", counterId)
	}
	maxCounterId := metaDataBuffer.Capacity()/MetadataLength - 1
	if counterId > maxCounterId {
		return fmt.Errorf("counter id %d out of range: 0 - maxCounterId=%d", counterId, maxCounterId)
	}
	return nil
}
