package rb

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron/util"
)

// Layout of the ring buffer trailer, identical to Agrona's RingBufferDescriptor: each counter sits in its
// own pair of cache lines to avoid false sharing.
const (
	TailPositionOffset       = util.CacheLineLength * 2
	HeadCachePositionOffset  = util.CacheLineLength * 4
	HeadPositionOffset       = util.CacheLineLength * 6
	CorrelationCounterOffset = util.CacheLineLength * 8
	ConsumerHeartbeatOffset  = util.CacheLineLength * 10
	TrailerLength            = util.CacheLineLength * 12
)

var descriptor = struct {
	tailPositionOffset       int32
	headCachePositionOffset  int32
	headPositionOffset       int32
	correlationCounterOffset int32
	consumerHeartbeatOffset  int32
	trailerLength            int32
}{
	TailPositionOffset,
	HeadCachePositionOffset,
	HeadPositionOffset,
	CorrelationCounterOffset,
	ConsumerHeartbeatOffset,
	TrailerLength,
}

// CheckCapacity validates that capacity is a power of two plus TrailerLength and at least
// minCapacity+TrailerLength, returning the data capacity.
func CheckCapacity(capacity, minCapacity int32) (int32, error) {
	dataCapacity := capacity - TrailerLength
	if !util.IsPowerOfTwo(int64(dataCapacity)) {
		return 0, fmt.Errorf("capacity must be a positive power of 2 + TRAILER_LENGTH: capacity=%d", capacity)
	}
	if dataCapacity < minCapacity {
		return 0, fmt.Errorf("insufficient capacity: minCapacity=%d, capacity=%d", minCapacity+TrailerLength, capacity)
	}
	return dataCapacity, nil
}
