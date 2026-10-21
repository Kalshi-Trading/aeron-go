package util

import "math/bits"

// Constants and helpers mirroring org.agrona.BitUtil.
const (
	SizeOfByte   int32 = 1
	SizeOfBool   int32 = 1
	SizeOfChar   int32 = 2
	SizeOfShort  int32 = 2
	SizeOfFloat  int32 = 4
	SizeOfDouble int32 = 8
)

// AlignInt64 rounds value up to the next multiple of alignment, which must be a power of two.
func AlignInt64(value, alignment int64) int64 {
	return (value + (alignment - 1)) & ^(alignment - 1)
}

// IsAligned reports whether address is a multiple of alignment, which must be a power of two.
func IsAligned(address, alignment int64) bool {
	return address&(alignment-1) == 0
}

// IsPowerOfTwoInt32 checks that value is a positive power of two.
func IsPowerOfTwoInt32(value int32) bool {
	return value > 0 && value&(value-1) == 0
}

// FindNextPositivePowerOfTwo returns the smallest power of two >= value (1 for value <= 1).
func FindNextPositivePowerOfTwo(value int32) int32 {
	if value <= 1 {
		return 1
	}
	return int32(1) << (32 - bits.LeadingZeros32(uint32(value-1)))
}

// FindNextPositivePowerOfTwo64 returns the smallest power of two >= value (1 for value <= 1).
func FindNextPositivePowerOfTwo64(value int64) int64 {
	if value <= 1 {
		return 1
	}
	return int64(1) << (64 - bits.LeadingZeros64(uint64(value-1)))
}

// IsEven reports whether value is even.
func IsEven(value int32) bool {
	return value&1 == 0
}
