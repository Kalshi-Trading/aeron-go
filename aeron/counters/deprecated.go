// Deprecated aliases kept for one release after the Id -> ID rename; remove in the next breaking release.

package counters

import "github.com/lirm/aeron-go/aeron/atomic"

// Deprecated: use TypeID.
func (ac *AtomicCounter) TypeId() int32 {
	return ac.TypeID()
}

// Deprecated: use MaxCounterID.
func (reader *Reader) MaxCounterId() int32 {
	return reader.MaxCounterID()
}

// Deprecated: use GetCounterRegistrationID.
func (reader *Reader) GetCounterRegistrationId(counterId int32) int64 {
	return reader.GetCounterRegistrationID(counterId)
}

// Deprecated: use GetCounterOwnerID.
func (reader *Reader) GetCounterOwnerId(counterId int32) int64 {
	return reader.GetCounterOwnerID(counterId)
}

// Deprecated: use GetCounterTypeID.
func (reader *Reader) GetCounterTypeId(counterId int32) int32 {
	return reader.GetCounterTypeID(counterId)
}

// Deprecated: use ValidateCounterID.
func (reader *Reader) ValidateCounterId(counterId int32) error {
	return reader.ValidateCounterID(counterId)
}

// Deprecated: use FindByRegistrationID.
func (reader *Reader) FindByRegistrationId(registrationId int64) int32 {
	return reader.FindByRegistrationID(registrationId)
}

// Deprecated: use FindByTypeIDAndRegistrationID.
func (reader *Reader) FindByTypeIdAndRegistrationId(typeId int32, registrationId int64) int32 {
	return reader.FindByTypeIDAndRegistrationID(typeId, registrationId)
}

// Deprecated: use GetCounterReferenceID.
func (reader *Reader) GetCounterReferenceId(counterId int32) int64 {
	return reader.GetCounterReferenceID(counterId)
}

// Deprecated: use FindHeartbeatCounterIDByRegistrationID.
func FindHeartbeatCounterIdByRegistrationId(reader *Reader, counterTypeId int32, registrationId int64) int32 {
	return FindHeartbeatCounterIDByRegistrationID(reader, counterTypeId, registrationId)
}

// Deprecated: use ValidateCounterTypeID.
func ValidateCounterTypeId(reader *Reader, counterId int32, expectedTypeId int32) error {
	return ValidateCounterTypeID(reader, counterId, expectedTypeId)
}

// Deprecated: use SetReferenceID.
func SetReferenceId(metaDataBuffer, valuesBuffer *atomic.Buffer, counterId int32, referenceId int64) error {
	return SetReferenceID(metaDataBuffer, valuesBuffer, counterId, referenceId)
}

// Deprecated: use ID.
func (ac *AtomicCounter) Id() int32 {
	return ac.ID()
}
