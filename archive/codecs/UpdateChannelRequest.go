// Generated SBE (Simple Binary Encoding) message codec

package codecs

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
)

type UpdateChannelRequest struct {
	ControlSessionId int64
	CorrelationId    int64
	RecordingId      int64
	Channel          []uint8
}

func (u *UpdateChannelRequest) Encode(_m *SbeGoMarshaller, _w io.Writer, doRangeCheck bool) error {
	if doRangeCheck {
		if err := u.RangeCheck(u.SbeSchemaVersion(), u.SbeSchemaVersion()); err != nil {
			return err
		}
	}
	if err := _m.WriteInt64(_w, u.ControlSessionId); err != nil {
		return err
	}
	if err := _m.WriteInt64(_w, u.CorrelationId); err != nil {
		return err
	}
	if err := _m.WriteInt64(_w, u.RecordingId); err != nil {
		return err
	}
	if err := _m.WriteUint32(_w, uint32(len(u.Channel))); err != nil {
		return err
	}
	if err := _m.WriteBytes(_w, u.Channel); err != nil {
		return err
	}
	return nil
}

func (u *UpdateChannelRequest) Decode(_m *SbeGoMarshaller, _r io.Reader, actingVersion uint16, blockLength uint16, doRangeCheck bool) error {
	if !u.ControlSessionIdInActingVersion(actingVersion) {
		u.ControlSessionId = u.ControlSessionIdNullValue()
	} else {
		if err := _m.ReadInt64(_r, &u.ControlSessionId); err != nil {
			return err
		}
	}
	if !u.CorrelationIdInActingVersion(actingVersion) {
		u.CorrelationId = u.CorrelationIdNullValue()
	} else {
		if err := _m.ReadInt64(_r, &u.CorrelationId); err != nil {
			return err
		}
	}
	if !u.RecordingIdInActingVersion(actingVersion) {
		u.RecordingId = u.RecordingIdNullValue()
	} else {
		if err := _m.ReadInt64(_r, &u.RecordingId); err != nil {
			return err
		}
	}
	if actingVersion > u.SbeSchemaVersion() && blockLength > u.SbeBlockLength() {
		io.CopyN(ioutil.Discard, _r, int64(blockLength-u.SbeBlockLength()))
	}

	if u.ChannelInActingVersion(actingVersion) {
		var ChannelLength uint32
		if err := _m.ReadUint32(_r, &ChannelLength); err != nil {
			return err
		}
		if cap(u.Channel) < int(ChannelLength) {
			u.Channel = make([]uint8, ChannelLength)
		}
		u.Channel = u.Channel[:ChannelLength]
		if err := _m.ReadBytes(_r, u.Channel); err != nil {
			return err
		}
	}
	if doRangeCheck {
		if err := u.RangeCheck(actingVersion, u.SbeSchemaVersion()); err != nil {
			return err
		}
	}
	return nil
}

func (u *UpdateChannelRequest) RangeCheck(actingVersion uint16, schemaVersion uint16) error {
	if u.ControlSessionIdInActingVersion(actingVersion) {
		if u.ControlSessionId < u.ControlSessionIdMinValue() || u.ControlSessionId > u.ControlSessionIdMaxValue() {
			return fmt.Errorf("Range check failed on u.ControlSessionId (%v < %v > %v)", u.ControlSessionIdMinValue(), u.ControlSessionId, u.ControlSessionIdMaxValue())
		}
	}
	if u.CorrelationIdInActingVersion(actingVersion) {
		if u.CorrelationId < u.CorrelationIdMinValue() || u.CorrelationId > u.CorrelationIdMaxValue() {
			return fmt.Errorf("Range check failed on u.CorrelationId (%v < %v > %v)", u.CorrelationIdMinValue(), u.CorrelationId, u.CorrelationIdMaxValue())
		}
	}
	if u.RecordingIdInActingVersion(actingVersion) {
		if u.RecordingId < u.RecordingIdMinValue() || u.RecordingId > u.RecordingIdMaxValue() {
			return fmt.Errorf("Range check failed on u.RecordingId (%v < %v > %v)", u.RecordingIdMinValue(), u.RecordingId, u.RecordingIdMaxValue())
		}
	}
	for idx, ch := range u.Channel {
		if ch > 127 {
			return fmt.Errorf("u.Channel[%d]=%d failed ASCII validation", idx, ch)
		}
	}
	return nil
}

func UpdateChannelRequestInit(u *UpdateChannelRequest) {
	return
}

func (*UpdateChannelRequest) SbeBlockLength() (blockLength uint16) {
	return 24
}

func (*UpdateChannelRequest) SbeTemplateId() (templateId uint16) {
	return 107
}

func (*UpdateChannelRequest) SbeSchemaId() (schemaId uint16) {
	return 101
}

func (*UpdateChannelRequest) SbeSchemaVersion() (schemaVersion uint16) {
	return 14
}

func (*UpdateChannelRequest) SbeSemanticType() (semanticType []byte) {
	return []byte("")
}

func (*UpdateChannelRequest) SbeSemanticVersion() (semanticVersion string) {
	return "5.2"
}

func (*UpdateChannelRequest) ControlSessionIdId() uint16 {
	return 1
}

func (*UpdateChannelRequest) ControlSessionIdSinceVersion() uint16 {
	return 0
}

func (u *UpdateChannelRequest) ControlSessionIdInActingVersion(actingVersion uint16) bool {
	return actingVersion >= u.ControlSessionIdSinceVersion()
}

func (*UpdateChannelRequest) ControlSessionIdDeprecated() uint16 {
	return 0
}

func (*UpdateChannelRequest) ControlSessionIdMetaAttribute(meta int) string {
	switch meta {
	case 1:
		return ""
	case 2:
		return ""
	case 3:
		return ""
	case 4:
		return "required"
	}
	return ""
}

func (*UpdateChannelRequest) ControlSessionIdMinValue() int64 {
	return math.MinInt64 + 1
}

func (*UpdateChannelRequest) ControlSessionIdMaxValue() int64 {
	return math.MaxInt64
}

func (*UpdateChannelRequest) ControlSessionIdNullValue() int64 {
	return math.MinInt64
}

func (*UpdateChannelRequest) CorrelationIdId() uint16 {
	return 2
}

func (*UpdateChannelRequest) CorrelationIdSinceVersion() uint16 {
	return 0
}

func (u *UpdateChannelRequest) CorrelationIdInActingVersion(actingVersion uint16) bool {
	return actingVersion >= u.CorrelationIdSinceVersion()
}

func (*UpdateChannelRequest) CorrelationIdDeprecated() uint16 {
	return 0
}

func (*UpdateChannelRequest) CorrelationIdMetaAttribute(meta int) string {
	switch meta {
	case 1:
		return ""
	case 2:
		return ""
	case 3:
		return ""
	case 4:
		return "required"
	}
	return ""
}

func (*UpdateChannelRequest) CorrelationIdMinValue() int64 {
	return math.MinInt64 + 1
}

func (*UpdateChannelRequest) CorrelationIdMaxValue() int64 {
	return math.MaxInt64
}

func (*UpdateChannelRequest) CorrelationIdNullValue() int64 {
	return math.MinInt64
}

func (*UpdateChannelRequest) RecordingIdId() uint16 {
	return 3
}

func (*UpdateChannelRequest) RecordingIdSinceVersion() uint16 {
	return 0
}

func (u *UpdateChannelRequest) RecordingIdInActingVersion(actingVersion uint16) bool {
	return actingVersion >= u.RecordingIdSinceVersion()
}

func (*UpdateChannelRequest) RecordingIdDeprecated() uint16 {
	return 0
}

func (*UpdateChannelRequest) RecordingIdMetaAttribute(meta int) string {
	switch meta {
	case 1:
		return ""
	case 2:
		return ""
	case 3:
		return ""
	case 4:
		return "required"
	}
	return ""
}

func (*UpdateChannelRequest) RecordingIdMinValue() int64 {
	return math.MinInt64 + 1
}

func (*UpdateChannelRequest) RecordingIdMaxValue() int64 {
	return math.MaxInt64
}

func (*UpdateChannelRequest) RecordingIdNullValue() int64 {
	return math.MinInt64
}

func (*UpdateChannelRequest) ChannelMetaAttribute(meta int) string {
	switch meta {
	case 1:
		return ""
	case 2:
		return ""
	case 3:
		return ""
	case 4:
		return "required"
	}
	return ""
}

func (*UpdateChannelRequest) ChannelSinceVersion() uint16 {
	return 0
}

func (u *UpdateChannelRequest) ChannelInActingVersion(actingVersion uint16) bool {
	return actingVersion >= u.ChannelSinceVersion()
}

func (*UpdateChannelRequest) ChannelDeprecated() uint16 {
	return 0
}

func (UpdateChannelRequest) ChannelCharacterEncoding() string {
	return "US-ASCII"
}

func (UpdateChannelRequest) ChannelHeaderLength() uint64 {
	return 4
}
