// Generated SBE (Simple Binary Encoding) message codec

package codecs

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
)

type Ping struct {
	ControlSessionId int64
}

func (p *Ping) Encode(_m *SbeGoMarshaller, _w io.Writer, doRangeCheck bool) error {
	if doRangeCheck {
		if err := p.RangeCheck(p.SbeSchemaVersion(), p.SbeSchemaVersion()); err != nil {
			return err
		}
	}
	if err := _m.WriteInt64(_w, p.ControlSessionId); err != nil {
		return err
	}
	return nil
}

func (p *Ping) Decode(_m *SbeGoMarshaller, _r io.Reader, actingVersion uint16, blockLength uint16, doRangeCheck bool) error {
	if !p.ControlSessionIdInActingVersion(actingVersion) {
		p.ControlSessionId = p.ControlSessionIdNullValue()
	} else {
		if err := _m.ReadInt64(_r, &p.ControlSessionId); err != nil {
			return err
		}
	}
	if actingVersion > p.SbeSchemaVersion() && blockLength > p.SbeBlockLength() {
		io.CopyN(ioutil.Discard, _r, int64(blockLength-p.SbeBlockLength()))
	}
	if doRangeCheck {
		if err := p.RangeCheck(actingVersion, p.SbeSchemaVersion()); err != nil {
			return err
		}
	}
	return nil
}

func (p *Ping) RangeCheck(actingVersion uint16, schemaVersion uint16) error {
	if p.ControlSessionIdInActingVersion(actingVersion) {
		if p.ControlSessionId < p.ControlSessionIdMinValue() || p.ControlSessionId > p.ControlSessionIdMaxValue() {
			return fmt.Errorf("Range check failed on p.ControlSessionId (%v < %v > %v)", p.ControlSessionIdMinValue(), p.ControlSessionId, p.ControlSessionIdMaxValue())
		}
	}
	return nil
}

func PingInit(p *Ping) {
	return
}

func (*Ping) SbeBlockLength() (blockLength uint16) {
	return 8
}

func (*Ping) SbeTemplateId() (templateId uint16) {
	return 106
}

func (*Ping) SbeSchemaId() (schemaId uint16) {
	return 101
}

func (*Ping) SbeSchemaVersion() (schemaVersion uint16) {
	return 14
}

func (*Ping) SbeSemanticType() (semanticType []byte) {
	return []byte("")
}

func (*Ping) SbeSemanticVersion() (semanticVersion string) {
	return "5.2"
}

func (*Ping) ControlSessionIdId() uint16 {
	return 1
}

func (*Ping) ControlSessionIdSinceVersion() uint16 {
	return 0
}

func (p *Ping) ControlSessionIdInActingVersion(actingVersion uint16) bool {
	return actingVersion >= p.ControlSessionIdSinceVersion()
}

func (*Ping) ControlSessionIdDeprecated() uint16 {
	return 0
}

func (*Ping) ControlSessionIdMetaAttribute(meta int) string {
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

func (*Ping) ControlSessionIdMinValue() int64 {
	return math.MinInt64 + 1
}

func (*Ping) ControlSessionIdMaxValue() int64 {
	return math.MaxInt64
}

func (*Ping) ControlSessionIdNullValue() int64 {
	return math.MinInt64
}
