package cnc

import (
	"bytes"
	"strings"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/util"
)

func putString(buf *atomic.Buffer, offset int32, s string) {
	b := []byte(s)
	buf.PutInt32(offset, int32(len(b)))
	buf.PutBytesArray(offset+4, &b, 0, int32(len(b)))
}

func TestReadErrors(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, 1024))
	offset := int32(0)
	for i, msg := range []string{"java.lang.IllegalStateException: one", "two"} {
		b := []byte(msg)
		length := errorEncodedErrorOffset + int32(len(b))
		buf.PutInt32(offset+errorObservationCountOffset, int32(i+1))
		buf.PutInt64(offset+errorLastObservationOffset, int64(2000+i))
		buf.PutInt64(offset+errorFirstObservationOffset, int64(1000+i))
		buf.PutBytesArray(offset+errorEncodedErrorOffset, &b, 0, int32(len(b)))
		buf.PutInt32(offset+errorLengthOffset, length)
		offset += util.AlignInt32(length, errorRecordAlignment)
	}
	var got []ErrorObservation
	if n := ReadErrors(buf, 0, func(o ErrorObservation) { got = append(got, o) }); n != 2 {
		t.Fatalf("got %d errors", n)
	}
	if got[0].EncodedError != "java.lang.IllegalStateException: one" || got[1].ObservationCount != 2 ||
		got[1].FirstObservationMs != 1001 || got[1].LastObservationMs != 2001 {
		t.Fatalf("unexpected %+v", got)
	}
	if n := ReadErrors(buf, 2001, func(ErrorObservation) {}); n != 1 {
		t.Fatalf("since filter: got %d", n)
	}
	if n := ReadErrors(atomic.MakeBuffer(make([]byte, 64)), 0, func(ErrorObservation) {}); n != 0 {
		t.Fatalf("empty: got %d", n)
	}
}

func TestReadLossReport(t *testing.T) {
	buf := atomic.MakeBuffer(make([]byte, 4096))
	offset := int32(0)
	for i, ch := range []string{"aeron:udp?endpoint=localhost:20121", "aeron:udp?endpoint=x:1"} {
		buf.PutInt64(offset+lossTotalBytesLostOffset, int64(100*(i+1)))
		buf.PutInt64(offset+lossFirstObservationOffset, 10)
		buf.PutInt64(offset+lossLastObservationOffset, 20)
		buf.PutInt32(offset+lossSessionIDOffset, int32(7+i))
		buf.PutInt32(offset+lossStreamIDOffset, 1001)
		putString(buf, offset+lossChannelOffset, ch)
		src := offset + lossChannelOffset + util.AlignInt32(4+int32(len(ch)), 4)
		putString(buf, src, "127.0.0.1:5555")
		buf.PutInt64(offset+lossObservationCountOffset, int64(i+3))
		offset += util.AlignInt32(src+4+14-offset, lossEntryAlignment)
	}
	var got []LossEntry
	if n := ReadLossReport(buf, func(e LossEntry) { got = append(got, e) }); n != 2 {
		t.Fatalf("got %d entries", n)
	}
	want := LossEntry{4, 200, 10, 20, 8, 1001, "aeron:udp?endpoint=x:1", "127.0.0.1:5555"}
	if got[1] != want || got[0].Channel != "aeron:udp?endpoint=localhost:20121" {
		t.Fatalf("unexpected %+v", got)
	}
}

func TestFilter(t *testing.T) {
	f, delay, watch, err := ParseFilterArgs([]string{"type=1|4", "stream=1001", "channel=.*20121.*", "delay=2", "watch=false"})
	if err != nil || delay != 2 || watch {
		t.Fatalf("parse: %v %d %v", err, delay, watch)
	}
	match := CounterInfo{TypeID: 4, StreamKey: true, StreamID: 1001, Channel: "aeron:udp?endpoint=localhost:20121"}
	if !f.Match(match) {
		t.Fatal("expected match")
	}
	for _, c := range []CounterInfo{
		{TypeID: 0, Label: "Bytes sent"},
		{TypeID: 4, StreamKey: true, StreamID: 1002, Channel: match.Channel},
		{TypeID: 4, StreamKey: true, StreamID: 1001, Channel: "aeron:ipc"},
	} {
		if f.Match(c) {
			t.Fatalf("unexpected match %+v", c)
		}
	}
	if !(Filter{}).Match(CounterInfo{TypeID: 0}) {
		t.Fatal("empty filter must match everything")
	}
	for _, bad := range []string{"nonsense", "colour=red", "type=("} {
		if _, _, _, err := ParseFilterArgs([]string{bad}); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	}
}

func TestBacklog(t *testing.T) {
	ch := "aeron:udp?endpoint=localhost:20121"
	sc := func(typeID int32, reg, value int64) CounterInfo {
		return CounterInfo{TypeID: typeID, Value: value, StreamKey: true, RegistrationID: reg, SessionID: 5, StreamID: 1001, Channel: ch}
	}
	streams := Backlog([]CounterInfo{
		sc(PublisherLimitTypeID, 1, 1000), sc(PublisherPositionTypeID, 1, 600),
		sc(SenderPositionTypeID, 1, 500), sc(SenderLimitTypeID, 1, 900),
		sc(ReceiverHwmTypeID, 2, 550), sc(ReceiverPositionTypeID, 2, 540),
		sc(SubscriberPositionTypeID, 3, 520), sc(SubscriberPositionTypeID, 4, 550),
		{TypeID: SystemCounterTypeID, Label: "Bytes sent"},
	})
	if len(streams) != 1 {
		t.Fatalf("got %d streams", len(streams))
	}
	b := streams[StreamKey{5, 1001, ch}]
	if b.Publisher.RemainingWindow() != 400 || b.Sender.Backlog(b.Publisher.Position) != 100 ||
		b.Sender.Window() != 400 || b.Receiver.HighWaterMark != 550 || len(b.Subscribers) != 2 {
		t.Fatalf("unexpected backlog %+v %+v %+v %+v", b.Publisher, b.Sender, b.Receiver, b.Subscribers)
	}
	var out bytes.Buffer
	PrintBacklog(&out, streams)
	for _, want := range []string{
		"sessionId=5 streamId=1001 channel=" + ch,
		"┌─for publisher 1 the last sampled position is 600 (~400 bytes before back-pressure)",
		"└─sender 1 has to send 100 bytes (400 bytes remaining in the sender window)",
		"┌─receiver 2 is at position 540",
		"├─subscriber 3 has 30 backlog bytes",
		"└─subscriber 4 has 0 backlog bytes",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q in\n%s", want, out.String())
		}
	}
}

func TestSemanticVersionString(t *testing.T) {
	if s := SemanticVersionString(512); s != "0.2.0" {
		t.Fatal(s)
	}
}
