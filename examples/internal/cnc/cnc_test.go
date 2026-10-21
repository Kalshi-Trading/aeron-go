package cnc

import (
	"bytes"
	"strings"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
)

func putString(buf *atomic.Buffer, offset int32, s string) {
	b := []byte(s)
	buf.PutInt32(offset, int32(len(b)))
	buf.PutBytesArray(offset+4, &b, 0, int32(len(b)))
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
