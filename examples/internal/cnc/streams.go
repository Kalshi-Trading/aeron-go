package cnc

import (
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"

	"github.com/lirm/aeron-go/aeron/counters"
)

// Driver counter type ids (io.aeron.AeronCounters).
const (
	SystemCounterTypeID        = 0
	PublisherLimitTypeID       = 1
	SenderPositionTypeID       = 2
	ReceiverHwmTypeID          = 3
	SubscriberPositionTypeID   = 4
	ReceiverPositionTypeID     = 5
	SendChannelStatusTypeID    = 6
	ReceiveChannelStatusTypeID = 7
	SenderLimitTypeID          = 9
	PerImageTypeID             = 10
	ClientHeartbeatTypeID      = 11
	PublisherPositionTypeID    = 12
	SenderBpeTypeID            = 13
	LocalSocketAddressTypeID   = 14
	SenderNaksReceivedTypeID   = 19
	ReceiverNaksSentTypeID     = 20
	ArchiveRecordingPosTypeID  = 100
)

// Stream counter key layout (io.aeron.driver.status.StreamCounter).
const (
	streamKeyRegistrationIDOffset = 0
	streamKeySessionIDOffset      = 8
	streamKeyStreamIDOffset       = 12
	streamKeyChannelOffset        = 16
)

// IsStreamCounter reports whether a counter type has a StreamCounter key.
func IsStreamCounter(typeID int32) bool {
	switch typeID {
	case PublisherLimitTypeID, SenderPositionTypeID, ReceiverHwmTypeID, SubscriberPositionTypeID,
		ReceiverPositionTypeID, SenderLimitTypeID, PerImageTypeID, PublisherPositionTypeID, SenderBpeTypeID,
		SenderNaksReceivedTypeID, ReceiverNaksSentTypeID:
		return true
	}
	return false
}

// CounterInfo is a decoded counter (value, label and, for stream counters, the key fields).
type CounterInfo struct {
	ID             int32
	TypeID         int32
	Value          int64
	Label          string
	StreamKey      bool
	RegistrationID int64
	SessionID      int32
	StreamID       int32
	Channel        string
}

// Snapshot reads every allocated counter.
func Snapshot(reader *counters.Reader) []CounterInfo {
	var out []CounterInfo
	reader.Scan(func(c counters.Counter) {
		info := CounterInfo{ID: c.Id, TypeID: c.TypeId, Value: c.Value, Label: c.Label}
		switch {
		case IsStreamCounter(c.TypeId):
			info.StreamKey = true
			info.RegistrationID, _ = reader.GetKeyPartInt64(c.Id, streamKeyRegistrationIDOffset)
			info.SessionID, _ = reader.GetKeyPartInt32(c.Id, streamKeySessionIDOffset)
			info.StreamID, _ = reader.GetKeyPartInt32(c.Id, streamKeyStreamIDOffset)
			info.Channel, _ = reader.GetKeyPartString(c.Id, streamKeyChannelOffset)
		case c.TypeId == SendChannelStatusTypeID || c.TypeId == ReceiveChannelStatusTypeID ||
			c.TypeId == LocalSocketAddressTypeID:
			info.Channel, _ = reader.GetKeyPartString(c.Id, 0)
		}
		out = append(out, info)
	})
	return out
}

// Filter mirrors AeronStat's CounterFilter: each non-nil pattern must match.
type Filter struct {
	Type, Identity, Session, Stream, Channel *regexp.Regexp
}

// ParseFilterArgs parses Java-style name=pattern args (type, identity, session, stream, channel)
// plus delay=<seconds> and watch=<bool>.
func ParseFilterArgs(args []string) (f Filter, delaySeconds int, watch bool, err error) {
	delaySeconds, watch = 1, true
	for _, arg := range args {
		name, value, ok := cutEq(arg)
		if !ok {
			return f, 0, false, fmt.Errorf("arguments must be in name=pattern format: invalid '%s'", arg)
		}
		var target **regexp.Regexp
		switch name {
		case "delay":
			if delaySeconds, err = strconv.Atoi(value); err != nil {
				return f, 0, false, err
			}
			continue
		case "watch":
			if watch, err = strconv.ParseBool(value); err != nil {
				return f, 0, false, err
			}
			continue
		case "type":
			target = &f.Type
		case "identity":
			target = &f.Identity
		case "session":
			target = &f.Session
		case "stream":
			target = &f.Stream
		case "channel":
			target = &f.Channel
		default:
			return f, 0, false, fmt.Errorf("unrecognised argument: '%s'", arg)
		}
		if *target, err = regexp.Compile("^(?:" + value + ")$"); err != nil {
			return f, 0, false, err
		}
	}
	return f, delaySeconds, watch, nil
}

func cutEq(s string) (string, string, bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == '=' {
			return s[:i], s[i+1:], true
		}
	}
	return "", "", false
}

// Match reports whether a counter passes the filter.
func (f Filter) Match(c CounterInfo) bool {
	if f.Type != nil && !f.Type.MatchString(strconv.Itoa(int(c.TypeID))) {
		return false
	}
	if f.Identity == nil && f.Session == nil && f.Stream == nil && f.Channel == nil {
		return true
	}
	if f.Channel != nil && !(c.Channel != "" && f.Channel.MatchString(c.Channel)) {
		return false
	}
	if f.Identity != nil || f.Session != nil || f.Stream != nil {
		if !c.StreamKey {
			return false
		}
		if f.Identity != nil && !f.Identity.MatchString(strconv.FormatInt(c.RegistrationID, 10)) {
			return false
		}
		if f.Session != nil && !f.Session.MatchString(strconv.Itoa(int(c.SessionID))) {
			return false
		}
		if f.Stream != nil && !f.Stream.MatchString(strconv.Itoa(int(c.StreamID))) {
			return false
		}
	}
	return true
}

// StreamKey identifies a stream (BacklogStat.StreamCompositeKey).
type StreamKey struct {
	SessionID int32
	StreamID  int32
	Channel   string
}

// PublisherBacklog is the publisher side of a stream.
type PublisherBacklog struct{ RegistrationID, Limit, Position int64 }

// RemainingWindow is the number of bytes before the publisher is back-pressured.
func (p PublisherBacklog) RemainingWindow() int64 { return p.Limit - p.Position }

// SenderBacklog is the sender side of a network stream.
type SenderBacklog struct{ RegistrationID, Limit, Position int64 }

// Backlog is the number of bytes the sender has still to send.
func (s SenderBacklog) Backlog(publisherPosition int64) int64 { return publisherPosition - s.Position }

// Window is the number of bytes remaining in the sender's flow control window.
func (s SenderBacklog) Window() int64 { return s.Limit - s.Position }

// ReceiverBacklog is the receiver side of a network stream.
type ReceiverBacklog struct{ RegistrationID, HighWaterMark, Position int64 }

// StreamBacklog groups the backlog of one stream.
type StreamBacklog struct {
	Publisher   *PublisherBacklog
	Sender      *SenderBacklog
	Receiver    *ReceiverBacklog
	Subscribers map[int64]int64
}

// Backlog builds the BacklogStat snapshot from counters.
func Backlog(infos []CounterInfo) map[StreamKey]*StreamBacklog {
	streams := map[StreamKey]*StreamBacklog{}
	for _, c := range infos {
		if !((c.TypeID >= PublisherLimitTypeID && c.TypeID <= ReceiverPositionTypeID) ||
			c.TypeID == SenderLimitTypeID || c.TypeID == PerImageTypeID || c.TypeID == PublisherPositionTypeID) {
			continue
		}
		key := StreamKey{c.SessionID, c.StreamID, c.Channel}
		b := streams[key]
		if b == nil {
			b = &StreamBacklog{Subscribers: map[int64]int64{}}
			streams[key] = b
		}
		pub := func() *PublisherBacklog {
			if b.Publisher == nil {
				b.Publisher = &PublisherBacklog{}
			}
			b.Publisher.RegistrationID = c.RegistrationID
			return b.Publisher
		}
		snd := func() *SenderBacklog {
			if b.Sender == nil {
				b.Sender = &SenderBacklog{}
			}
			b.Sender.RegistrationID = c.RegistrationID
			return b.Sender
		}
		rcv := func() *ReceiverBacklog {
			if b.Receiver == nil {
				b.Receiver = &ReceiverBacklog{}
			}
			b.Receiver.RegistrationID = c.RegistrationID
			return b.Receiver
		}
		switch c.TypeID {
		case PublisherLimitTypeID:
			pub().Limit = c.Value
		case PublisherPositionTypeID:
			pub().Position = c.Value
		case SenderPositionTypeID:
			snd().Position = c.Value
		case SenderLimitTypeID:
			snd().Limit = c.Value
		case ReceiverHwmTypeID:
			rcv().HighWaterMark = c.Value
		case ReceiverPositionTypeID:
			rcv().Position = c.Value
		case SubscriberPositionTypeID:
			b.Subscribers[c.RegistrationID] = c.Value
		}
	}
	return streams
}

// PrintBacklog writes the snapshot in BacklogStat.print format (streams sorted for stable output).
func PrintBacklog(w io.Writer, streams map[StreamKey]*StreamBacklog) {
	keys := make([]StreamKey, 0, len(streams))
	for k := range streams {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.StreamID != b.StreamID {
			return a.StreamID < b.StreamID
		}
		if a.SessionID != b.SessionID {
			return a.SessionID < b.SessionID
		}
		return a.Channel < b.Channel
	})
	for _, k := range keys {
		b := streams[k]
		fmt.Fprintf(w, "sessionId=%d streamId=%d channel=%s : ", k.SessionID, k.StreamID, k.Channel)
		if b.Publisher != nil {
			fmt.Fprintf(w, "\n┌─for publisher %d the last sampled position is %d (~%d bytes before back-pressure)",
				b.Publisher.RegistrationID, b.Publisher.Position, b.Publisher.RemainingWindow())
			if b.Sender != nil {
				fmt.Fprintf(w, "\n└─sender %d", b.Sender.RegistrationID)
				if backlog := b.Sender.Backlog(b.Publisher.Position); backlog > 0 {
					fmt.Fprintf(w, " has to send %d bytes", backlog)
				} else {
					fmt.Fprintf(w, " is at position %d", b.Sender.Position)
				}
				fmt.Fprintf(w, " (%d bytes remaining in the sender window)", b.Sender.Window())
			} else {
				fmt.Fprint(w, "\n└─no sender")
			}
		}
		if b.Receiver != nil {
			fmt.Fprintf(w, "\n┌─receiver %d is at position %d", b.Receiver.RegistrationID, b.Receiver.Position)
		}
		subs := make([]int64, 0, len(b.Subscribers))
		for id := range b.Subscribers {
			subs = append(subs, id)
		}
		sort.Slice(subs, func(i, j int) bool { return subs[i] < subs[j] })
		for i, id := range subs {
			prefix := "\n├"
			if i == len(subs)-1 {
				prefix = "\n└"
			}
			hwm := int64(0)
			if b.Receiver != nil {
				hwm = b.Receiver.HighWaterMark
			} else if b.Publisher != nil {
				hwm = b.Publisher.Position
			}
			fmt.Fprintf(w, "%s─subscriber %d has %d backlog bytes", prefix, id, hwm-b.Subscribers[id])
		}
		fmt.Fprint(w, "\n\n")
	}
}
