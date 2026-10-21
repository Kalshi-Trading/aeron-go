package archive

import (
	"errors"
	"strings"
	"testing"

	"github.com/lirm/aeron-go/aeron"
)

func TestReplayParamsDefaults(t *testing.T) {
	p := NewReplayParams()
	if p.Position != NullPosition || p.Length != ReplayAllAndFollow || p.BoundingLimitCounterID != -1 ||
		p.FileIoMaxLength != -1 || p.ReplayToken != NullValue || p.SubscriptionRegistrationID != NullValue {
		t.Fatalf("unexpected defaults %s", p)
	}
	if p.IsBounded() {
		t.Fatal("default params should not be bounded")
	}
	p.BoundingLimitCounterID = 7
	p.Position = 10
	if !p.IsBounded() {
		t.Fatal("expected bounded")
	}
	p.Reset()
	if p.IsBounded() || p.Position != NullPosition {
		t.Fatalf("reset failed %s", p)
	}
}

func TestIsResponseChannel(t *testing.T) {
	cases := map[string]bool{
		"aeron:udp?control-mode=response|control=localhost:10002": true,
		"aeron:ipc?control-mode=response":                         true,
		"aeron:udp?endpoint=localhost:20121":                      false,
		"aeron:udp?control-mode=dynamic|control=localhost:8030":   false,
		"not a channel": false,
	}
	for channel, expected := range cases {
		if IsResponseChannel(channel) != expected {
			t.Errorf("IsResponseChannel(%q) != %v", channel, expected)
		}
	}
}

func TestBuildReplayRequestChannel(t *testing.T) {
	channel, err := buildReplayRequestChannel("aeron:udp?endpoint=localhost:8010|session-id=5|term-id=1|init-term-id=1|term-offset=0|term-length=1048576|mtu=1408", 1234)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := aeron.ParseChannelUri(channel)
	if err != nil {
		t.Fatal(err)
	}
	for _, removed := range []string{"session-id", "term-id", "init-term-id", "term-offset"} {
		if uri.Get(removed) != "" {
			t.Errorf("%s should be removed from %s", removed, channel)
		}
	}
	expected := map[string]string{
		"endpoint":                "localhost:8010",
		"mtu":                     "1408",
		"response-correlation-id": "1234",
		"term-length":             "65536",
		"ssc":                     "false",
	}
	for k, v := range expected {
		if uri.Get(k) != v {
			t.Errorf("%s=%q, expected %q in %s", k, uri.Get(k), v, channel)
		}
	}
	if !strings.HasPrefix(channel, "aeron:udp?") {
		t.Errorf("unexpected channel %s", channel)
	}
}

func TestArchiveError(t *testing.T) {
	var err error = &ArchiveError{CorrelationID: 3, ErrorCode: ErrorCodeUnknownSubscription, Message: "no recording subscription"}
	var archiveErr *ArchiveError
	if !errors.As(err, &archiveErr) || archiveErr.ErrorCode != ErrorCodeUnknownSubscription {
		t.Fatal("errors.As failed")
	}
	if err.Error() != "response for correlationId=3, errorCode=4, error: no recording subscription" {
		t.Fatalf("unexpected message %q", err.Error())
	}
}
