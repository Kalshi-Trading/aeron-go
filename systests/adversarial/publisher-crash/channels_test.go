package publishercrash

import "fmt"

type transport string

const (
	ipc        transport = "ipc"
	udp        transport = "udp"
	multicast  transport = "multicast"
	mdcManual  transport = "mdc-manual"
	mdcDynamic transport = "mdc-dynamic"
	mds        transport = "multi-destination-subscription"
	spy        transport = "spy"
)

// channels are the publication and subscription side of a transport. port is the first of the two UDP ports the
// transport may use. term-length=1m keeps every log small and lets max-length messages (128 KiB) fragment.
type channels struct {
	pub, sub           string
	pubDests, subDests []string
}

func channelsFor(tr transport, port int) channels {
	endpoint := fmt.Sprintf("localhost:%d", port)
	switch tr {
	case ipc:
		return channels{pub: "aeron:ipc?term-length=1m", sub: "aeron:ipc"}
	case udp:
		return channels{pub: "aeron:udp?endpoint=" + endpoint + "|term-length=1m", sub: "aeron:udp?endpoint=" + endpoint}
	case multicast:
		group := fmt.Sprintf("224.0.1.1:%d|interface=127.0.0.1", port)
		return channels{pub: "aeron:udp?endpoint=" + group + "|term-length=1m", sub: "aeron:udp?endpoint=" + group}
	case mdcManual:
		return channels{pub: "aeron:udp?control-mode=manual|term-length=1m", sub: "aeron:udp?endpoint=" + endpoint,
			pubDests: []string{"aeron:udp?endpoint=" + endpoint}}
	case mdcDynamic:
		control := fmt.Sprintf("localhost:%d", port+1)
		return channels{pub: "aeron:udp?control=" + control + "|control-mode=dynamic|term-length=1m",
			sub: "aeron:udp?endpoint=" + endpoint + "|control=" + control + "|control-mode=dynamic"}
	case mds:
		return channels{pub: "aeron:udp?endpoint=" + endpoint + "|term-length=1m", sub: "aeron:udp?control-mode=manual",
			subDests: []string{"aeron:udp?endpoint=" + endpoint}}
	case spy:
		// ssc: the spy alone connects the publication.
		return channels{pub: "aeron:udp?endpoint=" + endpoint + "|term-length=1m|ssc=true",
			sub: "aeron-spy:aeron:udp?endpoint=" + endpoint}
	}
	panic(tr)
}

// pubArgs are the peer arguments of a publisher on ch.
func pubArgs(ch channels, stream int32, extra ...string) []string {
	args := []string{"channel=" + ch.pub, fmt.Sprintf("stream=%d", stream)}
	for _, d := range ch.pubDests {
		args = append(args, "dest="+d)
	}
	return append(args, extra...)
}

// subPeerArgs are the peer arguments of a subscriber on ch.
func subPeerArgs(ch channels, stream int32) []string {
	args := []string{"channel=" + ch.sub, fmt.Sprintf("stream=%d", stream)}
	for _, d := range ch.subDests {
		args = append(args, "dest="+d)
	}
	return args
}
