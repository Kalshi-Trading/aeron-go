// Command advsvc is the Go clustered service run by systests/adversarial/cluster next to each Java
// ClusteredMediaDriver member. It applies KindData messages to advproto.Replicated, echoes every message to the
// sending session, snapshots the full state and writes it to -state-dir when it applies a KindDump message.
// Lifecycle events are printed to stdout as "EVENT ..." lines for the test harness.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/lirm/aeron-go/systests/adversarial/cluster/internal/advproto"
)

var closeReasons = map[codecs.CloseReasonEnum]string{
	codecs.CloseReason.CLIENT_ACTION:  "CLIENT_ACTION",
	codecs.CloseReason.SERVICE_ACTION: "SERVICE_ACTION",
	codecs.CloseReason.TIMEOUT:        "TIMEOUT",
}

type service struct {
	node     int32
	stateDir string
	cluster  cluster.Cluster
	state    *advproto.Replicated
	loaded   bool
	reply    *atomic.Buffer
	replyArr []byte
}

func event(format string, args ...interface{}) {
	fmt.Printf("EVENT "+format+"\n", args...)
}

func (s *service) OnStart(c cluster.Cluster, image aeron.Image) {
	s.cluster = c
	s.state = advproto.NewReplicated()
	if image != nil {
		var data []byte
		assembler := aeron.NewFragmentAssembler(func(buf *atomic.Buffer, offset, length int32, _ *logbuffer.Header) {
			data = buf.GetBytesArray(offset, length)
		}, 0)
		deadline := time.Now().Add(30 * time.Second)
		for data == nil {
			if image.Poll(assembler.OnFragment, 10) == 0 {
				if image.IsClosed() || time.Now().After(deadline) {
					panic("snapshot image closed before state was read")
				}
				c.IdleStrategy().Idle(0)
			}
		}
		if err := json.Unmarshal(data, s.state); err != nil {
			panic(err)
		}
		s.loaded = true
	}
	event("start node=%d snapshot=%v messages=%d", s.node, s.loaded, s.state.Messages)
}

func (s *service) OnSessionOpen(session cluster.ClientSession, timestamp int64) {
	s.state.Opens++
	s.state.Mix(1, session.Id(), timestamp)
	event("open session=%d", session.Id())
}

func (s *service) OnSessionClose(session cluster.ClientSession, timestamp int64, reason codecs.CloseReasonEnum) {
	name := closeReasons[reason]
	s.state.Closes[name]++
	s.state.ClosedSessions[session.Id()] = name
	s.state.Mix(2, session.Id(), timestamp, int64(reason))
	event("close session=%d reason=%s", session.Id(), name)
}

func (s *service) OnSessionMessage(session cluster.ClientSession, timestamp int64, buffer *atomic.Buffer,
	offset, length int32, _ *logbuffer.Header) {
	b := buffer.GetBytesArray(offset, length)
	kind, tag, seq, err := advproto.Decode(b)
	if err != nil {
		s.state.Corrupt++
		s.state.Mix(3, int64(length))
		event("corrupt err=%v", err)
		return
	}
	s.state.Mix(4, int64(kind), tag, seq, int64(length), timestamp)
	switch kind {
	case advproto.KindData:
		r := s.state.Applied[tag]
		if !r.Add(seq) {
			s.state.Dups++
			event("dup tag=%d seq=%d", tag, seq)
		}
		s.state.Applied[tag] = r
		s.state.Messages++
		s.state.Bytes += int64(length)
	case advproto.KindDump:
		d := &advproto.Dump{Node: s.node, DumpID: seq, LogPosition: s.cluster.LogPosition(), State: s.state,
			SnapshotLoaded: s.loaded}
		if err := advproto.WriteJSON(advproto.DumpPath(s.stateDir, s.node, seq), d); err != nil {
			event("dump-error err=%v", err)
		}
	}
	if session == nil {
		return
	}
	advproto.Encode(s.replyArr, kind, tag, seq, 0)
	for i := 0; i < 2000; i++ {
		r := session.Offer(s.reply, 0, advproto.HeaderLength, nil)
		if r > 0 || r == cluster.ClientSessionMockedOffer || r == aeron.NotConnected || r == aeron.PublicationClosed {
			return
		}
		s.cluster.IdleStrategy().Idle(0)
	}
}

func (s *service) OnTimerEvent(int64, int64) { s.state.Timers++ }

func (s *service) OnTakeSnapshot(publication *aeron.ExclusivePublication) {
	b, err := json.Marshal(s.state)
	if err != nil {
		panic(err)
	}
	buf := atomic.MakeBuffer(b)
	for {
		r := publication.Offer(buf, 0, int32(len(b)), nil)
		if r > 0 {
			break
		}
		if r == aeron.PublicationClosed || r == aeron.MaxPositionExceeded {
			panic(fmt.Sprintf("snapshot offer failed: %d", r))
		}
		s.cluster.IdleStrategy().Idle(0)
	}
	event("snapshot messages=%d pos=%d", s.state.Messages, s.cluster.LogPosition())
}

func (s *service) OnRoleChange(role cluster.Role) { event("role=%v", role) }

func (s *service) OnTerminate(cluster.Cluster) { event("terminate") }

func (s *service) OnNewLeadershipTermEvent(leadershipTermId, logPosition, _, _ int64, leaderMemberId, _ int32,
	_ codecs.ClusterTimeUnitEnum, _ int32) {
	event("term=%d leader=%d pos=%d", leadershipTermId, leaderMemberId, logPosition)
}

func main() {
	aeronDir := flag.String("aeron-dir", "", "aeron directory of the member's media driver")
	clusterDir := flag.String("cluster-dir", "", "service mark file directory")
	stateDir := flag.String("state-dir", "", "dump output directory")
	node := flag.Int("node", 0, "member id")
	flag.Parse()

	ctx := aeron.NewContext().AeronDir(*aeronDir)
	opts := cluster.NewOptions()
	opts.ClusterDir = *clusterDir
	replyArr := make([]byte, advproto.HeaderLength)
	svc := &service{node: int32(*node), stateDir: *stateDir, reply: atomic.MakeBuffer(replyArr), replyArr: replyArr}
	agent, err := cluster.NewClusteredServiceAgent(ctx, opts, svc)
	if err != nil {
		event("exit err=%v", err)
		os.Exit(2)
	}
	err = agent.StartAndRun()
	event("exit err=%v", err)
	if err != nil {
		os.Exit(1)
	}
}
