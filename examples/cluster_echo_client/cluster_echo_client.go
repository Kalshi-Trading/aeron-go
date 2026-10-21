/*
Copyright 2016 Stanislav Liberman

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// cluster_echo_client connects to a cluster running an echo service (Java
// io.aeron.samples.cluster.EchoServiceNode, see docker/cluster.compose.yml, or the Go
// examples/cluster echo service), sends -m messages and waits for each to be echoed back.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster/client"
	"github.com/lirm/aeron-go/examples"
)

var (
	ingressEndpoints = flag.String("ingress", "0=localhost:9002", "cluster ingress endpoints (memberId=host:port,...)")
	egressChannel    = flag.String("egress", "aeron:udp?endpoint=localhost:0", "egress channel")
	timeout          = flag.Duration("timeout", 10*time.Second, "timeout to connect and to receive each echo")
)

type listener struct {
	client.NoOpEgressEvents
	echoes [][]byte
}

func (l *listener) OnMessage(clusterSessionId int64, timestamp int64, buffer *atomic.Buffer, offset int32,
	length int32, _ *logbuffer.Header) {
	msg := buffer.GetBytesArray(offset, length)
	fmt.Printf("Echo received: sessionId=%d timestamp=%d <<%s>>\n", clusterSessionId, timestamp, msg)
	l.echoes = append(l.echoes, msg)
}

func (l *listener) OnNewLeader(_ int64, leadershipTermId int64, leaderMemberId int32, _ string) {
	fmt.Printf("New leader: memberId=%d leadershipTermId=%d\n", leaderMemberId, leadershipTermId)
}

func main() {
	_ = flag.Set("m", "10")
	flag.Parse()
	examples.SetupLogging()
	ctx, cancel := examples.SignalContext()
	defer cancel()

	l := &listener{}
	clusterCtx := client.NewContext()
	clusterCtx.AeronDirectoryName = *examples.ExamplesConfig.AeronPrefix
	clusterCtx.IngressChannel = "aeron:udp"
	clusterCtx.IngressEndpoints = *ingressEndpoints
	clusterCtx.EgressChannel = *egressChannel
	clusterCtx.MessageTimeout = *timeout
	clusterCtx.EgressListener = l
	ac, err := client.Connect(clusterCtx)
	if err != nil {
		log.Fatalf("Failed to connect to cluster at %s: %v", *ingressEndpoints, err)
	}
	defer ac.Close()
	fmt.Printf("Connected: sessionId=%d leaderMemberId=%d leadershipTermId=%d\n",
		ac.ClusterSessionId(), ac.LeaderMemberId(), ac.LeadershipTermId())
	idle := examples.NewIdleStrategy()
	var deadline time.Time

	buffer := atomic.MakeBuffer(make([]byte, 256))
	messages := *examples.ExamplesConfig.Messages
	for i := 0; i < messages && ctx.Err() == nil; i++ {
		msg := []byte(fmt.Sprintf("Hello Cluster! %d", i))
		buffer.PutBytesArray(0, &msg, 0, int32(len(msg)))
		deadline = time.Now().Add(*timeout)
		for ac.Offer(buffer, 0, int32(len(msg))) < 0 {
			if ctx.Err() != nil || time.Now().After(deadline) {
				log.Fatalf("Failed to offer message %d", i)
			}
			idle.Idle(ac.PollEgress())
		}
		for len(l.echoes) <= i {
			if ctx.Err() != nil || time.Now().After(deadline) {
				log.Fatalf("Timed out waiting for echo of message %d", i)
			}
			idle.Idle(ac.PollEgress())
		}
		if !bytes.Equal(l.echoes[i], msg) {
			log.Fatalf("Echo mismatch: sent %q received %q", msg, l.echoes[i])
		}
	}
	fmt.Printf("Done: %d messages echoed\n", len(l.echoes))
}
