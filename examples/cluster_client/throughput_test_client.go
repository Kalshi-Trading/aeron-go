package main

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster/client"
	"github.com/lirm/aeron-go/cluster/codecs"
)

type TestContext struct {
	messageCount int
	latencies    []int64
}

func (ctx *TestContext) OnMessage(clusterSessionId int64, timestamp int64,
	buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
	recvTime := time.Now().UnixNano()
	msgNo := buffer.GetInt32(offset)
	sendTime := buffer.GetInt64(offset + 8)
	latency := recvTime - sendTime
	if msgNo < 1 || int(msgNo) > len(ctx.latencies) {
		fmt.Printf("OnMessage - sessionId=%d timestamp=%d pos=%d length=%d latency=%d\n",
			clusterSessionId, timestamp, header.Position(), length, latency)
	} else {
		ctx.latencies[msgNo-1] = latency
		ctx.messageCount++
	}
}

func (ctx *TestContext) OnSessionEvent(correlationId, clusterSessionId, leadershipTermId int64,
	leaderMemberId int32, code codecs.EventCodeEnum, detail string) {
	fmt.Printf("OnSessionEvent - sessionId=%d code=%d detail=%s\n", clusterSessionId, code, detail)
}

func (ctx *TestContext) OnNewLeader(clusterSessionId, leadershipTermId int64, leaderMemberId int32,
	ingressEndpoints string) {
	fmt.Printf("OnNewLeader - sessionId=%d leaderMemberId=%d leadershipTermId=%d\n",
		clusterSessionId, leaderMemberId, leadershipTermId)
}

func (ctx *TestContext) OnAdminResponse(clusterSessionId, correlationId int64,
	requestType codecs.AdminRequestTypeEnum, responseCode codecs.AdminResponseCodeEnum, message string,
	payload *atomic.Buffer, payloadOffset, payloadLength int32) {
	fmt.Printf("OnAdminResponse - correlationId=%d code=%d message=%s\n", correlationId, responseCode, message)
}

func main() {
	clusterCtx := client.NewContext()
	if aeronDir := os.Getenv("AERON_DIR"); aeronDir != "" {
		clusterCtx.AeronDirectoryName = aeronDir
		fmt.Println("aeron dir: ", aeronDir)
	} else if _, err := os.Stat("/dev/shm"); err == nil {
		path := fmt.Sprintf("/dev/shm/aeron-%s", aeron.UserName)
		clusterCtx.AeronDirectoryName = path
		fmt.Println("aeron dir: ", path)
	}
	if idleStr := os.Getenv("NO_OP_IDLE"); idleStr != "" {
		clusterCtx.IdleStrategy = &idlestrategy.Busy{}
	}
	clusterCtx.IngressChannel = "aeron:udp?alias=cluster-client-ingress"
	clusterCtx.IngressEndpoints = "0=localhost:20000,1=localhost:21000,2=localhost:22000"
	if endpoints := os.Getenv("INGRESS_ENDPOINTS"); endpoints != "" {
		clusterCtx.IngressEndpoints = endpoints
	}

	listener := &TestContext{latencies: make([]int64, 1000)}
	clusterCtx.EgressListener = listener

	clusterClient, err := client.Connect(clusterCtx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("connected - sessionId=%d leaderMemberId=%d leadershipTermId=%d\n",
		clusterClient.ClusterSessionId(), clusterClient.LeaderMemberId(), clusterClient.LeadershipTermId())

	idle := clusterCtx.IdleStrategy
	nextKeepAlive := time.Now().Add(time.Second)
	sendKeepAliveIfNecessary := func() {
		if time.Now().After(nextKeepAlive) {
			if ok, _ := clusterClient.SendKeepAlive(); ok {
				nextKeepAlive = nextKeepAlive.Add(time.Second)
			}
		}
	}

	sendBuf := atomic.MakeBuffer(make([]byte, 100))
	for round := 1; round <= 10 && !clusterClient.IsClosed(); round++ {
		fmt.Printf("starting round #%d\n", round)
		listener.messageCount = 0
		sentCt := 0
		beginTime := time.Now().UnixNano()
		latencies := listener.latencies
		for i := range latencies {
			latencies[i] = 0
		}
		ct := len(latencies)
		for i := 1; i <= ct; i++ {
			sendBuf.PutInt32(0, int32(i))
			sendBuf.PutInt64(8, time.Now().UnixNano())
			for {
				if r := clusterClient.Offer(sendBuf, 0, sendBuf.Capacity()); r >= 0 {
					sentCt++
					break
				}
				clusterClient.PollEgress()
				sendKeepAliveIfNecessary()
			}
		}
		for listener.messageCount < sentCt {
			pollCt := clusterClient.PollEgress()
			if pollCt == 0 {
				sendKeepAliveIfNecessary()
			}
			idle.Idle(pollCt)
		}
		now := time.Now()
		totalNs := now.UnixNano() - beginTime
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		fmt.Printf("round #%d complete, count=%d min=%d 10%%=%d 50%%=%d 90%%=%d max=%d throughput=%.2f\n",
			round, sentCt, latencies[ct-sentCt]/1000, latencies[ct/10]/1000, latencies[ct/2]/1000, latencies[9*(ct/10)]/1000,
			latencies[ct-1]/1000, (float64(sentCt) * 1000000000.0 / float64(totalNs)))

		for time.Since(now) < 10*time.Second {
			sendKeepAliveIfNecessary()
			idle.Idle(clusterClient.PollEgress())
		}
	}
	_ = clusterClient.Close()
	fmt.Println("done")
}
