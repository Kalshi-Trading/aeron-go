// Command advclient is a cluster client process the adversarial suite kills with SIGKILL. It connects, sends
// -count messages under -tag, waits for their echoes, prints "SESSION <id>" and "ACKED <n>", and then (with -hold)
// keeps the session alive by polling until it is killed.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/lirm/aeron-go/systests/adversarial/cluster/internal/advproto"
)

func main() {
	o := advproto.Options{}
	flag.StringVar(&o.AeronDir, "aeron-dir", "", "client media driver directory")
	flag.StringVar(&o.Endpoints, "endpoints", "", "ingress endpoints")
	flag.StringVar(&o.Egress, "egress", "", "egress channel")
	flag.Int64Var(&o.Tag, "tag", 1, "message tag")
	count := flag.Int64("count", 10, "messages to send")
	size := flag.Int("size", 64, "payload size")
	hold := flag.Bool("hold", false, "keep polling after sending")
	flag.Parse()

	s, err := advproto.NewSender(o)
	if err != nil {
		fmt.Printf("ERR connect %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("SESSION %d\n", s.AC.ClusterSessionId())
	deadline := time.Now().Add(30 * time.Second)
	for seq := int64(1); seq <= *count; {
		if s.Offer(advproto.KindData, seq, *size, false) > 0 {
			seq++
		} else {
			s.Poll(false)
		}
		if time.Now().After(deadline) {
			fmt.Println("ERR offer timeout")
			os.Exit(1)
		}
	}
	for s.Acked().Count() < *count {
		if s.Poll(false) == 0 {
			time.Sleep(time.Millisecond)
		}
		if time.Now().After(deadline) {
			fmt.Println("ERR ack timeout")
			os.Exit(1)
		}
	}
	fmt.Printf("ACKED %d\n", *count)
	for *hold {
		if s.Poll(false) == 0 {
			time.Sleep(time.Millisecond)
		}
		if s.AC.IsClosed() {
			fmt.Println("ERR closed")
			os.Exit(1)
		}
	}
	_ = s.AC.Close()
}
