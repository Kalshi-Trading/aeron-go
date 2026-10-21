package conductorfaults

import (
	"fmt"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/stretchr/testify/require"
)

type mediaPair struct {
	pub aeron.Publisher
	sub *aeron.Subscription
}

// mediaVariants build a connected publication/subscription pair on c for every channel topology.
func mediaVariants() map[string]func(t *testing.T, c *client, stream int32) mediaPair {
	return map[string]func(t *testing.T, c *client, stream int32) mediaPair{
		"MDCManual": func(t *testing.T, c *client, stream int32) mediaPair {
			port := nextPort()
			sub, err := c.AddSubscription(udp(port), stream)
			require.NoError(t, err)
			pub, err := c.AddExclusivePublication("aeron:udp?control-mode=manual", stream)
			require.NoError(t, err)
			_, err = c.AddDestination(pub.RegistrationID(), udp(port))
			require.NoError(t, err)
			return mediaPair{pub, sub}
		},
		"MDCDynamic": func(t *testing.T, c *client, stream int32) mediaPair {
			ctrl, port := nextPort(), nextPort()
			pub, err := c.AddPublication(fmt.Sprintf("aeron:udp?control=localhost:%d|control-mode=dynamic", ctrl), stream)
			require.NoError(t, err)
			sub, err := c.AddSubscription(fmt.Sprintf(
				"aeron:udp?endpoint=localhost:%d|control=localhost:%d|control-mode=dynamic", port, ctrl), stream)
			require.NoError(t, err)
			return mediaPair{pub, sub}
		},
		"MultiDestinationSubscription": func(t *testing.T, c *client, stream int32) mediaPair {
			port := nextPort()
			sub, err := c.AddSubscription("aeron:udp?control-mode=manual", stream)
			require.NoError(t, err)
			require.NoError(t, c.AddRcvDestination(sub.RegistrationID(), udp(port)))
			pub, err := c.AddPublication(udp(port), stream)
			require.NoError(t, err)
			return mediaPair{pub, sub}
		},
		"Spy": func(t *testing.T, c *client, stream int32) mediaPair {
			port := nextPort()
			pub, err := c.AddExclusivePublication(udp(port)+"|ssc=true", stream)
			require.NoError(t, err)
			sub, err := c.AddSubscription("aeron-spy:"+udp(port), stream)
			require.NoError(t, err)
			return mediaPair{pub, sub}
		},
		"Multicast": func(t *testing.T, c *client, stream int32) mediaPair {
			// Java requires an odd data group address (the control address is data+1).
			ch := fmt.Sprintf("aeron:udp?endpoint=224.0.1.%d:%d|interface=127.0.0.1|ttl=0", 1+2*(nextPort()%100), nextPort())
			sub, err := c.AddSubscription(ch, stream)
			require.NoError(t, err)
			pub, err := c.AddPublication(ch, stream)
			require.NoError(t, err)
			return mediaPair{pub, sub}
		},
	}
}

// TestMediaTopologiesAcrossDriverRestart streams over every channel topology (MDC manual/dynamic, MDS, spy,
// multicast), SIGKILLs the driver, and checks the client terminates with all publications and subscriptions closed,
// then rebuilds the same topology on a fresh client and streams again without leaks.
func TestMediaTopologiesAcrossDriverRestart(t *testing.T) {
	setup(t)
	for name, build := range mediaVariants() {
		build := build
		t.Run(name, func(t *testing.T) {
			startDriver(t)
			before := snapshot(t)
			c := connect(t, 3*time.Second)
			stream := nextStream()
			m := build(t, c, stream)
			roundTrip(t, m.pub, m.sub, 200)
			restartDriver(t)
			awaitTrue(t, 15*time.Second, "client terminated", c.IsClosed)
			require.True(t, m.sub.IsClosed())
			require.True(t, m.pub.IsClosed())
			require.NoError(t, c.Close())

			c2 := connect(t, 5*time.Second)
			m2 := build(t, c2, stream)
			roundTrip(t, m2.pub, m2.sub, 200)
			require.Empty(t, c2.errs.all())
			require.NoError(t, c2.Close())
			assertNoLeaks(t, before)
		})
	}
}
