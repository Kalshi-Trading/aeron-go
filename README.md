[![Build Status](https://travis-ci.org/lirm/aeron-go.svg?branch=master)](https://travis-ci.org/lirm/aeron-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/lirm/aeron-go)](https://goreportcard.com/report/github.com/lirm/aeron-go)
[![Join the chat at https://gitter.im/aeron-go/Lobby](https://badges.gitter.im/aeron-go/Lobby.svg)](https://gitter.im/aeron-go/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
<!--[![Coverage Status](https://coveralls.io/repos/github/lirm/aeron-go/badge.svg?branch=master)](https://coveralls.io/github/lirm/aeron-go?branch=master)-->

# aeron-go

Implementation of [Aeron](https://github.com/real-logic/Aeron) messaging client in Go.

Architecture, design, and protocol of Aeron can be found [here](https://github.com/real-logic/Aeron/wiki)

# Usage

Example subscriber can be found [here](https://github.com/lirm/aeron-go/tree/master/examples/basic_subscriber).

Example publication can be found [here](https://github.com/lirm/aeron-go/tree/master/examples/basic_publisher).

The full set of ported Java samples (throughput, ping/pong, AeronStat/ErrorStat/LossStat/BacklogStat,
archive record/replay, cluster echo client) is listed in [examples/README.md](examples/README.md); a
public API audit against Java 1.53.3 is in [docs/API_AUDIT.md](docs/API_AUDIT.md).

## Common

Instantiate Aeron with Context:
```go
ctx := aeron.NewContext().MediaDriverTimeout(time.Second * 10)

a := aeron.Connect(ctx)
```

## Subscribers

Create subscription:
```go
subscription, err := a.AddSubscription("aeron:ipc", 10)
if err != nil {
    log.Fatal(err)
}
defer subscription.Close()
```

`aeron.AddSubscription()` blocks until the driver has registered the subscription. Use
`AsyncAddSubscription()` + `GetSubscription()` to poll for registration without blocking.

Define callback for message processing:
```go
handler := func(buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
    bytes := buffer.GetBytesArray(offset, length)

    fmt.Printf("Received a fragment with payload: %s\n", string(bytes))
}
```

Poll for messages:
```go
idleStrategy := idlestrategy.Sleeping{SleepFor: time.Millisecond}

for {
    fragmentsRead := subscription.Poll(handler, 10)
    idleStrategy.Idle(fragmentsRead)
}
```

## Publications

Create publication:
```go
publication, err := a.AddPublication("aeron:ipc", 10)
if err != nil {
    log.Fatal(err)
}
defer publication.Close()
```

`aeron.AddPublication()` blocks until the driver has registered the publication. Use
`AsyncAddPublication()` + `GetPublication()` to poll for registration without blocking.

Create Aeron buffer to send the message:
```go
message := fmt.Sprintf("this is a message %d", counter)

srcBuffer := atomic.MakeBuffer(([]byte)(message))
```

Optionally make sure that there are connected subscriptions:
```go
for !publication.IsConnected() {
    time.Sleep(time.Millisecond * 10)
}
```

Send the message, by calling `publication.Offer`
```go
ret := publication.Offer(srcBuffer, 0, int32(len(message)), nil)
switch ret {
case aeron.NotConnected:
    log.Print("not connected yet")
case aeron.BackPressured:
    log.Print("back pressured")
default:
    if ret < 0 {
        log.Print("Unrecognized code: %d", ret)
    } else {
        log.Print("success!")
    }
}
```
