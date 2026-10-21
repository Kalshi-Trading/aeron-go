# aeron-go samples

Go ports of the Java Aeron 1.53.3 `io.aeron.samples`. All samples talk to an external media driver;
the easiest one is the Java 1.53.3 ArchivingMediaDriver in [`docker/`](../docker/README.md):

```sh
docker compose -f docker/docker-compose.yml up -d --build archiving-driver
export AERON_DIR=/dev/shm/aeron-compose     # or pass -p /dev/shm/aeron-compose
cd examples && make                          # builds everything into examples/bin
```

Common flags (see `common.go`): `-p` aeron dir (default `$AERON_DIR`), `-chan`/`-sid` channel and
stream (Java `SampleConfiguration` defaults `aeron:udp?endpoint=localhost:20121` / `1001`), `-m`
messages, `-len` message length, `-frags` fragment limit, `-idle busy|yield|sleep|backoff`,
`-exclusive`, `-random`, `-linger` ms, `-exit N` (subscribers stop after N messages), `-l` debug logging.

| Sample | Java equivalent | Try it |
|---|---|---|
| `basic_publisher`, `basic_subscriber` | BasicPublisher / BasicSubscriber | `bin/basic_subscriber` then `bin/basic_publisher -m 10` |
| `exclusive_publisher` | ExclusivePublisher | `bin/exclusive_publisher -m 10` |
| `simple_publisher`, `simple_subscriber` | SimplePublisher / SimpleSubscriber | channel `aeron:udp?endpoint=localhost:40123` stream 10 |
| `multiple_publishers_with_fragmentation`, `multiple_subscribers_with_fragment_assembly` | same names | streams `-sid` and `-sid+1`, 10000/9000 byte messages |
| `streaming_publisher`, `rate_subscriber` | StreamingPublisher / RateSubscriber | `bin/rate_subscriber` then `bin/streaming_publisher -m 1000000 -len 64` |
| `exclusive_throughput` | EmbeddedExclusiveThroughput | `bin/exclusive_throughput -chan aeron:ipc` (external driver) |
| `ping`, `pong` | Ping / Pong | `bin/pong` then `bin/ping -m 100000` (HdrHistogram RTT) |
| `aeronstat` | AeronStat | `bin/aeronstat watch=false stream=1001` (Java-style `key=value` filters) |
| `errorstat` | ErrorStat | distinct errors from the CnC error log |
| `lossstat` | LossStat | `loss-report.dat` as CSV |
| `backlogstat` | BacklogStat | publisher/sender/receiver/subscriber backlog per stream |
| `cluster_echo_client` | cluster EchoService client | needs `docker/cluster.compose.yml` echo-cluster; `bin/cluster_echo_client -m 10` |
| `cluster` (service) | cluster EchoService | Go clustered service |
| `../archive/examples/basic_recording_publisher` | archive RecordedBasicPublisher | `-prefix $AERON_DIR -messages 10` |
| `../archive/examples/basic_replayed_subscriber` | archive ReplayedBasicSubscriber | replays the latest recording; `-exit 10` |
| `../archive/examples/replay_throughput` | archive EmbeddedReplayThroughput | records 1M IPC messages, replays them, prints rates |

Differences from Java: there is no embedded media driver in Go, so `Embedded*` samples use the
external driver; `PrintAvailableImage` cannot print the image's channel/stream/source because the Go
`Image` interface does not expose them (see `docs/API_AUDIT.md` A1). The CnC error-log, loss-report and
stream-counter parsing used by the stat tools lives in `examples/internal/cnc` until a library API exists.

## End-to-end tests

`e2e/` runs every sample against the docker Java driver and checks real output:

```sh
AERON_E2E=1 go test -count=1 -v ./examples/e2e
# cluster echo too:
docker compose -f docker/docker-compose.yml -f docker/cluster.compose.yml up -d --build archiving-driver echo-cluster
AERON_E2E=1 AERON_E2E_CLUSTER=1 go test -count=1 -v ./examples/e2e
```
