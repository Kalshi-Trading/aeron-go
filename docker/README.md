# Local Aeron (Java 1.53.3) for aeron-go testing

    docker compose -f docker/docker-compose.yml up -d --build archiving-driver
    export AERON_DIR=/dev/shm/aeron-compose
    # archive control: aeron:udp?endpoint=localhost:8010 stream 10
    # recording events: aeron:udp?control-mode=dynamic|control=localhost:8030 stream 30
    docker compose -f docker/docker-compose.yml --profile clients up -d   # Java sample pub (udp :20121/1001) + sub (udp :20122/1002)
    docker compose -f docker/docker-compose.yml down -v

The driver runs with host networking and shares /dev/shm with the host, so Go clients on the host
connect via shared memory exactly as with a local driver. `scripts/fetch-aeron-jar.sh` downloads the
same aeron-all jar for the in-process systests (`systests/driver`).

If Docker Hub rate limits (429), `make docker-mirror` pulls every base image used under docker/ from
mirror.gcr.io and tags it with its Docker Hub name (or set JAVA_IMAGE / GO_IMAGE / RUNTIME_IMAGE).

## End-to-end tests

    make e2e          # start archiving-driver + auth archive + 1-node cluster, run every AERON_E2E=1 test, tear down
    make e2e-docker   # same driver plus the Go clients from e2e/cmd running in containers (profile e2e)

`make e2e-docker` is `docker compose -f docker/docker-compose.yml --profile e2e up --build` plus
`docker compose wait` on the checkers, so it fails if any client exits non-zero. The `e2e` profile runs
two scenarios with the multi-stage image from `go-clients.Dockerfile`:

| service | role |
|---|---|
| go-subscriber | live subscriber on udp :22121 stream 3001, checks sequence/gaps/corruption of `E2E_COUNT` (100000) messages |
| go-recorder | archive StartRecording of the same channel, waits for the recording to stop |
| go-publisher | publishes `E2E_COUNT` sequenced messages once subscriber and recorder are ready |
| go-replayer | replays the finished recording and checks every message |
| go-mdc-publisher | records (remote) and publishes `E2E_MERGE_COUNT` (20000) messages on MDC control :22141 stream 3003 |
| go-replay-merge | replays that recording from the start and merges onto the live MDC stream (archive/replaymerge) |

Go tests in `e2e/` (and anything else gated on `AERON_E2E=1`) expect the driver at `AERON_DIR`
(`/dev/shm/aeron-compose`) and use udp ports 21121-21133; the compose clients use 22121-22151.

