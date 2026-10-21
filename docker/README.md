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
