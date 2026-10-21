# Java Aeron 1.53.3 cluster for cluster client E2E tests

`cluster.compose.yml` runs `io.aeron.samples.cluster.EchoServiceNode` (ClusteredMediaDriver +
ClusteredServiceContainer with an echo service, from the aeron-all jar) plus a standalone `client-driver`
MediaDriver used by Go clients, so cluster nodes can be killed without killing the client's driver.
Admin requests are allowed (`aeron.cluster.authorisation.service.supplier=ALLOW_ALL`).

    # 1 node (ingress 0=localhost:9002)
    docker compose -f docker/cluster.compose.yml up -d --build client-driver cluster-node-0
    AERON_E2E=1 go test ./cluster/client -run E2E -v

    # 3 nodes (ingress 0=localhost:9002,1=localhost:9102,2=localhost:9202); stop cluster-node-0 first
    docker compose -f docker/cluster.compose.yml rm -sf cluster-node-0
    docker compose -f docker/cluster.compose.yml --profile three up -d --build client-driver node0 node1 node2
    AERON_E2E=1 CLUSTER_E2E_NODES=3 go test ./cluster/client -run E2E -v   # includes leader failover (docker kill)

    docker compose -f docker/cluster.compose.yml --profile three down

Clients use `AERON_DIR=/dev/shm/aeron-cluster-client` (the test default). Nodes use
`/dev/shm/aeron-cluster-<n>-driver` and keep cluster/archive state on a tmpfs at `/data`.
