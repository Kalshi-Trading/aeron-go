# aeron-go/cluster

Implementation of [Aeron Cluster](https://github.com/real-logic/Aeron/tree/master/aeron-cluster) service container in Go.
Most structs and functions have near one-to-one parity with the Java classes and
methods on which they are based.

The [Java media driver, archive and consensus module](https://github.com/real-logic/aeron/blob/master/aeron-cluster/src/main/java/io/aeron/cluster/ClusteredMediaDriver.java)
must be used to run a cluster.

The [Aeron Cluster
protocol](http://github.com/real-logic/aeron/blob/master/aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml)
is specified in xml using the [Simple Binary Encoding (SBE)](https://github.com/real-logic/simple-binary-encoding).

## Current State
Parity target: Java `io.aeron.cluster.service` 1.53.3 against cluster schema v17. Supported: snapshots (incl. standby
snapshot flags), timers, multiple services, the full [Cluster](cluster.go) API (egress `Offer`/`TryClaim`, session
lookup/enumeration/close, timers, time unit, role, idle strategy), `ServiceAck` with ackId/relevantId,
`RequestServiceAck`, `ServiceTerminationPosition` ackRequired, `JoinLog` isStandby, lazy egress publications,
semantic-major app version validation and a Java-compatible `ClusterMarkFile`.

An end-to-end test against a Java 1.53.3 `ClusteredMediaDriver` lives in [e2e_test.go](e2e_test.go):

    docker compose -f docker/cluster-service.compose.yml up -d --build cluster-node
    AERON_E2E=1 go test ./cluster -run TestE2E -v

## Examples

[echo_service.go](../examples/cluster/echo_service.go) implements a basic echo service and can be
used in place of its [Java equivalent](https://github.com/real-logic/aeron/blob/master/aeron-samples/src/main/java/io/aeron/samples/cluster/EchoService.java).

[throughput_test_client.go](../examples/cluster_client/throughput_test_client.go) implements an example
of using the cluster client. 

# Backlog
 * godoc improvements
 * testing
 * SBE encoding/decoding improvements
