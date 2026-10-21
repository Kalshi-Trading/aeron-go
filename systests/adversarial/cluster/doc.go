// Package cluster is the adversarial crash/failover suite for the Go cluster client (cluster/client) and the Go
// clustered service (cluster) against real Java Aeron 1.53.3 ClusteredMediaDrivers in docker.
//
// Every test brings up its own docker/adversarial-cluster.compose.yml stack (compose project adv-cluster: three
// members node0..2 or the single member solo, plus a client media driver at AERON_DIR=/dev/shm/aeron-adv-cluster),
// starts one Go clustered service process (internal/advsvc) per member, and then crashes things for real: SIGKILL
// and SIGSTOP of member containers, SIGKILL of the Go service and of Go cluster client processes
// (internal/advclient), iptables partitions and tc netem loss/delay inside a member's network namespace (via the
// netem sidecar image with NET_ADMIN). After recovery it asserts that every member's service state is identical at
// the same log position, that no acknowledged message was lost or applied twice, and that the Go client leaks no
// goroutines, mmaps, fds or driver counters.
//
// Resources: UDP ports 27600-27699 on the 172.30.76.0/24 bridge network, cluster streams 9600-9608, state under
// $ADV_CLUSTER_DATA (default $HOME/adv-cluster-data, wiped per test).
//
// Run (images are built on first use; run make docker-mirror first if Docker Hub rate-limits):
//
//	docker compose -f docker/adversarial-cluster.compose.yml --profile tools --profile one build
//	AERON_E2E=1 AERON_ADVERSARIAL=1 go test -p 1 -count=1 -timeout 60m -v ./systests/adversarial/cluster/
package cluster
