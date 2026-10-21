// Package conductorfaults adversarially tests the Go ClientConductor and the client/driver command protocol against a
// real Java Aeron 1.53.3 media driver: driver SIGSTOP stalls during every blocking command, driver error responses,
// a full to-driver ring buffer, async add/cancel, counters and heartbeats across driver restarts, add/remove/offer/
// poll/close storms during driver crashes, kill -9 / SIGSTOP of real Go and Java client processes, and UDP
// blackholes via iptables. Every test asserts Java-equivalent typed errors, bounded blocking, callbacks, data
// integrity and no leaked goroutines, fds, mmaps or driver counters.
//
// The suite owns AERON_DIR=/dev/shm/aeron-adv-conductor-faults, UDP ports 27800-27899, stream ids 9800-9899 and the
// compose project adv-conductor-faults (docker/adversarial-conductor-faults.compose.yml). Tests (re)start the driver
// container themselves and do not depend on order. Run with:
//
//	make docker-mirror jar        # once; avoids Docker Hub 429s
//	docker compose -f docker/interop/docker-compose.yml --profile tools build   # Java peer image
//	docker compose -p adv-conductor-faults -f docker/adversarial-conductor-faults.compose.yml up -d --build --wait
//	AERON_E2E=1 AERON_ADVERSARIAL=1 go test -count=1 -p 1 -race -v ./systests/adversarial/conductor-faults/
//	docker compose -p adv-conductor-faults -f docker/adversarial-conductor-faults.compose.yml down
//
// The iptables scenarios need passwordless sudo (or root); they are skipped otherwise. The Java peer scenarios use
// the aeron-go-interop-java:1.53.3 image (override with AERON_INTEROP_IMAGE) and are skipped if it is missing.
package conductorfaults
