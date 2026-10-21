// Package archivecrash is an adversarial end-to-end suite for archive crashes during recording, replay and
// replication. It SIGKILLs/SIGSTOPs real Java Aeron 1.53.3 media drivers and archives running in docker (see
// docker/adversarial-archive-crash.compose.yml, compose project adv-archive-crash) and real Go client processes
// (the test binary re-executed as a child), and asserts that the Go archive client fails within its message
// timeout with the Java-equivalent errors, that recordings are recovered with a correct stop position, can be
// extended, and replay gap-free and byte-identical, and that nothing leaks.
//
// Resources are reserved for this suite only: AERON_DIR /dev/shm/aeron-adv-archive-crash, archive data under
// /dev/shm/adv-archive-crash-data, UDP ports 27300-27399 and stream ids 9300-9399. Every test recreates the stack
// (wiping the catalogs) so tests are independent of order.
//
// Run (requires docker, the docker-archiving-driver image built by `make e2e-docker` or `docker compose -f
// docker/adversarial-archive-crash.compose.yml build`, and for the Java peer tests the interop image built with
// `docker compose -f docker/interop/docker-compose.yml --profile tools build`):
//
//	AERON_E2E=1 AERON_ADVERSARIAL=1 go test -count=1 -p 1 -v ./systests/adversarial/archive-crash/
//
// Multicast scenarios need a multicast capable loopback (`sudo ip link set lo multicast on`) and are skipped
// otherwise.
package archivecrash
