// Package publishercrash adversarially tests publisher crashes, image lifecycle and resumption end to end against a
// real Java Aeron 1.53.3 media driver (docker/adversarial-publisher-crash.compose.yml, compose project
// adv-publisher-crash, AERON_DIR /dev/shm/aeron-adv-publisher-crash, UDP ports 27200-27299, stream ids 9200-9299).
//
// Publishers and subscribers run as real child processes, Go (cmd/crashclient) and Java 1.53.3 (the CrashTool image
// built from docker/adversarial-publisher-crash), and are crashed with SIGKILL/SIGSTOP; the driver container is
// recreated for every test and SIGKILLed/SIGSTOPped by some tests. Network faults use tc netem on lo from a
// NET_ADMIN sidecar on the host network.
//
// Run (Docker with host networking, /dev/shm, `make jar` and `make docker-mirror` done):
//
//	docker compose -p adv-publisher-crash -f docker/adversarial-publisher-crash.compose.yml --profile tools build
//	AERON_E2E=1 AERON_ADVERSARIAL=1 go test -count=1 -p 1 -v ./systests/adversarial/publisher-crash/
//
// The tests build the images and the Go helper themselves if needed, start and stop the driver container and leave
// nothing running. Without AERON_E2E=1 AERON_ADVERSARIAL=1 every test is skipped.
package publishercrash
