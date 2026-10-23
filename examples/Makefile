# Builds every sample into ./bin. Run against a driver with e.g. `AERON_DIR=/dev/shm/aeron-compose bin/basic_subscriber`.
targets := basic_publisher basic_subscriber exclusive_publisher simple_publisher simple_subscriber \
	multiple_publishers_with_fragmentation multiple_subscribers_with_fragment_assembly \
	streaming_publisher rate_subscriber exclusive_throughput ping pong \
	aeronstat errorstat lossstat backlogstat cluster_echo_client basic_publisher_claim
archive_targets := basic_recording_publisher basic_replayed_subscriber basic_replay_merge persistent_subscriber replay_throughput

.PHONY: all e2e $(targets) $(archive_targets)
all: $(targets) $(archive_targets)

$(targets):
	go build -o bin/ ./$@

$(archive_targets):
	go build -o bin/ ../archive/examples/$@

# Requires the docker Java 1.53.3 driver (and docker/cluster.compose.yml for the cluster test).
e2e:
	AERON_E2E=1 go test -count=1 -v ./e2e
