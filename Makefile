# Top-level targets for unit, race and docker-backed end-to-end tests.
GO              ?= go
COMPOSE         ?= docker compose -f docker/docker-compose.yml
AUTH_COMPOSE    ?= docker compose -f docker/auth.compose.yml
CLUSTER_COMPOSE ?= docker compose -f docker/cluster.compose.yml
CLUSTER_AERON_DIR ?= /dev/shm/aeron-cluster-client
# Docker Hub rate limits anonymous pulls (429); `make docker-mirror` pre-pulls the base images of every
# docker/ Dockerfile from this mirror and tags them with their Docker Hub names.
DOCKER_MIRROR   ?= mirror.gcr.io/library
BASE_IMAGES     ?= eclipse-temurin:21-jre eclipse-temurin:21-jdk golang:1.21-bookworm debian:bookworm-slim
export AERON_DIR ?= /dev/shm/aeron-compose
export UID      ?= $(shell id -u)
export GID      ?= $(shell id -g)

# Core client packages run under the race detector (which also enables checkptr).
RACE_PKGS       ?= ./aeron/... ./archive/... ./cluster/...
RACE_FLAGS      ?= -race

E2E_CHECKERS    := go-subscriber go-recorder go-publisher go-replayer go-mdc-publisher go-replay-merge

.PHONY: all build vet test race jar docker-mirror e2e e2e-up e2e-test e2e-down e2e-docker e2e-bin

all: build vet test

build:
	$(GO) build ./...

vet:
	$(GO) vet ./...

jar:
	scripts/fetch-aeron-jar.sh

# Unit tests; systests start an in-process Java 1.53.3 media driver (make jar) and skip without one.
test:
	$(GO) test -count=1 ./...

race:
	$(GO) test -count=1 $(RACE_FLAGS) $(RACE_PKGS)

e2e-bin:
	$(GO) build -o bin/ ./e2e/cmd/...

docker-mirror:
	for image in $(BASE_IMAGES); do \
		docker pull -q $(DOCKER_MIRROR)/$$image && docker tag $(DOCKER_MIRROR)/$$image $$image || exit 1; \
	done

REPL_COMPOSE    ?= docker compose -f docker/replication.compose.yml
TERM_COMPOSE    ?= docker compose -f docker/driver-protocol.compose.yml
CLUSTER_SVC_COMPOSE ?= docker compose -f docker/cluster-service.compose.yml
INTEROP_COMPOSE ?= docker compose -f docker/interop/docker-compose.yml
AERON_JAR_PATH  ?= $(CURDIR)/systests/driver/aeron-all-1.53.3.jar
E2E_ENV         ?= AERON_E2E=1 AERON_JAR=$(AERON_JAR_PATH) AERON_TERMINATE_DIR=/dev/shm/aeron-terminable \
                   AERON_E2E_CLUSTER=1 AERON_E2E_BASIC_SUBSCRIBER=1

# Java 1.53.3 stacks used by AERON_E2E tests (docker/*.compose.yml): archiving driver + Java BasicSubscriber,
# second archive (replication), authenticating archive, publication verifier, terminable driver, 1-node echo
# cluster with its client driver, and the interop tool image. counters/logbuffer/reassembly helpers and the
# clustered-service node are started by their tests.
e2e-up: jar
	$(COMPOSE) --profile clients up -d --build --wait archiving-driver java-subscriber
	$(REPL_COMPOSE) up -d --build --wait
	$(AUTH_COMPOSE) up -d --build --wait auth-archiving-driver
	$(COMPOSE) -f docker/publications.compose.yml up -d --build --wait pub-verifier
	$(TERM_COMPOSE) up -d --build --wait terminable-driver
	$(CLUSTER_COMPOSE) up -d --build --wait client-driver cluster-node-0
	$(INTEROP_COMPOSE) --profile tools build

e2e-down:
	-$(CLUSTER_SVC_COMPOSE) down -v
	-$(CLUSTER_COMPOSE) --profile three down -v
	-$(TERM_COMPOSE) down -v
	-$(REPL_COMPOSE) down -v
	-$(AUTH_COMPOSE) down -v
	-docker compose -p aeron-counters -f docker/counters.compose.yml down -v
	$(COMPOSE) -f docker/publications.compose.yml -f docker/logbuffer.compose.yml -f docker/reassembly.compose.yml \
		--profile e2e --profile clients --profile tools down -v --remove-orphans

# Every AERON_E2E=1 gated test in the repository; keeps going after a failing phase and fails at the end.
#   1. everything except cluster/ and cluster/client against the archiving driver at $(AERON_DIR)
#   2. cluster/client against the 1-node cluster (its own client driver)
#   3. cluster/ (Go clustered service); its ClusteredMediaDriver shares archive port 8110 with the auth archive
#   4. cluster/client with CLUSTER_E2E_NODES=3 (leader failover); the 3 nodes reuse cluster-node-0's ports
#   5. systests/conductore2e driver-kill test (AERON_E2E_KILL=1 kills and restarts the archiving driver), last
e2e-test:
	status=0; \
	$(E2E_ENV) $(GO) test -count=1 -p 1 -timeout 20m $$($(GO) list ./... | grep -v -e '/cluster$$' -e '/cluster/client$$') || status=1; \
	$(E2E_ENV) AERON_DIR=$(CLUSTER_AERON_DIR) $(GO) test -count=1 -timeout 10m ./cluster/client/ || status=1; \
	$(AUTH_COMPOSE) down -v; \
	$(E2E_ENV) $(GO) test -count=1 -timeout 10m ./cluster/ || status=1; \
	$(CLUSTER_COMPOSE) rm -sf cluster-node-0; \
	$(CLUSTER_COMPOSE) --profile three up -d --build --wait node0 node1 node2 || status=1; \
	$(E2E_ENV) AERON_DIR=$(CLUSTER_AERON_DIR) CLUSTER_E2E_NODES=3 $(GO) test -count=1 -timeout 10m ./cluster/client/ || status=1; \
	$(E2E_ENV) AERON_E2E_KILL=1 $(GO) test -count=1 -timeout 5m -run TestDriverKillTimesOutAndClosesCleanly ./systests/conductore2e/ || status=1; \
	exit $$status

e2e:
	$(MAKE) e2e-up
	$(MAKE) e2e-test; status=$$?; $(MAKE) e2e-down; exit $$status

# The Go clients in containers (docker compose --profile e2e), failing if any checker fails.
e2e-docker:
	$(COMPOSE) --profile e2e up -d --build
	$(COMPOSE) --profile e2e wait $(E2E_CHECKERS); status=$$?; \
	$(COMPOSE) --profile e2e logs --no-color $(E2E_CHECKERS) | tail -n 40; \
	$(MAKE) e2e-down; exit $$status
