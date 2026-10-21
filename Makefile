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

# Core client packages run under the race detector. checkptr (enabled by -race) is disabled because
# the flyweight/counters code does pointer arithmetic over mmapped buffers that checkptr rejects.
RACE_PKGS       ?= ./aeron/... ./archive/... ./cluster/...
RACE_FLAGS      ?= -race -gcflags=all=-d=checkptr=0

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

# Java 1.53.3 stacks used by AERON_E2E tests: archiving driver (docker-compose.yml), authenticating
# archive (auth.compose.yml) and a 1-node echo cluster with its client driver (cluster.compose.yml).
e2e-up:
	$(COMPOSE) up -d --build --wait archiving-driver
	$(AUTH_COMPOSE) up -d --build --wait auth-archiving-driver
	$(CLUSTER_COMPOSE) up -d --build --wait client-driver cluster-node-0

e2e-down:
	-$(CLUSTER_COMPOSE) --profile three down -v
	-$(AUTH_COMPOSE) down -v
	$(COMPOSE) --profile e2e --profile clients down -v --remove-orphans

# Every AERON_E2E=1 gated test in the repository against the docker Java 1.53.3 stacks. cluster/client
# talks to the cluster's own client driver, everything else to the archiving driver at $(AERON_DIR).
e2e-test:
	AERON_E2E=1 $(GO) test -count=1 -p 1 -timeout 15m $$($(GO) list ./... | grep -v '/cluster/client$$')
	AERON_E2E=1 AERON_DIR=$(CLUSTER_AERON_DIR) $(GO) test -count=1 -timeout 10m ./cluster/client/

e2e:
	$(MAKE) e2e-up
	$(MAKE) e2e-test; status=$$?; $(MAKE) e2e-down; exit $$status

# The Go clients in containers (docker compose --profile e2e), failing if any checker fails.
e2e-docker:
	$(COMPOSE) --profile e2e up -d --build
	$(COMPOSE) --profile e2e wait $(E2E_CHECKERS); status=$$?; \
	$(COMPOSE) --profile e2e logs --no-color $(E2E_CHECKERS) | tail -n 40; \
	$(MAKE) e2e-down; exit $$status
