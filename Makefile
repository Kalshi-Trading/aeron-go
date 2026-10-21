# Top-level targets for unit, race and docker-backed end-to-end tests.
GO              ?= go
COMPOSE         ?= docker compose -f docker/docker-compose.yml
export AERON_DIR ?= /dev/shm/aeron-compose
export UID      ?= $(shell id -u)
export GID      ?= $(shell id -g)

# Core client packages run under the race detector. checkptr (enabled by -race) is disabled because
# the flyweight/counters code does pointer arithmetic over mmapped buffers that checkptr rejects.
RACE_PKGS       ?= ./aeron/... ./archive/... ./cluster/...
RACE_FLAGS      ?= -race -gcflags=all=-d=checkptr=0

E2E_CHECKERS    := go-subscriber go-recorder go-publisher go-replayer go-mdc-publisher go-replay-merge

.PHONY: all build vet test race jar e2e e2e-up e2e-test e2e-down e2e-docker e2e-bin

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

e2e-up:
	$(COMPOSE) up -d --build --wait archiving-driver

e2e-down:
	$(COMPOSE) --profile e2e --profile clients down -v --remove-orphans

# Every AERON_E2E=1 gated test in the repository against the docker Java 1.53.3 archiving driver.
e2e-test:
	AERON_E2E=1 $(GO) test -count=1 -p 1 -timeout 15m ./...

e2e:
	$(MAKE) e2e-up
	$(MAKE) e2e-test; status=$$?; $(MAKE) e2e-down; exit $$status

# The Go clients in containers (docker compose --profile e2e), failing if any checker fails.
e2e-docker:
	$(COMPOSE) --profile e2e up -d --build
	$(COMPOSE) --profile e2e wait $(E2E_CHECKERS); status=$$?; \
	$(COMPOSE) --profile e2e logs --no-color $(E2E_CHECKERS) | tail -n 40; \
	$(MAKE) e2e-down; exit $$status
