.PHONY: test test-race test-integration test-integration-redis test-integration-kafka test-contract test-e2e test-fault test-perf db-reset lint build

# require_tagged_tests guards a layered target: when no test file carries the
# build tag yet, the layer reports NOT RUN and exits non-zero instead of
# passing vacuously (verification.md §4: an unrun check must never read as a
# pass). The layer is "not run", not "green".
define require_tagged_tests
	@files="$$(grep -rl --include='*_test.go' --exclude-dir=.git --exclude-dir=.omo --exclude-dir=.evidence '^//go:build $(1)' . 2>/dev/null || true)"; \
	if [ -z "$$files" ]; then \
		echo "$(2): NOT RUN — no $(1)-tagged tests found"; \
		exit 1; \
	fi
endef

# Unit tests (no Docker required). -count=1 defeats the test cache so CI
# always executes the tests instead of reporting a cached pass.
test:
	go test -count=1 -timeout 5m ./...

# Unit tests under the race detector (no Docker required).
test-race:
	go test -race -count=1 -timeout 10m ./...

# Integration tests, PostgreSQL layer (testcontainers; requires a Docker daemon).
test-integration:
	go test -tags integration -count=1 -timeout 20m ./...

# Integration tests, Redis layer (testcontainers; requires a Docker daemon).
test-integration-redis:
	$(call require_tagged_tests,integration_redis,test-integration-redis)
	go test -tags integration_redis -count=1 -timeout 20m ./...

# Integration tests, Kafka layer (testcontainers; requires a Docker daemon).
test-integration-kafka:
	$(call require_tagged_tests,integration_kafka,test-integration-kafka)
	go test -tags integration_kafka -count=1 -timeout 30m ./...

# Contract layer: event envelope, catalog, schema-version and consumer
# compatibility. No middleware, no Docker — plain Go (FR-28;
# verification.md §3). Contract cases arrive with T014/T051/T058.
test-contract:
	$(call require_tagged_tests,contract,test-contract)
	go test -tags contract -count=1 -timeout 10m ./...

# End-to-end core deposit/withdrawal flows (full stack + Anvil). Independent
# layer, never part of a plain unit run.
test-e2e:
	$(call require_tagged_tests,e2e,test-e2e)
	go test -tags e2e -count=1 -timeout 30m ./...

# Fault injection and performance layers run independently (scheduled/manual/
# release gate) and never block ordinary PRs (FR-28; verification.md §4).
test-fault:
	$(call require_tagged_tests,fault,test-fault)
	go test -tags fault -count=1 -timeout 60m ./...

test-perf:
	$(call require_tagged_tests,perf,test-perf)
	go test -tags perf -count=1 -timeout 60m ./...

# Explicit database wipe: removes the named volume (data is kept otherwise).
db-reset:
	docker compose down -v

# Format and vet, covering both build tags.
lint:
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed:"; gofmt -l .; exit 1; }
	go vet ./...
	go vet -tags integration ./...

build:
	go build ./...
