.PHONY: test test-race test-integration test-integration-redis test-integration-kafka test-contract test-e2e test-fault test-perf test-drill db-reset lint build smoke-quickstart

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
	go test -tags integration -count=1 -timeout 20m -p 6 ./...

# Integration tests, Redis layer (testcontainers; requires a Docker daemon).
test-integration-redis:
	$(call require_tagged_tests,integration_redis,test-integration-redis)
	go test -tags integration_redis -count=1 -timeout 20m ./internal/cache ./internal/ratelimit ./internal/testutil

# Integration tests, Kafka layer (testcontainers; requires a Docker daemon).
test-integration-kafka:
	$(call require_tagged_tests,integration_kafka,test-integration-kafka)
	go test -tags integration_kafka -count=1 -timeout 30m ./internal/app ./internal/events ./internal/health ./internal/testutil

# Contract layer: event envelope, catalog, schema-version and consumer
# compatibility. No middleware, no Docker — plain Go (FR-28;
# verification.md §3). Contract cases arrive with T014/T051/T058.
test-contract:
	$(call require_tagged_tests,contract,test-contract)
	go test -tags contract -count=1 -timeout 10m ./internal/events ./internal/reconciliation ./internal/recovery

# End-to-end core deposit/withdrawal flows (full stack + Anvil). Independent
# layer, never part of a plain unit run.
test-e2e:
	$(call require_tagged_tests,e2e,test-e2e)
	go test -tags e2e -count=1 -timeout 30m ./internal/app

# Fault injection, performance and full-drill layers run independently
# (scheduled/manual/release gate) and never block ordinary PRs (FR-28/FR-33;
# verification.md §4).
test-fault:
	$(call require_tagged_tests,fault,test-fault)
	go test -tags fault -count=1 -timeout 60m ./...

test-perf:
	$(call require_tagged_tests,perf,test-perf)
	go test -tags perf -count=1 -timeout 60m ./...

# Full disaster-recovery drill layer (quickstart S1-S12 + failure matrix F1-F7;
# real PG/Anvil and, for the event scenarios, Kafka). Independent channel only
# (scheduled/manual/release gate, see .github/workflows/drill.yml): it never
# runs on ordinary pull requests and never blocks them (quickstart.md §3;
# verification.md §4).
#
# NOT RUN discipline: in addition to requiring drill-tagged tests, the JSON
# checker requires every named S1-S12/F1-F7/effect/reentry scenario (and the
# restore-dependent F1/F3 subtests) to PASS. Missing or skipped cases fail this
# target, while unrelated optional skips remain allowed.
# Raw Go events and a per-run coverage report archive to the configured evidence
# directory. Each invocation gets a unique subdirectory; prior runs are untouched.
test-drill:
	$(call require_tagged_tests,drill,test-drill)
	@set +e; \
	if [ -n "$${TXHARBOR_DRILL_EVIDENCE_DIR:-}" ]; then \
		mkdir -p "$$TXHARBOR_DRILL_EVIDENCE_DIR" || exit 1; \
		run_dir="$$(mktemp -d "$$TXHARBOR_DRILL_EVIDENCE_DIR/run.XXXXXX")" || exit 1; \
		keep=1; \
	else \
		run_dir="$$(mktemp -d)" || exit 1; keep=0; \
	fi; \
	trap '[ "$$keep" -eq 1 ] || rm -rf -- "$$run_dir"' EXIT; \
	events="$$run_dir/go-test.json"; report="$$run_dir/coverage.json"; \
	started="$$(date -u +%Y-%m-%dT%H:%M:%SZ)"; run_id="$$(basename "$$run_dir")"; \
	go test -json -tags drill -count=1 -timeout 120m ./... > "$$events"; go_status=$$?; \
	if [ $$go_status -ne 0 ]; then cat "$$events"; fi; \
	go run scripts/drillcoverage/check.go "$$events" "$$report" "$$go_status" ./... "$$run_id" "$$started"; check_status=$$?; \
	if [ $$go_status -ne 0 ]; then exit $$go_status; fi; exit $$check_status

# Quick Start cold-start smoke test: fresh isolated Compose project, ports and
# volumes, environment built from .env.example only; a running development
# stack is never touched (see scripts/quickstart-smoke/run_smoke.sh).
smoke-quickstart:
	bash scripts/quickstart-smoke/run_smoke.sh

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
