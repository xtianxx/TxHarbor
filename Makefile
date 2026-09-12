.PHONY: test test-race test-integration db-reset lint build

# Unit tests (no Docker required). -count=1 defeats the test cache so CI
# always executes the tests instead of reporting a cached pass.
test:
	go test -count=1 -timeout 5m ./...

# Unit tests under the race detector (no Docker required).
test-race:
	go test -race -count=1 -timeout 10m ./...

# Integration tests (testcontainers; requires a Docker daemon).
test-integration:
	go test -tags integration -count=1 -timeout 20m ./...

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
