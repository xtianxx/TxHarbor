.PHONY: test test-integration db-reset lint

# Unit tests (no Docker required).
test:
	go test ./...

# Integration tests (testcontainers; requires a Docker daemon).
test-integration:
	go test -tags integration ./...

# Explicit database wipe: removes the named volume (data is kept otherwise).
db-reset:
	docker compose down -v

lint:
	@test -z "$$(gofmt -l .)" || { echo "gofmt needed:"; gofmt -l .; exit 1; }
	go vet ./...
