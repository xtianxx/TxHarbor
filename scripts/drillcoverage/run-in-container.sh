#!/usr/bin/env bash
set -euo pipefail

IMAGE='postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280'
IMAGE_DIGEST="${IMAGE#*@}"
SMOKE_ONLY=0
if [[ "${1:-}" == "--smoke" && $# -eq 1 ]]; then
	SMOKE_ONLY=1
elif [[ $# -ne 0 ]]; then
	echo "usage: $0 [--smoke]" >&2
	exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
GOROOT_HOST="$(go env GOROOT)"
GOMODCACHE_HOST="$(go env GOMODCACHE)"
GOROOT_CONTAINER=/host-go
GOMODCACHE_CONTAINER=/host-gomodcache
DOCKER_HOST_BIN="$(readlink -f "$(command -v docker)")"
MAKE_HOST_BIN="$(readlink -f "$(command -v make)")"
DOCKER_SOCKET="${DOCKER_SOCKET:-/var/run/docker.sock}"
RUNNER_UID="$(id -u)"
RUNNER_GID="$(id -g)"
DOCKER_SOCKET_GID="$(stat -c '%g' "$DOCKER_SOCKET")"
EVIDENCE_HOST="${TXHARBOR_DRILL_EVIDENCE_DIR:-${TMPDIR:-/tmp}/txharbor-drill-evidence}"
EVIDENCE_HOST="$(mkdir -p "$EVIDENCE_HOST" && cd "$EVIDENCE_HOST" && pwd -P)"
WRAPPER_RUN="$(mktemp -d "$EVIDENCE_HOST/wrapper.XXXXXX")"

for required in "$GOROOT_HOST/bin/go" "$GOMODCACHE_HOST" "$DOCKER_HOST_BIN" "$MAKE_HOST_BIN" "$DOCKER_SOCKET"; do
	if [[ ! -e "$required" ]]; then
		echo "drill container prerequisite missing: $required" >&2
		exit 1
	fi
done

cd "$REPO_ROOT"
TREE_FINGERPRINT_BEFORE="$(go run scripts/drillcoverage/check.go --fingerprint)"
if [[ ! "$TREE_FINGERPRINT_BEFORE" =~ ^sha256:[0-9a-f]{64}$ ]]; then
	echo "could not establish runtime/test source fingerprint" >&2
	exit 1
fi

SCRATCH_HOST="$(mktemp -d "${TMPDIR:-/tmp}/txharbor-drill.XXXXXX")"
cleanup() { rm -rf -- "$SCRATCH_HOST"; }
trap cleanup EXIT
mkdir -p "$SCRATCH_HOST/gocache" "$SCRATCH_HOST/tmp" "$SCRATCH_HOST/home"

docker_args=(
	--rm
	--init
	--user "$RUNNER_UID:$RUNNER_GID"
	--group-add "$DOCKER_SOCKET_GID"
	--add-host=host.docker.internal:host-gateway
	--mount "type=bind,src=$REPO_ROOT,dst=/workspace,readonly"
	--mount "type=bind,src=$GOROOT_HOST,dst=$GOROOT_CONTAINER,readonly"
	--mount "type=bind,src=$GOMODCACHE_HOST,dst=$GOMODCACHE_CONTAINER,readonly"
	--mount "type=bind,src=$DOCKER_HOST_BIN,dst=$DOCKER_HOST_BIN,readonly"
	--mount "type=bind,src=$MAKE_HOST_BIN,dst=$MAKE_HOST_BIN,readonly"
	--mount "type=bind,src=$DOCKER_SOCKET,dst=/var/run/docker.sock"
	--mount "type=bind,src=$SCRATCH_HOST,dst=/drill-scratch"
	--mount "type=bind,src=$EVIDENCE_HOST,dst=/drill-evidence"
	-e "PATH=/usr/lib/postgresql/18/bin:$GOROOT_CONTAINER/bin:$(dirname "$DOCKER_HOST_BIN"):$(dirname "$MAKE_HOST_BIN"):/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	-e "GOROOT=$GOROOT_CONTAINER"
	-e "GOMODCACHE=$GOMODCACHE_CONTAINER"
	# The pinned PostgreSQL image is intentionally minimal (no C compiler); use
	# Go's pure-Go implementations rather than installing unpinned build tools.
	-e "CGO_ENABLED=0"
	-e "GOCACHE=/drill-scratch/gocache"
	-e "TMPDIR=/drill-scratch/tmp"
	-e "HOME=/drill-scratch/home"
	-e "DOCKER_HOST=unix:///var/run/docker.sock"
	-e "TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal"
	-e "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock"
	-e "CI=true"
	-e "TXHARBOR_REQUIRE_DOCKER=1"
	-e "TXHARBOR_DRILL_EVIDENCE_DIR=/drill-evidence"
	-e "TXHARBOR_TESTED_TREE_FINGERPRINT=$TREE_FINGERPRINT_BEFORE"
	-e "TXHARBOR_DRILL_METADATA=/drill-evidence/$(basename "$WRAPPER_RUN")/metadata.json"
	-e "TXHARBOR_DRILL_CONTAINER_SMOKE=$SMOKE_ONLY"
	-e "IMAGE_DIGEST=$IMAGE_DIGEST"
)

for variable in TXHARBOR_PG_DSN TXHARBOR_RPC_URL TXHARBOR_CHAIN_ID TXHARBOR_COMMIT; do
	if [[ -v "$variable" ]]; then
		docker_args+=(--env "$variable=${!variable}")
	fi
done

set +e
docker run "${docker_args[@]}" --entrypoint /bin/sh "$IMAGE" -ec '
	unset PGDATA PG_MAJOR PG_VERSION
	cd /workspace
	command -v pg_restore >/dev/null || { echo "pg_restore is missing" >&2; exit 1; }
	command -v pg_dump >/dev/null || { echo "pg_dump is missing" >&2; exit 1; }
	pg_restore --version
	pg_dump --version
	pg_restore_version="$(pg_restore --version)"
	pg_dump_version="$(pg_dump --version)"
	go_version="$(go version)"
	make_version="$(make --version | head -n 1)"
	for tool in pg_restore pg_dump; do
		binary="$(readlink -f "$(command -v "$tool")")"
		magic="$(od -An -N4 -tx1 "$binary" | tr -d " \n")"
		if [ "$magic" != 7f454c46 ]; then
			echo "$tool is not a direct ELF executable: $binary" >&2
			exit 1
		fi
	done
	case "$(pg_restore --version)" in
		"pg_restore (PostgreSQL) 18.6"*) ;;
		*) echo "expected PostgreSQL 18.6 pg_restore" >&2; exit 1 ;;
	esac
	case "$(pg_dump --version)" in
		"pg_dump (PostgreSQL) 18.6"*) ;;
		*) echo "expected PostgreSQL 18.6 pg_dump" >&2; exit 1 ;;
	esac
	go_version="$(go version)"
	make_version="$(make --version | head -n 1)"
	docker info >/dev/null
	make -n test-drill >/dev/null
	go test ./scripts/drillcoverage
	# Metadata intentionally contains tool/runtime facts only, never inherited DSNs,
	# RPC URLs, keys, or a dump of the process environment.
	printf "%s\n" "$IMAGE_DIGEST" | grep -Eq "^sha256:[0-9a-f]{64}$" || { echo "image digest metadata is missing or invalid" >&2; exit 1; }
	printf "{\"image_digest\":\"%s\",\"go\":\"%s\",\"make\":\"%s\",\"pg_restore\":\"%s\",\"pg_dump\":\"%s\",\"cgo_mode\":\"CGO_ENABLED=0\",\"network\":\"host docker socket; testcontainers bridge\",\"mount_modes\":{\"workspace\":\"read-only\",\"go_root\":\"read-only\",\"module_cache\":\"read-only\",\"scratch\":\"read-write\",\"evidence\":\"read-write\",\"docker_socket\":\"read-write\"},\"native_postgres_tools\":\"direct ELF verified\"}\n" \
		"$IMAGE_DIGEST" "$go_version" "$make_version" "$pg_restore_version" "$pg_dump_version" > "$TXHARBOR_DRILL_METADATA"
	echo "Pinned-image tools and host Docker connectivity verified."
	if [ "$TXHARBOR_DRILL_CONTAINER_SMOKE" = 1 ]; then
		exit 0
	fi
	exec make test-drill
'
docker_status=$?
set -e
TREE_FINGERPRINT_AFTER="$(go run scripts/drillcoverage/check.go --fingerprint)"
tree_stable=false
if [[ "$TREE_FINGERPRINT_BEFORE" == "$TREE_FINGERPRINT_AFTER" ]]; then tree_stable=true; fi
printf '{"source_tree_stable":%s,"fingerprint_before":"%s","fingerprint_after":"%s"}\n' \
	"$tree_stable" "$TREE_FINGERPRINT_BEFORE" "$TREE_FINGERPRINT_AFTER" > "$WRAPPER_RUN/tree-validation.json"
if [[ "$tree_stable" != true ]]; then
	echo "runtime/test source changed during drill; evidence is not claimable" >&2
	exit 1
fi
exit "$docker_status"
