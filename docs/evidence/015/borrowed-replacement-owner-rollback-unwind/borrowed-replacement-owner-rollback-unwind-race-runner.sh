#!/usr/bin/env bash
set -euo pipefail
# Owner rollback-unwind lane race runner (corrected test selection) (validation-only): executes
# the sealed host-linked -race binary in the identical pinned root/private-PID
# container pattern as the gold runner.
LABEL=race
ROOT=/home/dream/product_env/TxHarbor
IMAGE='postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280'
RUN_DIR="/tmp/opencode/015-e504462/borrowed-replacement-owner-rollback-unwind-$LABEL-run"
SCRATCH="$RUN_DIR/scratch"
mkdir -p "$RUN_DIR" "$SCRATCH/gocache" "$SCRATCH/tmp" "$SCRATCH/home"
cp /tmp/opencode/015-e504462/borrowed-replacement-owner-rollback-unwind-race.test "$RUN_DIR/borrowed-replacement-owner-rollback-unwind-race.test"
LOG="/tmp/opencode/015-e504462/borrowed-replacement-owner-rollback-unwind-$LABEL.log"
if [[ -e "$LOG" ]]; then
  echo "refusing to overwrite existing log: $LOG" >&2
  exit 3
fi
set +e
docker run --rm --init --network host --user 0:0 --group-add 989 \
  --mount "type=bind,src=$ROOT,dst=/workspace,readonly" \
  --mount "type=bind,src=$ROOT,dst=$ROOT,readonly" \
  --mount "type=bind,src=/usr/local/go,dst=/host-go,readonly" \
  --mount "type=bind,src=/home/dream/go/pkg/mod,dst=/host-gomodcache,readonly" \
  --mount "type=bind,src=/usr/bin/docker,dst=/usr/bin/docker,readonly" \
  --mount "type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock" \
  --mount "type=bind,src=$RUN_DIR,dst=/pg-evidence" \
  --mount "type=bind,src=$SCRATCH,dst=/pg-scratch" \
  -e 'PATH=/usr/lib/postgresql/18/bin:/host-go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' \
  -e 'GOROOT=/host-go' -e 'GOMODCACHE=/host-gomodcache' \
  -e 'CGO_ENABLED=0' -e 'GOCACHE=/pg-scratch/gocache' \
  -e 'TMPDIR=/pg-scratch/tmp' -e 'HOME=/pg-scratch/home' \
  -e 'DOCKER_HOST=unix:///var/run/docker.sock' \
  -e 'TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1' \
  -e 'TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock' \
  -e 'CI=true' -e 'TXHARBOR_REQUIRE_DOCKER=1' \
  --entrypoint /bin/sh "$IMAGE" -ec '
    unset PGDATA PG_MAJOR PG_VERSION
    cd /workspace
    pg_restore --version
    go version
    /pg-evidence/borrowed-replacement-owner-rollback-unwind-race.test -test.v -test.count=1 -test.timeout=30m -test.run "^TestBorrowedReplacementOwnerRollbackUnwind" 2>&1
  ' | tee "$LOG"
STATUS=${PIPESTATUS[0]}
echo "exit=$STATUS" | tee -a "$LOG"
exit "$STATUS"
