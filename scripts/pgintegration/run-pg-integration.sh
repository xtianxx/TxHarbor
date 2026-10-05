#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# 015 持久复现入口：本脚本即 pgfull3b 类全仓 PG 集成运行的仓内复现入口
# （git 挂载：只读挂载真实仓库 /workspace 与真实 .git，并挂载宿主 git 二进制及
# 其共享库；pgfull3b 轮次的一次性 runner 以 root + safe.directory=/workspace 等价运行）。
# 与 /tmp/r2lab/run_suite.sh 的关系：run_suite.sh 为当轮一次性临时变体（LABEL 目录 +
# 固定 drill/focus 命令，不入仓）；本脚本为持久入口（--smoke/--focused/full、
# 源指纹前后校验、证据写入 TXHARBOR_PG_EVIDENCE_DIR）。
# focus 定向运行（drill 标签）可用如下等价形式：
#   docker run ... --entrypoint /bin/sh 'postgres@sha256:4ef4dbc9...' -ec \
#     'cd /workspace && go test -tags "linux,drill" -count=1 -run REGEX -v ./internal/recovery/'
# 归档与命令细节见 docs/evidence/015/verification-archive-2026-10-05/INDEX.md。
# 本注释块不改变任何执行逻辑。
# -----------------------------------------------------------------------------
set -euo pipefail

# Pinned PostgreSQL client tools plus host Go/Docker. Use host networking so
# the runner shares loopback with sibling containers' 127.0.0.1 port publishes.
IMAGE='postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280'
IMAGE_DIGEST="${IMAGE#*@}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
MODE="${1:-full}"
case "$MODE" in --smoke|--focused|full) ;; *) echo "usage: $0 [--smoke|--focused]" >&2; exit 2 ;; esac
if [[ "$MODE" == full && $# -ne 0 || "$MODE" != full && $# -ne 1 ]]; then
  echo "usage: $0 [--smoke|--focused]" >&2; exit 2
fi

for cmd in go docker git readlink stat ldd awk sort python3; do
  command -v "$cmd" >/dev/null || { echo "NOT RUN: required host dependency missing: $cmd" >&2; exit 1; }
done
DOCKER_BIN="$(readlink -f "$(command -v docker)")"
GIT_BIN="$(readlink -f "$(command -v git)")"
GO_ROOT="$(go env GOROOT)"
GO_MODCACHE="$(go env GOMODCACHE)"
DOCKER_SOCKET="${DOCKER_SOCKET:-/var/run/docker.sock}"
[[ -x "$GO_ROOT/bin/go" && -d "$GO_MODCACHE" ]] || {
  echo "NOT RUN: required Go toolchain/module cache missing" >&2; exit 1;
}
[[ -S "$DOCKER_SOCKET" ]] || { echo "NOT RUN: Docker socket missing: $DOCKER_SOCKET" >&2; exit 1; }
[[ -d "$ROOT/.git" ]] || { echo "intact Git working tree required at $ROOT" >&2; exit 1; }

# Mount real Git, its shared libraries and its external core-command/template
# data. Git executes normally against the intact, read-only repository.
GIT_LIBS=()
while IFS= read -r path; do [[ -f "$path" ]] && GIT_LIBS+=("$path"); done \
  < <(ldd "$GIT_BIN" | awk '/=> \/|^[[:space:]]*\// { for (i=1; i<=NF; i++) if ($i ~ /^\//) { print $i; break } }')
mapfile -t GIT_LIBS < <(printf '%s\n' "${GIT_LIBS[@]}" | sort -u)
for path in "${GIT_LIBS[@]}"; do
  [[ -r "$path" ]] || { echo "Git runtime dependency unavailable: $path" >&2; exit 1; }
done

EVIDENCE_ROOT="${TXHARBOR_PG_EVIDENCE_DIR:-/tmp/opencode/pg-integration}"
mkdir -p "$EVIDENCE_ROOT"
RUN_DIR="$(mktemp -d "$EVIDENCE_ROOT/run.XXXXXX")"
SCRATCH="$(mktemp -d /tmp/opencode/txharbor-pg-integration.XXXXXX)"
trap 'rm -rf -- "$SCRATCH"' EXIT
mkdir -p "$SCRATCH/gocache" "$SCRATCH/tmp" "$SCRATCH/home"
cd "$ROOT"
FINGERPRINT_BEFORE="$(go run scripts/drillcoverage/check.go --fingerprint)"
[[ "$FINGERPRINT_BEFORE" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "cannot establish source fingerprint" >&2; exit 1; }

UID_RUN="$(id -u)"; GID_RUN="$(id -g)"; SOCKET_GID="$(stat -c '%g' "$DOCKER_SOCKET")"
docker_args=(
  --rm --init --network host --user "$UID_RUN:$GID_RUN" --group-add "$SOCKET_GID"
  --mount "type=bind,src=$ROOT,dst=/workspace,readonly"
  --mount "type=bind,src=$GO_ROOT,dst=/host-go,readonly"
  --mount "type=bind,src=$GO_MODCACHE,dst=/host-gomodcache,readonly"
  --mount "type=bind,src=$DOCKER_BIN,dst=$DOCKER_BIN,readonly"
  --mount "type=bind,src=$GIT_BIN,dst=/host-bin/git,readonly"
  --mount "type=bind,src=$DOCKER_SOCKET,dst=/var/run/docker.sock"
  --mount "type=bind,src=$SCRATCH,dst=/pg-scratch"
  --mount "type=bind,src=$RUN_DIR,dst=/pg-evidence"
  --mount "type=bind,src=/usr/lib/git-core,dst=/host-git-core,readonly"
  --mount "type=bind,src=/usr/share/git-core/templates,dst=/host-git-templates,readonly"
  -e 'PATH=/usr/lib/postgresql/18/bin:/host-go/bin:/host-bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
  -e 'GOROOT=/host-go' -e 'GOMODCACHE=/host-gomodcache'
  -e 'GIT_EXEC_PATH=/host-git-core' -e 'GIT_TEMPLATE_DIR=/host-git-templates'
  -e 'CGO_ENABLED=0' -e 'GOCACHE=/pg-scratch/gocache'
  -e 'TMPDIR=/pg-scratch/tmp' -e 'HOME=/pg-scratch/home'
  -e 'DOCKER_HOST=unix:///var/run/docker.sock'
  -e 'TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1'
  -e 'TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock'
  -e 'CI=true' -e 'TXHARBOR_REQUIRE_DOCKER=1'
  -e "TXHARBOR_TESTED_TREE_FINGERPRINT=$FINGERPRINT_BEFORE"
)
for path in "${GIT_LIBS[@]}"; do docker_args+=(--mount "type=bind,src=$path,dst=$path,readonly"); done

set +e
docker run "${docker_args[@]}" --entrypoint /bin/sh "$IMAGE" -ec '
  unset PGDATA PG_MAJOR PG_VERSION
  cd /workspace
  for tool in pg_restore pg_dump; do
    command -v "$tool" >/dev/null || { echo "NOT RUN: pinned PostgreSQL tool missing: $tool" >&2; exit 1; }
    bin="$(readlink -f "$(command -v "$tool")")"
    magic="$(od -An -N4 -tx1 "$bin" | tr -d " \n")"
    [ "$magic" = 7f454c46 ] || { echo "NOT RUN: $tool is not direct ELF: $bin" >&2; exit 1; }
  done
  pg_restore --version | grep -Eq "^pg_restore \(PostgreSQL\) 18\.6" || { echo "NOT RUN: pg_restore must be PostgreSQL 18.6" >&2; exit 1; }
  pg_dump --version | grep -Eq "^pg_dump \(PostgreSQL\) 18\.6" || { echo "NOT RUN: pg_dump must be PostgreSQL 18.6" >&2; exit 1; }
  go version
  git --version
  git -C /workspace rev-parse --show-toplevel >/dev/null
  git -C /workspace rev-parse HEAD >/dev/null
  docker info >/dev/null
  go_version="$(go version)"
  git_version="$(git --version)"
  pg_restore_version="$(pg_restore --version)"
  pg_dump_version="$(pg_dump --version)"
  printf "tools: %s; %s; %s; %s; network=runner host namespace + sibling testcontainers bridge; workspace=read-only; git metadata=real/intact\n" \
    "$go_version" "$git_version" "$pg_restore_version" "$pg_dump_version"
  printf "{\"go\":\"%s\",\"git\":\"%s\",\"pg_restore\":\"%s\",\"pg_dump\":\"%s\"}\n" \
    "$go_version" "$git_version" "$pg_restore_version" "$pg_dump_version" > /pg-evidence/tool-versions.json
  if [ "'$MODE'" = --smoke ]; then exit 0; fi
  if [ "'$MODE'" = --focused ]; then
    go test -tags=integration -count=1 -timeout=15m -v ./internal/app -run "^TestServeDurablePauseKeepsServiceAliveAndPaused$" &&
    go test -tags=integration -count=1 -timeout=5m -v ./internal/health -run "^TestReadyzFlipsAndRecoversWithRealDependencies$" &&
    go test -tags=integration -count=1 -timeout=10m -v ./internal/nonce -run "^TestNonceMigrationHistoryUntouched$" &&
    go test -tags=integration -count=1 -timeout=10m -v ./internal/signer -run "^TestSignerMigrationHistoryUntouched$" &&
    go test -tags=integration -count=1 -timeout=10m -v ./internal/withdrawal -run "^TestWithdrawalMigrationHistoryUntouched$"
    exit $?
  fi
  set +e
  go test -json -tags=integration -count=1 -timeout=30m ./... \
    > /pg-evidence/go-test.jsonl 2> /pg-evidence/go-test.stderr
  test_status=$?
  set -e
  cat /pg-evidence/go-test.jsonl
  cat /pg-evidence/go-test.stderr >&2
  exit "$test_status"
'
STATUS=$?
set -e
FINGERPRINT_AFTER="$(go run scripts/drillcoverage/check.go --fingerprint)"
STABLE=false
[[ "$FINGERPRINT_BEFORE" == "$FINGERPRINT_AFTER" ]] && STABLE=true
if [[ "$MODE" == full && -s "$RUN_DIR/go-test.jsonl" ]]; then
  python3 - "$RUN_DIR/go-test.jsonl" "$RUN_DIR/test-summary.json" <<'PY'
import json
import sys

events_path, summary_path = sys.argv[1:]
started = set()
terminal = {}
for raw in open(events_path, encoding="utf-8"):
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        continue
    name = event.get("Test")
    package = event.get("Package")
    if not name or not package:
        continue
    key = (package, name)
    action = event.get("Action")
    if action == "run":
        started.add(key)
    elif action in {"pass", "fail", "skip"}:
        terminal[key] = action

counts = {"top_level": {k: 0 for k in ("PASS", "FAIL", "SKIP", "NOTRUN")},
          "subtest": {k: 0 for k in ("PASS", "FAIL", "SKIP", "NOTRUN")}}
for key in started | set(terminal):
    action = terminal.get(key, "notrun").upper()
    level = "subtest" if "/" in key[1] else "top_level"
    counts[level][action] += 1
with open(summary_path, "w", encoding="utf-8") as out:
    json.dump(counts, out, sort_keys=True)
    out.write("\n")
PY
fi
python3 - "$IMAGE_DIGEST" "$FINGERPRINT_BEFORE" "$FINGERPRINT_AFTER" "$STABLE" "$MODE" "$STATUS" "$RUN_DIR" <<'PY'
import json
import pathlib
import sys

digest, before, after, stable, mode, status, run_dir = sys.argv[1:]
run_dir = pathlib.Path(run_dir)
metadata = {
    "image_digest": digest,
    "source_fingerprint_before": before,
    "source_fingerprint_after": after,
    "source_tree_stable": stable == "true",
    "runner_network": "host",
    "testcontainers_host_override": "127.0.0.1",
    "workspace_mount": "read-only",
    "git_tree": "real intact .git mounted with workspace",
    "native_postgres_tools": "direct ELF PostgreSQL 18.6",
    "mode": mode,
    "exit_code": int(status),
}
versions_path = run_dir / "tool-versions.json"
if versions_path.exists():
    metadata["tool_versions"] = json.loads(versions_path.read_text(encoding="utf-8"))
summary_path = run_dir / "test-summary.json"
if summary_path.exists():
    metadata["test_counts"] = json.loads(summary_path.read_text(encoding="utf-8"))
(run_dir / "metadata.json").write_text(json.dumps(metadata, sort_keys=True) + "\n", encoding="utf-8")
PY
echo "PG integration evidence metadata: $RUN_DIR/metadata.json"
if [[ "$STATUS" -ne 0 && ! -s "$RUN_DIR/go-test.jsonl" ]]; then
  echo "NOT RUN: integration tests did not start or runner prerequisites failed (exit $STATUS)" >&2
fi
if [[ "$STABLE" != true ]]; then echo "source changed during integration run; result is not claimable" >&2; exit 1; fi
exit "$STATUS"
