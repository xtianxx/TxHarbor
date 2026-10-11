#!/usr/bin/env bash
# TxHarbor portfolio demo: verify/fetch the pinned official solc and compile the
# MinimalERC20 fixture.
#
# Modes:
#   --fetch     one-time network step: download the pinned official solc 0.8.30
#               (linux-amd64) into the isolated tools dir and verify its sha256
#               against the official list.json entry.
#   --compile   (default) compile fixtures/compile-input.json with the pinned
#               solc and write ABI/creation/runtime + PROVENANCE.md. Never
#               downloads.
#   --verify    recompute every artifact hash, compare against PROVENANCE.md and
#               check the embedded source copy; never downloads, never compiles.
#
# The compiler is never installed system-wide and never becomes a product
# dependency; it stays in $TXHARBOR_DEMO_TOOLS_DIR (default below).
set -euo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
FIXTURES="$SCRIPT_DIR/fixtures"
TOOLS_DIR="${TXHARBOR_DEMO_TOOLS_DIR:-/tmp/opencode/txharbor-demo-tools}"

SOLC_VERSION="0.8.30"
SOLC_LONG_VERSION="0.8.30+commit.73712a01"
SOLC_BINARY_NAME="solc-linux-amd64-v0.8.30+commit.73712a01"
SOLC_SHA256="f3e987dc6ecebd4bd350c48edcbc320b46cf9e3109bd3fc3d88f1acaf4c428f7"
MANIFEST_URL="https://binaries.soliditylang.org/linux-amd64/list.json"
BINARY_URL="https://binaries.soliditylang.org/linux-amd64/$SOLC_BINARY_NAME"

log() { printf '%s\n' "$*"; }
die() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

solc_bin() { printf '%s\n' "$TOOLS_DIR/solc-$SOLC_VERSION"; }

verify_solc() {
  local bin; bin="$(solc_bin)"
  [ -x "$bin" ] || die "pinned solc not present at $bin (run: $0 --fetch)"
  local actual; actual="$(sha256sum "$bin" | cut -d' ' -f1)"
  [ "$actual" = "$SOLC_SHA256" ] || die "solc sha256 mismatch: $actual != $SOLC_SHA256"
  "$bin" --version | grep -Fq "Version: $SOLC_LONG_VERSION" \
    || die "solc --version does not report $SOLC_LONG_VERSION"
}

democtl_bin() {
  local dir="$TOOLS_DIR/build/helper.$$"
  local bin="$dir/democtl"
  [ ! -L "$TOOLS_DIR/build" ] || die "refusing symlinked build dir: $TOOLS_DIR/build"
  [ ! -L "$dir" ] || die "refusing symlinked build dir: $dir"
  [ ! -L "$bin" ] || die "refusing symlinked democtl target: $bin"
  # Always build the current source: a pre-existing binary is never reused.
  mkdir -p "$dir"
  ( cd "$REPO_ROOT" && go build -o "$bin" ./scripts/portfolio-demo/democtl ) \
    || die "democtl build failed"
  printf '%s\n' "$bin"
}

fetch_solc() {
  command -v curl >/dev/null || die "curl is required for --fetch"
  mkdir -p "$TOOLS_DIR"
  local manifest="$TOOLS_DIR/solc-list.json"
  log "fetching $MANIFEST_URL"
  curl --disable --noproxy '*' --connect-timeout 15 -fsS --max-time 120 -o "$manifest" "$MANIFEST_URL" || die "manifest download failed"
  local manifest_sha; manifest_sha="$(sha256sum "$manifest" | cut -d' ' -f1)"
  local democtl; democtl="$(democtl_bin)"
  local entry; entry="$("$democtl" solc-manifest "$SOLC_VERSION" < "$manifest")" \
    || die "manifest lookup failed for $SOLC_VERSION"
  local path long sha
  IFS=$'\t' read -r path long sha <<<"$entry"
  [ "$path" = "$SOLC_BINARY_NAME" ] || die "manifest path mismatch: $path"
  [ "$long" = "$SOLC_LONG_VERSION" ] || die "manifest longVersion mismatch: $long"
  [ "${sha#0x}" = "$SOLC_SHA256" ] || die "manifest sha256 mismatch: $sha"
  log "manifest ok (list.json sha256=$manifest_sha)"
  log "fetching $BINARY_URL"
  curl --disable --noproxy '*' --connect-timeout 15 -fsS --max-time 600 -o "$(solc_bin)" "$BINARY_URL" || die "solc binary download failed"
  chmod 700 "$(solc_bin)"
  verify_solc
  printf 'url=%s\nmanifest_url=%s\nmanifest_sha256=%s\nbinary_sha256=%s\nlong_version=%s\nfetched_at=%s\n' \
    "$BINARY_URL" "$MANIFEST_URL" "$manifest_sha" "$SOLC_SHA256" "$SOLC_LONG_VERSION" \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$TOOLS_DIR/solc-$SOLC_VERSION.provenance.txt"
  log "solc $SOLC_LONG_VERSION verified at $(solc_bin)"
}

write_provenance() {
  local manifest_note="unknown (run --fetch to record)"
  local pfile="$TOOLS_DIR/solc-$SOLC_VERSION.provenance.txt"
  if [ -f "$pfile" ]; then
    manifest_note="$(sed -n 's/^manifest_sha256=//p' "$pfile" | head -n1)"
    [ -n "$manifest_note" ] || manifest_note="$(sed -n 's/^list_json_sha256=//p' "$pfile" | head -n1)"
  fi
  {
    printf '# Provenance: MinimalERC20 demo fixture (NOT FOR PRODUCTION)\n\n'
    printf 'solc_version=%s\n' "$SOLC_LONG_VERSION"
    printf 'solc_url=%s\n' "$BINARY_URL"
    printf 'solc_sha256=%s\n' "$SOLC_SHA256"
    printf 'solc_manifest_url=%s\n' "$MANIFEST_URL"
    printf 'solc_manifest_sha256=%s\n' "$manifest_note"
    printf 'settings_evm_version=cancun\n'
    printf 'settings_optimizer=false\n'
    printf 'settings_via_ir=false\n'
    printf 'settings_metadata_bytecode_hash=none\n'
    printf 'settings_metadata_append_cbor=false\n'
    printf 'compile_command=solc --standard-json < compile-input.json > output.json\n'
    printf 'sha256_source=%s\n' "$(sha256sum "$FIXTURES/MinimalERC20.sol" | cut -d' ' -f1)"
    printf 'sha256_compile_input=%s\n' "$(sha256sum "$FIXTURES/compile-input.json" | cut -d' ' -f1)"
    printf 'sha256_abi=%s\n' "$(sha256sum "$FIXTURES/MinimalERC20.abi.json" | cut -d' ' -f1)"
    printf 'sha256_creation=%s\n' "$(sha256sum "$FIXTURES/MinimalERC20.creation.bin" | cut -d' ' -f1)"
    printf 'sha256_runtime=%s\n' "$(sha256sum "$FIXTURES/MinimalERC20.runtime.bin" | cut -d' ' -f1)"
    printf 'note=solc binary sha256 verified against the official binaries.soliditylang.org list.json entry; the manifest authenticity itself (TLS/domain only) is not independently cross-checked.\n'
  } > "$FIXTURES/PROVENANCE.md"
}

compile_fixture() {
  verify_solc
  local democtl; democtl="$(democtl_bin)"
  [ -f "$FIXTURES/compile-input.json" ] || die "missing compile-input.json"
  # The compile temp dir is kept (never deleted): the raw solc output stays
  # inspectable under the approved tools dir.
  mkdir -p "$TOOLS_DIR/build"
  local tmp; tmp="$(mktemp -d "$TOOLS_DIR/build/compile.XXXXXX")"
  # solc --standard-json can exit 0 while reporting errors, so the JSON
  # errors[].severity is the authoritative signal (never the exit code); a
  # parse failure in solc-check is a failure too, never a clean compile.
  "$(solc_bin)" --standard-json < "$FIXTURES/compile-input.json" > "$tmp/output.json" \
    || die "solc invocation failed"
  "$democtl" solc-check < "$tmp/output.json" || die "solc-check failed"
  "$democtl" json get --pretty contracts.MinimalERC20.MinimalERC20.abi \
    < "$tmp/output.json" > "$tmp/abi.json" || die "abi extraction failed"
  "$democtl" json get contracts.MinimalERC20.MinimalERC20.evm.bytecode.object \
    < "$tmp/output.json" > "$tmp/creation.bin" || die "creation extraction failed"
  "$democtl" json get contracts.MinimalERC20.MinimalERC20.evm.deployedBytecode.object \
    < "$tmp/output.json" > "$tmp/runtime.bin" || die "runtime extraction failed"
  local f
  for f in creation runtime; do
    [ -s "$tmp/$f.bin" ] || die "$f bytecode is empty"
    grep -Eq '^[0-9a-f]+$' "$tmp/$f.bin" || die "$f bytecode is not plain hex"
  done
  local abi_count; abi_count="$("$democtl" json count . < "$tmp/abi.json")"
  [ "$abi_count" -ge 8 ] || die "abi too small ($abi_count entries)"
  install -m 600 "$tmp/abi.json" "$FIXTURES/MinimalERC20.abi.json"
  install -m 600 "$tmp/creation.bin" "$FIXTURES/MinimalERC20.creation.bin"
  install -m 600 "$tmp/runtime.bin" "$FIXTURES/MinimalERC20.runtime.bin"
  write_provenance
  verify_artifacts
  log "compiled MinimalERC20 with solc $SOLC_LONG_VERSION (cancun, optimizer off, no metadata)"
}

check_hash() {
  local label="$1" file="$2" want got
  want="$(sed -n "s/^sha256_${label}=//p" "$FIXTURES/PROVENANCE.md" | head -n1)"
  [ -n "$want" ] || die "PROVENANCE missing sha256_${label}"
  got="$(sha256sum "$file" | cut -d' ' -f1)"
  [ "$got" = "$want" ] || die "sha256 mismatch for $label: $got != $want"
}

verify_artifacts() {
  local democtl; democtl="$(democtl_bin)"
  [ -f "$FIXTURES/PROVENANCE.md" ] || die "missing PROVENANCE.md"
  check_hash source "$FIXTURES/MinimalERC20.sol"
  check_hash compile_input "$FIXTURES/compile-input.json"
  check_hash abi "$FIXTURES/MinimalERC20.abi.json"
  check_hash creation "$FIXTURES/MinimalERC20.creation.bin"
  check_hash runtime "$FIXTURES/MinimalERC20.runtime.bin"
  local embedded source_text
  embedded="$("$democtl" json get sources.MinimalERC20.content < "$FIXTURES/compile-input.json")"
  source_text="$(cat "$FIXTURES/MinimalERC20.sol")"
  [ "$embedded" = "$source_text" ] || die "compile-input source copy differs from MinimalERC20.sol"
  local name
  for name in totalSupply balanceOf transfer allowance approve transferFrom Transfer Approval; do
    grep -Eq "\"name\": *\"$name\"" "$FIXTURES/MinimalERC20.abi.json" || die "ABI missing $name"
  done
  if grep -Eq '"name": *"mint"' "$FIXTURES/MinimalERC20.abi.json"; then
    die "ABI unexpectedly exposes a mint function"
  fi
  # Fixed compiler settings, checked as values (not just self-consistent
  # hashes) directly against the compile input.
  local expect
  for expect in \
    settings.evmVersion=cancun \
    settings.optimizer.enabled=false \
    settings.viaIR=false \
    settings.metadata.bytecodeHash=none \
    settings.metadata.appendCBOR=false
  do
    "$democtl" json require "${expect%%=*}" "${expect#*=}" < "$FIXTURES/compile-input.json" \
      || die "compile-input.json: ${expect%%=*} is not ${expect#*=}"
  done
  # Fixed compiler identity and settings, pinned exactly in PROVENANCE.md.
  for expect in \
    "solc_version=$SOLC_LONG_VERSION" \
    "solc_sha256=$SOLC_SHA256" \
    settings_evm_version=cancun \
    settings_optimizer=false \
    settings_via_ir=false \
    settings_metadata_bytecode_hash=none \
    settings_metadata_append_cbor=false
  do
    grep -Fxq "$expect" "$FIXTURES/PROVENANCE.md" || die "PROVENANCE.md missing: $expect"
  done
  local f
  for f in creation runtime; do
    [ -s "$FIXTURES/MinimalERC20.$f.bin" ] || die "$f.bin is empty"
    grep -Eq '^[0-9a-f]+$' "$FIXTURES/MinimalERC20.$f.bin" || die "$f.bin is not plain hex"
  done
  if [ -x "$(solc_bin)" ]; then verify_solc; else log "note: solc binary absent; compiler-hash check skipped"; fi
  log "artifact verification ok"
}

# H3: approve TOOLS_DIR before any mode acts. Only two roots are approved;
# only --verify may additionally use one runs/<run-id> child of an approved
# root. Every used component must be a non-symlink, uid-owned directory.
validate_tools_dir() {
  local mode="$1" root leaf="" run_id uid component parent
  local LC_ALL=C
  local roots=("/tmp/opencode/txharbor-demo-tools" "$REPO_ROOT/.evidence/portfolio-demo-tools")
  case "$mode" in
    fetch|compile|verify) ;;
    *) die "internal: unknown tools-dir mode: $mode" ;;
  esac
  case "$TOOLS_DIR" in
    *//*|*/) die "refusing tools dir with empty path components: $TOOLS_DIR" ;;
    /*) ;;
    *) die "refusing relative tools dir: $TOOLS_DIR" ;;
  esac
  for root in "${roots[@]}"; do
    if [ "$TOOLS_DIR" = "$root" ]; then
      leaf="$root"
      break
    fi
    [ "$mode" = verify ] || continue
    case "$TOOLS_DIR" in
      "$root"/runs/*) ;;
      *) continue ;;
    esac
    # Component-exact child: run-id must match ^[a-z0-9][a-z0-9-]{0,63}$.
    run_id="${TOOLS_DIR#"$root"/runs/}"
    case "$run_id" in
      */*|'') die "refusing tools dir (want <root>/runs/<run-id>): $TOOLS_DIR" ;;
    esac
    [ "${#run_id}" -le 64 ] || die "refusing run-id longer than 64 chars: $run_id"
    case "$run_id" in
      -*|*[!a-z0-9-]*) die "refusing invalid run-id: $run_id" ;;
    esac
    leaf="$TOOLS_DIR"
    break
  done
  [ -n "$leaf" ] || die "refusing unapproved tools dir for --$mode: $TOOLS_DIR"
  uid="$(id -u)"
  # The root's parent must exist (or be created), be a real dir owned by us.
  parent="${root%/*}"
  [ ! -L "$parent" ] || die "refusing symlinked tools-dir parent: $parent"
  if [ ! -e "$parent" ]; then
    mkdir -m 700 -p "$parent" || die "cannot create tools-dir parent: $parent"
  fi
  [ ! -L "$parent" ] || die "refusing symlinked tools-dir parent: $parent"
  [ -d "$parent" ] || die "tools-dir parent is not a directory: $parent"
  [ "$(stat -c %u "$parent")" = "$uid" ] || die "tools-dir parent not owned by uid $uid: $parent"
  # Root and every component down to the leaf: real dir, current owner,
  # created 0700 when missing (an existing dir is never chmodded).
  local components=("$root")
  [ "$leaf" = "$root" ] || components+=("$root/runs" "$leaf")
  for component in "${components[@]}"; do
    [ ! -L "$component" ] || die "refusing symlinked tools-dir component: $component"
    if [ ! -e "$component" ]; then
      mkdir -m 700 -p "$component" || die "cannot create tools dir: $component"
    fi
    [ ! -L "$component" ] || die "refusing symlinked tools-dir component: $component"
    [ -d "$component" ] || die "tools-dir component is not a directory: $component"
    [ "$(stat -c %u "$component")" = "$uid" ] || die "tools-dir component not owned by uid $uid: $component"
  done
  # build/ and the per-pid democtl target must never be symlinks.
  [ ! -L "$leaf/build" ] || die "refusing symlinked build dir: $leaf/build"
  if [ -e "$leaf/build" ]; then
    [ -d "$leaf/build" ] || die "build path is not a directory: $leaf/build"
  fi
  [ ! -L "$leaf/build/helper.$$/democtl" ] || die "refusing symlinked democtl target: $leaf/build/helper.$$/democtl"
}

usage() {
  sed -n '2,18p' "$0"
}

case "${1:---compile}" in
  --fetch) validate_tools_dir fetch; fetch_solc ;;
  --compile) validate_tools_dir compile; compile_fixture ;;
  --verify) validate_tools_dir verify; verify_artifacts ;;
  -h|--help) usage ;;
  *) die "usage: $0 [--fetch|--compile|--verify]" ;;
esac
