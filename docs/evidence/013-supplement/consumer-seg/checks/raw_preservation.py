#!/usr/bin/env python3
"""Read-only raw-evidence preservation audit for the 013 consumer-seg batch.

Baseline = the committed tree at BASELINE_REV (each file read via
`git show <rev>:<path>` and hashed with sha256); current = the working tree.
The audit FAILS (exit 1) if anything under the raw set — runs/, logs/,
environment.txt, source_fingerprint.txt, smoke/ — is added, removed or
modified; a rename counts as a removal plus an addition. Additions, changes
and removals outside the raw set (corrections, checks/, manifests) are
reported for the record and do not fail the audit. Read errors are never
swallowed: any failing git or file operation aborts the audit non-zero.

BASELINE_REV defaults to the batch commit and can be overridden with the
RAW_PRESERVATION_REV environment variable, which the isolated regression
fixture (checks/raw_preservation_test.sh) uses against a temporary git repo.

Usage (run from the repository root):
  python3 docs/evidence/013-supplement/consumer-seg/checks/raw_preservation.py \
      docs/evidence/013-supplement/consumer-seg
"""

import hashlib
import os
import subprocess
import sys

BASELINE_REV = os.environ.get(
    "RAW_PRESERVATION_REV", "27cc51d0801862f85c1ae04fd1fc74a4d300e80a"
)
RAW_PREFIXES = ("./runs/", "./logs/", "./environment.txt", "./source_fingerprint.txt", "./smoke/")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    root = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else ".")
    repo = subprocess.run(
        ["git", "-C", root, "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    rel_root = os.path.relpath(root, repo)

    tracked = subprocess.run(
        ["git", "-C", repo, "ls-tree", "-r", "--name-only", BASELINE_REV, "--", rel_root],
        capture_output=True, text=True, check=True,
    ).stdout.split()
    baseline = {}
    for path in tracked:
        blob = subprocess.run(["git", "-C", repo, "show", f"{BASELINE_REV}:{path}"],
                              capture_output=True, check=True).stdout
        baseline["./" + os.path.relpath(path, rel_root)] = sha256(blob)

    current = {}
    for dirpath, _, files in os.walk(root):
        for name in files:
            full = os.path.join(dirpath, name)
            rel = "./" + os.path.relpath(full, root)
            with open(full, "rb") as fh:
                current[rel] = sha256(fh.read())

    changed = sorted(p for p in baseline if p in current and baseline[p] != current[p])
    added = sorted(p for p in current if p not in baseline)
    removed = sorted(p for p in baseline if p not in current)
    raw_changed = [p for p in changed if p.startswith(RAW_PREFIXES)]
    raw_added = [p for p in added if p.startswith(RAW_PREFIXES)]
    raw_removed = [p for p in removed if p.startswith(RAW_PREFIXES)]
    violations = raw_changed + raw_added + raw_removed

    print(f"baseline revision: {BASELINE_REV}")
    print(f"baseline files: {len(baseline)}; current files: {len(current)}")
    print(f"changed ({len(changed)}):")
    for p in changed:
        print(f"  {p}")
    print(f"added ({len(added)}):")
    for p in added:
        print(f"  {p}")
    print(f"removed ({len(removed)}): {removed}")
    print("raw set (runs/, logs/, environment.txt, source_fingerprint.txt, smoke/):")
    print("  changed: " + (", ".join(raw_changed) if raw_changed else "NONE"))
    print("  added:   " + (", ".join(raw_added) if raw_added else "NONE"))
    print("  removed: " + (", ".join(raw_removed) if raw_removed else "NONE"))
    print("VERDICT: " + ("raw evidence byte-identical" if not violations
                         else "RAW EVIDENCE CHANGED - INVESTIGATE"))
    return 0 if not violations else 1


if __name__ == "__main__":
    sys.exit(main())
