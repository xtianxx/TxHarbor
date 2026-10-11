#!/usr/bin/env python3
"""Pure-stdlib helper for the log-filter mock suite.

Mirrors the small democtl surface the demo helpers use (`json get`,
`json count`, `uint256 hex`, `uint256 eq`) and adds `castlogs`: a SYNTHETIC
stand-in for `cast logs` that filters a fixture array by the topics it actually
receives (missing positions are wildcards, exactly like a real topic filter).

It never talks to a chain, a container, a daemon or the network, and it holds no
secrets: every fixture value is obviously synthetic.
"""
import json
import sys


def die(msg, code=1):
    print(msg, file=sys.stderr)
    sys.exit(code)


def split_seg(seg):
    if "[" in seg and seg.endswith("]"):
        name, _, idx = seg.partition("[")
        return name, int(idx[:-1])
    return seg, None


def lookup(value, path):
    if path in ("", "."):
        return value
    for seg in path.split("."):
        name, idx = split_seg(seg)
        if name:
            if not isinstance(value, dict) or name not in value:
                die("lookup %s: key not found" % name)
            value = value[name]
        if idx is not None:
            if not isinstance(value, list) or idx >= len(value):
                die("lookup [%d]: index out of range" % idx)
            value = value[idx]
    return value


def render(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    return json.dumps(value, separators=(",", ":"))


def load_stdin():
    try:
        return json.load(sys.stdin)
    except Exception as exc:  # noqa: BLE001 - any decode failure is fatal here
        die("decode: %s" % exc)


def parse_uint(text):
    return int(text, 16) if text.startswith("0x") else int(text)


def is_topic(text):
    return len(text) == 66 and text.startswith("0x") and all(
        c in "0123456789abcdefABCDEF" for c in text[2:])


# The demo's only event; kept as a literal so the stub needs no keccak.
SIG_TOPICS = {
    "Transfer(address,address,uint256)":
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
}


def castlogs(args):
    if not args:
        die("castlogs: fixture path required")
    fixture, rest = args[0], args[1:]
    addr = None
    positionals = []
    i = 0
    while i < len(rest):
        item = rest[i]
        if item == "--address":
            addr = rest[i + 1].lower()
            i += 2
            continue
        if item in ("--from-block", "--to-block"):
            i += 2
            continue
        positionals.append(item)
        i += 1
    topics = [None, None, None]
    if positionals and not is_topic(positionals[0]):
        # Signature form: v1.8.1 derives topic0 from the event and zips the
        # remaining arguments against the event's INDEXED inputs
        # (build_filter_event_sig). This event has no indexed input, so every
        # extra argument is dropped and topic1/topic2 stay wildcards — the
        # defect the raw-topic form avoids.
        topics[0] = SIG_TOPICS.get(positionals[0])
        if topics[0] is None:
            print("[]")
            return
    else:
        for slot, item in enumerate(positionals[:3]):
            topics[slot] = item.lower()
    logs = json.load(open(fixture))
    if isinstance(logs, dict):
        logs = logs["logs"]
    out = []
    for log in logs:
        if addr is not None and log.get("address", "").lower() != addr:
            continue
        log_topics = [t.lower() for t in log.get("topics", [])]
        if all(t is None or (len(log_topics) > n and log_topics[n] == t)
               for n, t in enumerate(topics)):
            out.append(log)
    print(json.dumps(out, separators=(",", ":")))


def main(argv):
    if not argv:
        die("usage")
    cmd = argv[0]
    if cmd == "json" and len(argv) >= 3 and argv[1] == "get":
        args = argv[2:]
        pretty = False
        if args and args[0] == "--pretty":
            pretty = True
            args = args[1:]
        if len(args) != 1:
            die("usage")
        value = lookup(load_stdin(), args[0])
        print(json.dumps(value, indent=2) if pretty else render(value))
        return
    if cmd == "json" and len(argv) == 3 and argv[1] == "count":
        value = lookup(load_stdin(), argv[2])
        if isinstance(value, (list, dict)):
            print(len(value))
            return
        die("count: %s is not an array or object" % argv[2])
    if cmd == "uint256" and len(argv) == 3 and argv[1] == "hex":
        print("0x%064x" % parse_uint(argv[2]))
        return
    if cmd == "uint256" and len(argv) == 4 and argv[1] in ("eq", "ge"):
        a, b = parse_uint(argv[2]), parse_uint(argv[3])
        ok = (a == b) if argv[1] == "eq" else (a >= b)
        print("ok" if ok else "cmp %d %s %d" % (a, argv[1], b))
        sys.exit(0 if ok else 1)
    if cmd == "castlogs":
        castlogs(argv[1:])
        return
    die("usage")


if __name__ == "__main__":
    main(sys.argv[1:])
