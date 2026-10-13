#!/usr/bin/env bash
# Integration check of quiescing and accounting; not a capacity measurement.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT
export BENCH_TEST_PYTHON
BENCH_TEST_PYTHON=$(command -v python3)
cat > "$tmp/python3" <<'WRAPPER'
#!/usr/bin/env bash
if [[ "$1" == scripts/bench.py && "$*" != *--validate* ]]; then
  exec "$BENCH_TEST_PYTHON" -c 'import json, sys; sys.path.insert(0, "scripts"); import bench; sys.argv = sys.argv[1:]; bench.growing_backlog = lambda samples: len(samples) >= 2; status = bench.main(); directory = max((bench.ROOT / "docs/bench/results").iterdir(), key=lambda p: p.name); (directory / "probe.json").write_text(json.dumps({"purpose": "forced graceful stop check", "guard": "forced at second window; not an overload measurement"}) + "\n"); sys.exit(status)' "$@"
fi
exec "$BENCH_TEST_PYTHON" "$@"
WRAPPER
chmod +x "$tmp/python3"
PATH="$tmp:$PATH" make bench BENCH_SCENARIO=soak BENCH_RATE=100 BENCH_DURATION=120 BENCH_WARMUP=0
