#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
profile=${1:-durable}
scenario=${2:-setup}
case "$profile" in
  durable)
    export BENCH_SYNCHRONOUS_COMMIT=on BENCH_SHARED_BUFFERS=128MB BENCH_MAX_WAL_SIZE=${BENCH_MAX_WAL_SIZE:-1GB}
    ;;
  bench)
    export BENCH_SYNCHRONOUS_COMMIT=off BENCH_SHARED_BUFFERS=512MB BENCH_MAX_WAL_SIZE=${BENCH_MAX_WAL_SIZE:-4GB}
    ;;
  *) echo "Profile must be durable or bench." >&2; exit 2 ;;
esac
case "$scenario" in
  setup) ;;
  ingest|herd|soak) python3 scripts/bench.py "$profile" "$scenario" --validate ;;
  *) echo "Scenario must be setup, ingest, herd or soak." >&2; exit 2 ;;
esac
for name in BENCH_WATCHERS BENCH_EXECUTORS BENCH_PARTITIONS BENCH_DB_POOL_MAX_CONNS; do
  value=${!name:-1}
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$name must be a positive integer." >&2
    exit 2
  fi
done
if [[ ! ${BENCH_AUTOVACUUM_NAPTIME:-1min} =~ ^[1-9][0-9]*(s|min|h)$ ]]; then
  echo "BENCH_AUTOVACUUM_NAPTIME must be a positive PostgreSQL duration." >&2
  exit 2
fi
if [[ ! $BENCH_MAX_WAL_SIZE =~ ^[1-9][0-9]*(MB|GB)$ ]]; then
  echo "BENCH_MAX_WAL_SIZE must be positive MB or GB." >&2
  exit 2
fi

dev=(docker compose -f docker-compose.yml)
bench=(docker compose -p scheduler-bench -f docker-compose.yml -f docker-compose.bench.yml)
dev_name=$("${dev[@]}" config | sed -n 's/^name: //p')
if [[ "$dev_name" == scheduler-bench ]]; then
  echo "Development project must differ from scheduler-bench." >&2
  exit 2
fi
"${bench[@]}" config --quiet
# Build and pull before stopping development services.
"${bench[@]}" build api watcher executor migrate
"${bench[@]}" pull --policy missing k6 postgres redis kafka topic-init echo prometheus grafana

running=$("${dev[@]}" ps -q --status running)
restore=()
if [[ -n "$running" ]]; then
  mapfile -t restore <<< "$running"
fi
cleanup() {
  status=$?
  trap - EXIT INT TERM
  "${bench[@]}" stop --timeout 40 || status=1
  if ((${#restore[@]})); then
    docker start "${restore[@]}" || status=1
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if ((${#restore[@]})); then
  docker stop --timeout 40 "${restore[@]}"
fi
# Only this fixed project's volumes are disposable. Keep them after the run
# for inspection; the next independent run starts clean.
"${bench[@]}" down --volumes --remove-orphans
echo "Benchmark profile: $profile (synchronous_commit=$BENCH_SYNCHRONOUS_COMMIT, fsync=on, max_wal_size=$BENCH_MAX_WAL_SIZE)"
"${bench[@]}" up -d --wait --wait-timeout 180
if [[ "$scenario" == setup ]]; then
  "${bench[@]}" run --rm --no-deps -T k6 run /scripts/setup.js
else
  python3 scripts/bench.py "$profile" "$scenario"
fi
