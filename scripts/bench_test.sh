#!/usr/bin/env bash
# Exercise restoration without touching a Docker daemon.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT
export BENCH_TEST_LOG="$tmp/log"
cat > "$tmp/docker" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$BENCH_TEST_LOG"
case "$*" in
  'compose -f docker-compose.yml config') echo "name: ${BENCH_TEST_DEV_NAME:-development}" ;;
  'compose -f docker-compose.yml ps -q --status running')
    if [[ ${BENCH_TEST_EMPTY:-0} != 1 ]]; then printf 'dev-api\ndev-db\n'; fi ;;
  *' build '*) exit "${BENCH_TEST_BUILD_STATUS:-0}" ;;
  *' run '*)
    if [[ ${BENCH_TEST_SIGNAL:-0} == 1 ]]; then kill -TERM "$PPID"; fi
    exit "${BENCH_TEST_RUN_STATUS:-0}" ;;
esac
MOCK
chmod +x "$tmp/docker"
export PATH="$tmp:$PATH"
unset BENCH_WATCHERS BENCH_EXECUTORS BENCH_PARTITIONS BENCH_DB_POOL_MAX_CONNS
run() {
  : > "$BENCH_TEST_LOG"
  result=0
  bash "$root/scripts/bench.sh" "$@" > "$tmp/output" 2>&1 || result=$?
}
contains() { grep -Fqx -- "$1" "$BENCH_TEST_LOG"; }
run durable setup
[[ $result == 0 ]]
contains 'stop --timeout 40 dev-api dev-db'
contains 'start dev-api dev-db'
contains 'compose -p scheduler-bench -f docker-compose.yml -f docker-compose.bench.yml down --volumes --remove-orphans'
grep -Fq 'synchronous_commit=on, fsync=on' "$tmp/output"
run bench setup
[[ $result == 0 ]]
grep -Fq 'synchronous_commit=off, fsync=on' "$tmp/output"

export BENCH_TEST_RUN_STATUS=27
run durable setup
[[ $result == 27 ]]
contains 'start dev-api dev-db'
unset BENCH_TEST_RUN_STATUS
export BENCH_TEST_SIGNAL=1
run durable setup
[[ $result == 143 ]]
contains 'start dev-api dev-db'
unset BENCH_TEST_SIGNAL

export BENCH_TEST_BUILD_STATUS=28
run durable setup
[[ $result == 28 ]]
! contains 'stop --timeout 40 dev-api dev-db'
unset BENCH_TEST_BUILD_STATUS
export BENCH_TEST_EMPTY=1
run durable setup
[[ $result == 0 ]]
! contains 'start dev-api dev-db'
unset BENCH_TEST_EMPTY

export BENCH_TEST_DEV_NAME=scheduler-bench
run durable setup
[[ $result == 2 ]]
! contains 'compose -p scheduler-bench -f docker-compose.yml -f docker-compose.bench.yml down --volumes --remove-orphans'
unset BENCH_TEST_DEV_NAME
for args in 'unknown setup' 'durable soak'; do
  read -r -a values <<< "$args"
  run "${values[@]}"
  [[ $result == 2 && ! -s "$BENCH_TEST_LOG" ]]
done
export BENCH_PARTITIONS=0
run durable setup
[[ $result == 2 && ! -s "$BENCH_TEST_LOG" ]]
echo 'PASS: profiles, input validation, isolated reset, and restoration on success/failure/SIGTERM.'
