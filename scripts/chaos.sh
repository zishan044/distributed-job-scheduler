#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
count=${1:-1}
if [[ ! "$count" =~ ^[1-9][0-9]*$ || ${#count} -gt 3 ]]; then
  echo 'CHAOS_COUNT must be an integer from 1 to 999.' >&2
  exit 2
fi

mkdir -p docs/chaos/results
exec 9>docs/chaos/results/.lock
if ! flock -n 9; then
  echo 'Another chaos invocation holds the scheduler-chaos lock.' >&2
  exit 2
fi
export CHAOS_RESULTS_DIR
CHAOS_RESULTS_DIR=$(mktemp -d "$root/docs/chaos/results/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")
compose=(docker compose -p scheduler-chaos -f docker-compose.yml -f docker-compose.chaos.yml)
"${compose[@]}" config --quiet

inspect_owned() {
  local labels project service paused
  labels=$(timeout 10 docker inspect --format '{{index .Config.Labels "com.docker.compose.project"}} {{index .Config.Labels "com.docker.compose.service"}} {{.State.Paused}}' "$1") || return 1
  read -r project service paused <<< "$labels"
  [[ $project == scheduler-chaos ]] || return 1
  case "$service" in
    chaos-runner|api|watcher|executor|postgres|redis|kafka|migrate|topic-init) ;;
    *) return 1 ;;
  esac
  printf '%s\n' "$labels"
}

cleanup() {
  status=$?
  trap - EXIT
  trap '' INT TERM
  set +e
  cleanup_failed() { if [[ $status == 0 ]]; then status=1; fi; }
  # Stop the runner first so its Go cleanups can finish before host restoration.
  ids=$(timeout 20 docker ps -a -q --filter label=com.docker.compose.project=scheduler-chaos)
  [[ $? == 0 ]] || cleanup_failed
  for id in $ids; do
    if ! labels=$(inspect_owned "$id"); then
      cleanup_failed
      continue
    fi
    read -r project service paused <<< "$labels"
    if [[ $service == chaos-runner ]]; then
      timeout 20 docker stop --time 10 "$id" >/dev/null || cleanup_failed
    fi
  done
  # --rm may remove the runner as soon as stop completes; refresh discovery.
  ids=$(timeout 20 docker ps -a -q --filter label=com.docker.compose.project=scheduler-chaos)
  [[ $? == 0 ]] || cleanup_failed
  for id in $ids; do
    if ! labels=$(inspect_owned "$id"); then
      cleanup_failed
      continue
    fi
    read -r project service paused <<< "$labels"
    if [[ $paused == true ]]; then
      timeout 15 docker unpause "$id" >> "$CHAOS_RESULTS_DIR/cleanup.log" 2>&1 || cleanup_failed
    fi
    timeout 10 docker inspect --format '{"id":{{json .Id}},"service":{{json (index .Config.Labels "com.docker.compose.service")}},"state":{{json .State}},"restart_count":{{.RestartCount}}}' "$id" >> "$CHAOS_RESULTS_DIR/containers.jsonl" || cleanup_failed
  done
  timeout 30 "${compose[@]}" logs --no-color --timestamps > "$CHAOS_RESULTS_DIR/compose.log" 2>&1 || cleanup_failed
  timeout 90 "${compose[@]}" down --volumes --remove-orphans >> "$CHAOS_RESULTS_DIR/cleanup.log" 2>&1 || cleanup_failed
  printf '{"exit_code":%d,"requested_passes":%d,"implemented_scenarios":["duplicate_delivery"]}\n' "$status" "$count" > "$CHAOS_RESULTS_DIR/result.json"
  echo "Chaos evidence: $CHAOS_RESULTS_DIR (exit $status)"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Reset only this reserved project, then build with tags distinct from dev/bench.
"${compose[@]}" down --volumes --remove-orphans
build_status=0
"${compose[@]}" build api watcher executor migrate chaos-runner > "$CHAOS_RESULTS_DIR/build.log" 2>&1 || build_status=$?
if [[ $build_status != 0 ]]; then
  tail -n 40 "$CHAOS_RESULTS_DIR/build.log"
  exit "$build_status"
fi
"${compose[@]}" up -d --wait --wait-timeout 180 api watcher executor redis

# Background + wait makes TERM/INT traps run while Docker is attached.
"${compose[@]}" run --rm --no-deps -T --use-aliases --name scheduler-chaos-runner chaos-runner \
  -test.v -test.failfast -test.count="$count" -test.timeout="$((count * 12 + 3))m" \
  > "$CHAOS_RESULTS_DIR/tests.jsonl" 2> "$CHAOS_RESULTS_DIR/runner.log" &
runner_pid=$!
status=0
wait "$runner_pid" || status=$?
cat "$CHAOS_RESULTS_DIR/tests.jsonl"
if [[ $status != 0 ]]; then cat "$CHAOS_RESULTS_DIR/runner.log"; fi
exit "$status"
