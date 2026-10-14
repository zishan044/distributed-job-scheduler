#!/usr/bin/env bash
# Check lifecycle failure paths without touching a Docker daemon.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT
suite_root="$tmp/project"
mkdir -p "$suite_root/scripts"
cp "$root/scripts/chaos.sh" "$suite_root/scripts/chaos.sh"
export CHAOS_TEST_LOG="$tmp/docker.log"
cat > "$tmp/docker" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$CHAOS_TEST_LOG"
case "$*" in
  'ps -a -q --filter label=com.docker.compose.project=scheduler-chaos')
    printf 'runner-id\npaused-id\nwatcher-id\nredis-id\n'
    if [[ ${CHAOS_TEST_FOREIGN:-0} == 1 ]]; then echo foreign-id; fi ;;
  'inspect --format '*runner-id)
    echo 'scheduler-chaos chaos-runner false no' ;;
  'inspect --format '*paused-id)
    echo 'scheduler-chaos executor true no' ;;
  'inspect --format '*watcher-id)
    echo 'scheduler-chaos watcher false no' ;;
  'inspect --format '*redis-id)
    echo 'scheduler-chaos redis false unless-stopped' ;;
  'inspect --format '*foreign-id)
    echo 'development executor true no' ;;
  *' build '*) exit "${CHAOS_TEST_BUILD_STATUS:-0}" ;;
  *' run '*)
    if [[ ${CHAOS_TEST_SIGNAL:-0} == 1 ]]; then kill -TERM "$PPID"; fi
    echo '{"Action":"pass","Test":"TestDuplicateDelivery"}'
    exit "${CHAOS_TEST_RUN_STATUS:-0}" ;;
esac
MOCK
chmod +x "$tmp/docker"
export PATH="$tmp:$PATH"

run() {
  : > "$CHAOS_TEST_LOG"
  result=0
  bash "$suite_root/scripts/chaos.sh" "$@" > "$tmp/output" 2>&1 || result=$?
}
contains() { rg -Fqx -- "$1" "$CHAOS_TEST_LOG"; }
restored() {
  contains 'stop --time 10 runner-id'
  contains 'unpause paused-id'
  contains 'update --restart=unless-stopped paused-id'
  contains 'start paused-id'
  contains 'update --restart=unless-stopped watcher-id'
  contains 'start watcher-id'
  contains 'start redis-id'
  ! contains 'unpause foreign-id'
  ! contains 'update --restart=unless-stopped foreign-id'
  contains 'compose -p scheduler-chaos -f docker-compose.yml -f docker-compose.chaos.yml logs --no-color --timestamps'
  contains 'compose -p scheduler-chaos -f docker-compose.yml -f docker-compose.chaos.yml down --volumes --remove-orphans'
  ! rg -q 'scheduler-bench|stop .*dev-' "$CHAOS_TEST_LOG"
}

run 1
[[ $result == 0 ]]
restored

export CHAOS_TEST_RUN_STATUS=27
run 1
[[ $result == 27 ]]
restored
unset CHAOS_TEST_RUN_STATUS

export CHAOS_TEST_SIGNAL=1
run 1
[[ $result == 143 ]]
restored
unset CHAOS_TEST_SIGNAL

export CHAOS_TEST_BUILD_STATUS=28
run 1
[[ $result == 28 ]]
restored
! rg -q ' run ' "$CHAOS_TEST_LOG"
unset CHAOS_TEST_BUILD_STATUS

# A foreign container deliberately returned by the fake discovery must fail closed.
export CHAOS_TEST_FOREIGN=1
run 1
[[ $result == 1 ]]
restored
unset CHAOS_TEST_FOREIGN

for count in 0 -1 abc 1000; do
  run "$count"
  [[ $result == 2 && ! -s "$CHAOS_TEST_LOG" ]]
done

# A held lock must stop the invocation before any Docker command.
exec 8>"$suite_root/docs/chaos/results/.lock"
flock -n 8
run 1
[[ $result == 2 && ! -s "$CHAOS_TEST_LOG" ]]
flock -u 8
echo 'PASS: scoped restoration, failure/SIGTERM/build cleanup, input validation, and serialization.'
