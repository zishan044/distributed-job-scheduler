// One-shot noop workloads. No SDK retries or per-job polling during load.
import http from 'k6/http';
import exec from 'k6/execution';
import { check, fail, sleep } from 'k6';
import { Counter, Gauge } from 'k6/metrics';

const scenario = __ENV.BENCH_SCENARIO;
const rate = Number(__ENV.BENCH_RATE || 5000);
const duration = Number(__ENV.BENCH_DURATION || (scenario === 'soak' ? 600 : 60));
const warmup = scenario === 'soak' ? Number(__ENV.BENCH_WARMUP || 30) : 0;
const herdJobs = Number(__ENV.BENCH_HERD_JOBS || 50000);
const lead = Number(__ENV.BENCH_HERD_LEAD || 30);
const vus = Number(__ENV.BENCH_VUS || 50);
const maxVus = Number(__ENV.BENCH_MAX_VUS || 200);
const prefix = 'm7/' + __ENV.BENCH_RUN_ID + '/';
if (!['ingest', 'herd', 'soak'].includes(scenario) || !__ENV.BENCH_RUN_ID) {
  throw new Error('BENCH_SCENARIO and BENCH_RUN_ID are required');
}
if (![rate, duration, herdJobs, lead, vus, maxVus].every((n) => Number.isSafeInteger(n) && n > 0) ||
    !Number.isSafeInteger(warmup) || warmup < 0 || rate % 100 || herdJobs % 100 || maxVus < vus) {
  throw new Error('Positive integer settings required; rate/herd jobs must be multiples of 100');
}

const accepted = new Counter('accepted_jobs');
const measuredAccepted = new Counter('measurement_accepted_jobs');
const rejected = new Counter('rejected_batches');
const windowStart = new Gauge('measurement_start_seconds');
const windowEnd = new Gauge('measurement_end_seconds');
const herdDue = new Gauge('herd_due_seconds');
const seedEnd = new Gauge('seed_end_seconds');
const arrival = (seconds, start = 0) => ({
  executor: 'constant-arrival-rate', rate: rate / 100, timeUnit: '1s',
  duration: seconds + 's', startTime: start + 's',
  preAllocatedVUs: vus, maxVUs: maxVus, gracefulStop: '30s',
});
export const options = {
  setupTimeout: '120s',
  scenarios: scenario === 'herd'
    ? { measure: { executor: 'shared-iterations', vus: 1, iterations: 1, maxDuration: (lead + 10) + 's' } }
    : {
      ...(warmup ? { warmup: arrival(warmup) } : {}),
      measure: arrival(duration, warmup),
    },
  thresholds: { checks: ['rate==1'] },
};

function submit(phase, at, suffix) {
  const jobs = Array.from({ length: 100 }, (_, i) => ({
    name: prefix + phase + '/' + suffix + '/' + i,
    type: 'handler', payload: { handler: 'noop' }, scheduled_for: at,
  }));
  const response = http.post('http://api:8080/v1/jobs/batch', JSON.stringify({ jobs }), {
    headers: { Authorization: 'Bearer ' + __ENV.API_KEY, 'Content-Type': 'application/json' },
    timeout: '10s', tags: { name: 'POST /v1/jobs/batch', phase },
  });
  let created;
  try { created = response.json(); } catch (_) { created = null; }
  const ok = check(response, {
    'fresh complete batch': (r) => r.status === 201 && Array.isArray(created) &&
      created.length === 100 && created.every((job) => typeof job.id === 'string'),
  });
  if (!ok) { rejected.add(1); return false; }
  accepted.add(100);
  if (phase === 'measure') measuredAccepted.add(100);
  return true;
}

export function setup() {
  accepted.add(0); measuredAccepted.add(0); rejected.add(0);
  const future = new Date(Date.now() + (duration + 30 + 300) * 1000).toISOString();
  if (scenario === 'herd') {
    const due = Date.now() + lead * 1000;
    herdDue.add(due / 1000);
    for (let i = 0; i < herdJobs / 100; i++) {
      if (Date.now() >= due || !submit('measure', new Date(due).toISOString(), 'seed-' + i)) {
        fail('Herd seeding failed or crossed the common due timestamp');
      }
    }
    seedEnd.add(Date.now() / 1000);
    if (Date.now() >= due) fail('Herd seeding finished too late');
    return { due, future };
  }
  return { future };
}

export default function (data) {
  if (scenario === 'herd') {
    windowStart.add(data.due / 1000);
    while (Date.now() < data.due) sleep(Math.min(1, (data.due - Date.now()) / 1000));
    windowEnd.add(Date.now() / 1000);
    return;
  }
  if (scenario === 'soak') {
    const control = http.get('http://api:8080/v1/jobs/' + __ENV.BENCH_CONTROL_ID, {
      headers: { Authorization: 'Bearer ' + __ENV.API_KEY }, timeout: '5s',
      tags: { name: 'GET benchmark control', phase: 'control' },
    });
    if (!check(control, { 'stop control readable': (r) => r.status === 200 })) {
      fail('Could not read benchmark stop control');
    }
    if (control.json().paused) return;
  }
  const phase = exec.scenario.name;
  if (phase === 'measure') windowStart.add(exec.scenario.startTime / 1000);
  submit(phase, scenario === 'ingest' ? data.future : new Date().toISOString(),
    exec.vu.idInTest + '-' + exec.scenario.iterationInTest);
  if (phase === 'measure') {
    windowEnd.add(Date.now() / 1000);
  }
}

export function handleSummary(data) {
  data.benchmark = { scenario, rate, duration, warmup, herd_jobs: herdJobs, lead, vus, max_vus: maxVus, prefix };
  return {
    '/results/k6-summary.json': JSON.stringify(data, null, 2),
    stdout: 'k6 workload finished; accepted jobs: ' + data.metrics.accepted_jobs.values.count + '\n',
  };
}
