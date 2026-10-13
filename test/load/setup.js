// Subset 1 setup check, not a capacity measurement.
import http from 'k6/http';
import { check, fail, sleep } from 'k6';

export const options = {
  vus: 1,
  iterations: 1,
  thresholds: { checks: ['rate==1'] },
};

export default function () {
  const base = 'http://api:8080';
  const params = {
    headers: { Authorization: `Bearer ${__ENV.API_KEY}`, 'Content-Type': 'application/json' },
    timeout: '5s',
  };
  const jobs = Array.from({ length: 10 }, (_, i) => ({
    name: `m7-setup-${Date.now()}-${i}`,
    type: 'handler',
    payload: { handler: 'noop' },
    scheduled_for: new Date().toISOString(),
  }));
  const response = http.post(`${base}/v1/jobs/batch`, JSON.stringify({ jobs }), params);
  if (!check(response, { 'fresh batch accepted': (r) => r.status === 201 })) {
    fail(`Batch rejected: HTTP ${response.status}: ${response.body}`);
  }
  const created = response.json();
  if (!check(created, { 'ten jobs returned': (value) => Array.isArray(value) && value.length === 10 })) {
    fail('Unexpected batch response');
  }
  const pending = new Set(created.map((job) => job.id));
  const deadline = Date.now() + 60000;
  while (pending.size && Date.now() < deadline) {
    for (const id of pending) {
      const result = http.get(`${base}/v1/jobs/${id}/runs?limit=1`, params);
      if (result.status !== 200) fail(`Run lookup: HTTP ${result.status}`);
      const runs = result.json();
      if (runs.length === 1 && runs[0].status === 'SUCCEEDED' && runs[0].attempt === 1) {
        pending.delete(id);
      } else if (runs.some((run) => ['DEAD', 'CANCELLED', 'FAILED'].includes(run.status))) {
        fail(`Unexpected terminal outcome for ${id}`);
      }
    }
    if (pending.size) sleep(0.2);
  }
  if (!check(pending, { 'all ten runs succeeded on attempt one': (value) => value.size === 0 })) {
    fail(`${pending.size} runs did not finish within 60s`);
  }
}
