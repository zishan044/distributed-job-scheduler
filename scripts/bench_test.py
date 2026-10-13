#!/usr/bin/env python3
import copy
import os
import unittest
from unittest.mock import patch

import bench


class MeasurementTest(unittest.TestCase):
    def fixture(self):
        data = {"metrics": {key: {"values": values} for key, values in {
            "accepted_jobs": {"count": 1000},
            "measurement_accepted_jobs": {"count": 800},
            "measurement_start_seconds": {"min": 100},
            "measurement_end_seconds": {"max": 110},
            "herd_due_seconds": {"min": 100},
            "seed_end_seconds": {"max": 99},
        }.items()}, "benchmark": {"rate": 100, "herd_jobs": 1000}}
        db = dict(jobs=1000, runs=1000, jobs_with_runs=1000, attempt_rows=1000,
                  invalid_runs=0, invalid_attempts=0, due_min=100, due_max=100,
                  last_queued=102, last_finished=110, dispatches_in_window=900,
                  successes_in_window=700, statuses={"SUCCEEDED": 1000},
                  start_latency_seconds={"p99": 0.5})
        return data, db

    def test_windows_and_job_rates(self):
        data, db = self.fixture()
        result = bench.summarize(data, db, "soak")
        self.assertTrue(result["reconciled"])
        self.assertEqual(result["rates"]["ingest_jobs_per_second"], 80)
        self.assertEqual(result["rates"]["dispatch_jobs_per_second"], 90)
        self.assertEqual(result["rates"]["successful_jobs_per_second"], 70)
        herd = bench.summarize(data, db, "herd")
        self.assertEqual(herd["rates"]["dispatch_jobs_per_second"], 500)
        self.assertEqual(herd["rates"]["successful_jobs_per_second"], 100)
        db["attempt_rows"] = 0
        db["statuses"] = {"PENDING": 1000}
        self.assertTrue(bench.summarize(data, db, "ingest")["reconciled"])
        series = [{"values": [[95, "0"], [100, "100"], [105, "600"], [110, "1100"], [115, "1200"]]}]
        counter = bench.execution_counter_rate(series, 100, 110)
        self.assertEqual(counter["successful_jobs_per_second"], 100)
        self.assertEqual(counter["window_seconds"], 10)
        self.assertIsNone(bench.execution_counter_rate(series, 101, 109))
        self.assertIsNone(bench.execution_counter_rate([{"values": [[100, "100"], [105, "0"]]}], 100, 110))

    def test_integrity_failure_and_drops(self):
        data, db = self.fixture()
        for key in ("jobs", "runs", "jobs_with_runs", "attempt_rows"):
            broken = dict(db, **{key: 999})
            self.assertFalse(bench.summarize(data, broken, "soak")["reconciled"], key)
        for key in ("invalid_runs", "invalid_attempts"):
            self.assertFalse(bench.summarize(data, dict(db, **{key: 1}), "soak")["reconciled"])
        data["metrics"]["dropped_iterations"] = {"values": {"count": 2}}
        result = bench.summarize(data, db, "soak")
        self.assertTrue(result["reconciled"])
        self.assertFalse(result["load_generation_complete"])
        data["metrics"]["seed_end_seconds"]["values"]["max"] = 100
        self.assertFalse(bench.summarize(data, db, "herd")["reconciled"])

    def test_validation_and_redaction(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(bench.settings("soak")["BENCH_DURATION"], 600)
            self.assertEqual(bench.settings("ingest")["BENCH_WARMUP"], 0)
            for key, value in {"BENCH_RATE": "101", "BENCH_DURATION": "0", "BENCH_VUS": "201"}.items():
                with patch.dict(os.environ, {key: value}):
                    with self.assertRaises(ValueError):
                        bench.settings("soak")
        config = {"services": {"api": {"environment": {
            "API_KEY": "private", "POSTGRES_PASSWORD": "private",
            "DATABASE_URL": "postgres://user:private@postgres:5432/scheduler?pool_max_conns=12"}}}}
        result = str(bench.redact(copy.deepcopy(config)))
        self.assertNotIn("private", result)
        self.assertIn("pool_max_conns=12", result)

    def test_backlog_and_interrupted_window(self):
        self.assertTrue(bench.growing_backlog([{"backlog": n} for n in (0, 100, 200, 300)]))
        self.assertFalse(bench.growing_backlog([{"backlog": n} for n in (100, 100, 90, 100)]))
        self.assertFalse(bench.growing_backlog([{"backlog": n} for n in (0, 100, 200)]))
        self.assertFalse(bench.growing_backlog([{"backlog": n} for n in (8859, 8872, 8992, 9028)]))
        data, db = self.fixture()
        data["benchmark"].update(duration=600, aborted_for_overload=True)
        self.assertEqual(bench.summarize(data, db, "soak")["window_seconds"], 10)
        data["benchmark"]["aborted_for_overload"] = False
        self.assertEqual(bench.summarize(data, db, "soak")["window_seconds"], 600)
        data["benchmark"]["aborted_for_overload"] = True
        db["last_finished"] = 112
        self.assertEqual(bench.summarize(data, db, "soak")["drain_seconds"], 2)


if __name__ == "__main__":
    unittest.main()
