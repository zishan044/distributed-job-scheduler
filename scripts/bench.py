#!/usr/bin/env python3
"""Collect benchmark evidence using the standard library and existing services."""
import argparse
import datetime
import gzip
import json
import math
import os
from pathlib import Path
import platform
import secrets
import shutil
import subprocess
import threading
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-p", "scheduler-bench", "-f", "docker-compose.yml",
           "-f", "docker-compose.bench.yml"]
QUERIES = {
    "created": 'sum(jobs_created_total{job="api"})',
    "succeeded": 'sum(executions_total{job="executor",result="succeeded"})',
    "ingest_rate": 'sum(rate(jobs_created_total{job="api"}[1m]))',
    "dispatch_rate": 'sum(rate(watcher_claimed_total{job="watcher"}[1m]))',
    "success_rate": 'sum(rate(executions_total{job="executor",result="succeeded"}[1m]))',
    "start_p99": 'histogram_quantile(0.99, sum by (le) (rate(start_latency_seconds_bucket{job="executor"}[1m])))',
    "lag": 'kafka_consumer_lag{job="api"}',
    "inflight": 'sum(inflight_jobs{job="executor"})',
    "workers": 'active_workers{job="api"}',
    "pool_saturation": 'pgxpool_acquired_conns / pgxpool_max_conns',
    "up": 'up{job=~"api|watcher|executor"}',
}


def settings(scenario):
    defaults = {"BENCH_RATE": 5000, "BENCH_DURATION": 600 if scenario == "soak" else 60,
                "BENCH_WARMUP": 30 if scenario == "soak" else 0, "BENCH_HERD_JOBS": 50000,
                "BENCH_HERD_LEAD": 30, "BENCH_VUS": 50, "BENCH_MAX_VUS": 200,
                "BENCH_DRAIN_TIMEOUT": 300}
    values = {key: int(os.environ.get(key, default)) for key, default in defaults.items()}
    if any(value < (0 if key == "BENCH_WARMUP" else 1) for key, value in values.items()):
        raise ValueError("Benchmark settings must be positive integers (warmup may be zero)")
    if values["BENCH_RATE"] % 100 or values["BENCH_HERD_JOBS"] % 100:
        raise ValueError("BENCH_RATE and BENCH_HERD_JOBS must be multiples of 100")
    if values["BENCH_MAX_VUS"] < values["BENCH_VUS"]:
        raise ValueError("BENCH_MAX_VUS must be at least BENCH_VUS")
    if scenario != "soak":
        values["BENCH_WARMUP"] = 0
    return values


def command(args, **kwargs):
    return subprocess.check_output(args, cwd=ROOT, text=True, timeout=150, **kwargs).strip()


def save(path, data):
    path.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n")


def redact(config):
    for service in config["services"].values():
        env = service.get("environment", {})
        for key, value in env.items():
            if any(word in key.upper() for word in ("PASSWORD", "SECRET", "TOKEN", "API_KEY")):
                env[key] = "<redacted>"
            elif key == "DATABASE_URL":
                url = urllib.parse.urlsplit(value)
                env[key] = urllib.parse.urlunsplit(url._replace(netloc="<redacted>@" + url.netloc.rsplit("@", 1)[-1]))
    return config


def prometheus(expression, start=None, end=None, query_time=None):
    args = {"query": expression}
    endpoint = "query"
    if start is not None:
        endpoint = "query_range"
        args.update(start=start, end=end, step=5)
    elif query_time is not None:
        args["time"] = query_time
    with urllib.request.urlopen("http://127.0.0.1:19090/api/v1/" + endpoint + "?" +
                                urllib.parse.urlencode(args), timeout=10) as response:
        data = json.load(response)
    if data["status"] != "success":
        raise RuntimeError("Prometheus query failed: " + expression)
    return data["data"]["result"]


def sql(filename, **variables):
    args = COMPOSE + ["exec", "-T", "postgres", "sh", "-c",
                      'exec psql -XqAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"',
                      "sh"]
    for key, value in variables.items():
        args += ["-v", key + "=" + str(value)]
    return command(args, input=(ROOT / "internal/store" / filename).read_text())


def metric(data, name, value="count"):
    return data.get("metrics", {}).get(name, {}).get("values", {}).get(value, 0)


def api_request(method, path, key, body=None):
    request = urllib.request.Request("http://127.0.0.1:18080" + path, method=method,
                                    data=json.dumps(body).encode() if body is not None else None,
                                    headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def stop_load(control_id, key):
    if control_id:
        api_request("POST", "/v1/jobs/" + control_id + "/pause", key)
        # An iteration can still be in its 5s control lookup before a 10s POST.
        time.sleep(16)
    command(["docker", "kill", "--signal", "SIGINT", "scheduler-bench-generator"])


def growing_backlog(samples):
    # ponytail: one-batch noise floor; inspect saved averages for slower drift.
    return len(samples) >= 4 and all(b["backlog"] - a["backlog"] >= 100
                                   for a, b in zip(samples[-4:], samples[-3:]))


def execution_counter_rate(series, start, end):
    """Use scrape-aligned boundaries within the load window; fresh stack, no resets."""
    if len(series) != 1:
        return None
    points = [(at, float(value)) for at, value in series[0]["values"]
              if start <= at <= end and math.isfinite(float(value))]
    if len(points) < 2 or any(b[1] < a[1] for a, b in zip(points, points[1:])):
        return None
    elapsed = points[-1][0] - points[0][0]
    if elapsed <= 0:
        return None
    successes = points[-1][1] - points[0][1]
    return {"source": QUERIES["succeeded"], "window_start": points[0][0],
            "window_end": points[-1][0], "window_seconds": elapsed,
            "samples": len(points), "successful_executions": successes,
            "successful_jobs_per_second": successes / elapsed}


def summarize(data, db, scenario):
    accepted = int(metric(data, "accepted_jobs"))
    measured = int(metric(data, "measurement_accepted_jobs"))
    start = metric(data, "measurement_start_seconds", "min")
    end = metric(data, "measurement_end_seconds", "max")
    due = metric(data, "herd_due_seconds", "min")
    if scenario == "herd":
        start, end = due, db["last_finished"] or due
    elif not data["benchmark"].get("aborted_for_overload"):
        end = max(end, start + data["benchmark"].get("duration", 0))
    elapsed = end - start
    errors = []
    if accepted <= 0 or not start or elapsed <= 0:
        errors.append("No complete measurement window or accepted jobs")
    if any(db[key] != accepted for key in ("jobs", "runs", "jobs_with_runs")):
        errors.append("Accepted jobs, jobs, and one initial run per job do not reconcile")
    expected_attempts = 0 if scenario == "ingest" else accepted
    if db["attempt_rows"] != expected_attempts or db["invalid_runs"] or db["invalid_attempts"]:
        errors.append("Run states, timestamps, or successful attempt history do not reconcile")
    if scenario == "herd" and (metric(data, "seed_end_seconds", "max") >= due or
                              abs((db["due_min"] or 0) - due) > 0.001 or
                              abs((db["due_max"] or 0) - due) > 0.001 or
                              accepted != data["benchmark"]["herd_jobs"]):
        errors.append("Herd was not completely seeded before one common due timestamp")
    dropped = int(metric(data, "dropped_iterations"))
    rejected = int(metric(data, "rejected_batches"))
    rates = {"ingest_jobs_per_second": None, "dispatch_jobs_per_second": None,
             "successful_jobs_per_second": None}
    if elapsed > 0:
        if scenario == "ingest":
            rates["ingest_jobs_per_second"] = measured / elapsed
        elif scenario == "herd":
            dispatch_time = (db["last_queued"] or due) - due
            if dispatch_time > 0:
                rates["dispatch_jobs_per_second"] = accepted / dispatch_time
            rates["successful_jobs_per_second"] = db["statuses"].get("SUCCEEDED", 0) / elapsed
        else:
            rates = {"ingest_jobs_per_second": measured / elapsed,
                     "dispatch_jobs_per_second": db["dispatches_in_window"] / elapsed,
                     "successful_jobs_per_second": db["successes_in_window"] / elapsed}
    return {"scenario": scenario, "accepted_jobs": accepted, "measurement_accepted_jobs": measured,
            "offered_jobs_per_second": data["benchmark"]["rate"] if scenario != "herd" else None,
            "window_start": start, "window_end": end, "window_seconds": elapsed,
            "dropped_iterations": dropped, "rejected_batches": rejected,
            "reconciled": not errors, "errors": errors,
            "load_generation_complete": not dropped and not rejected,
            "rates": rates, "start_latency_seconds": db["start_latency_seconds"],
            "drain_seconds": max(0, (db["last_finished"] or end) -
                                 (due if scenario == "herd" else metric(data, "measurement_end_seconds", "max")))
                                 if scenario != "ingest" else 0}


def sample(stop, path):
    with gzip.open(path, "wt") as output:
        while not stop.is_set():
            row = {"time": time.time()}
            try:
                ids = subprocess.check_output(
                    ["docker", "ps", "--filter", "label=com.docker.compose.project=scheduler-bench",
                     "--format", "{{.ID}}"], text=True, timeout=5).splitlines()
                if not ids:
                    raise RuntimeError("No running benchmark containers")
                text = subprocess.check_output(
                    ["docker", "stats", "--no-stream", "--format", "{{json .}}"] + ids,
                    text=True, timeout=15)
                row["containers"] = [json.loads(line) for line in text.splitlines()]
                row["counters"] = {key: prometheus(QUERIES[key]) for key in ("created", "succeeded")}
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                row["error"] = str(error)
            output.write(json.dumps(row) + "\n")
            output.flush()
            stop.wait(5)


def run(profile, scenario, values):
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(3)
    directory = ROOT / "docs/bench/results" / (run_id + "-" + profile + "-" + scenario)
    directory.mkdir(parents=True)
    os.environ["BENCH_RESULTS_DIR"] = str(directory)
    config = json.loads(command(COMPOSE + ["--profile", "tools", "config", "--format", "json"]))
    api_key = config["services"]["api"]["environment"]["API_KEY"]
    save(directory / "compose.json", redact(config))
    control_id = None
    if scenario == "soak":
        control = api_request("POST", "/v1/jobs", api_key, {
            "name": "m7-control-" + run_id, "type": "handler", "payload": {"handler": "noop"},
            "cron_expr": "0 0 1 1 *",
            "next_fire_at": (datetime.datetime.now(datetime.timezone.utc) +
                             datetime.timedelta(days=7)).isoformat()})
        control_id = control["id"]
    save(directory / "environment.json", {
        "profile": profile, "settings": values, "run_id": run_id,
        "platform": platform.platform(), "cpu": json.loads(command(["lscpu", "-J"])),
        "ram_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
        "disks": json.loads(command(["lsblk", "-J", "-o", "NAME,MODEL,ROTA,SIZE,MOUNTPOINTS"])),
        "git_commit": command(["git", "rev-parse", "HEAD"]),
        "git_status": command(["git", "status", "--short"]),
        "generator_shares_host": True, "control_job_id": control_id,
    })
    stop = threading.Event()
    sampler = threading.Thread(target=sample, args=(stop, directory / "resources.jsonl.gz"), daemon=True)
    sampler.start()
    began = time.time()
    process = None
    try:
        # Give discovery/scraping a full interval before recording the baseline.
        time.sleep(10)
        before = {key: prometheus(expression) for key, expression in QUERIES.items()}
        save(directory / "prometheus-before.json", before)
        args = COMPOSE + ["run", "--rm", "--no-deps", "-T", "--name", "scheduler-bench-generator", "--user",
                          str(os.getuid()) + ":" + str(os.getgid()), "-e", "BENCH_SCENARIO=" + scenario,
                          "-e", "BENCH_RUN_ID=" + run_id]
        if control_id:
            args += ["-e", "BENCH_CONTROL_ID=" + control_id]
        for key, value in values.items():
            args += ["-e", key + "=" + str(value)]
        args += ["k6", "run", "--out", "json=/results/k6.json.gz", "/scripts/workload.js"]
        print("Evidence: " + str(directory), flush=True)
        samples, window, overloaded = [], [], False
        with (directory / "k6.log").open("w") as log:
            process = subprocess.Popen(args, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
            next_window = time.monotonic() + values["BENCH_WARMUP"]
            while process.poll() is None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    if scenario == "soak" and not overloaded:
                        window.append(int(sql("bench_pending.sql")))
                        if time.monotonic() >= next_window:
                            samples.append(dict(time=time.time(), source="postgres",
                                                backlog=sum(window) / len(window), samples=len(window),
                                                minimum=min(window), maximum=max(window)))
                            save(directory / "backlog.json", samples)
                            with (directory / "diagnostics.log").open("a") as diagnostics:
                                diagnostics.write(sql("bench_diagnostics.sql",
                                                      batch=int(os.environ.get("WATCHER_BATCH_SIZE", 1000))) + "\n")
                            if growing_backlog(samples):
                                overloaded = True
                                print("Backlog grew across three 30-second windows; stopping offered load.", flush=True)
                                stop_load(control_id, api_key)
                            window = []
                            next_window = time.monotonic() + 30
            status = process.returncode
        if not (directory / "k6-summary.json").exists():
            raise RuntimeError("k6 exited with status " + str(status) +
                               " before writing its summary; see " + str(directory / "k6.log"))
        data = json.loads((directory / "k6-summary.json").read_text())
        data["benchmark"]["aborted_for_overload"] = overloaded
        drain_start = time.monotonic()
        pending = 0
        next_diagnostic = 0
        if scenario != "ingest":
            while True:
                pending = int(sql("bench_pending.sql"))
                if not pending or time.monotonic() - drain_start >= values["BENCH_DRAIN_TIMEOUT"]:
                    break
                if scenario == "herd" and time.monotonic() >= next_diagnostic:
                    with (directory / "diagnostics.log").open("a") as diagnostics:
                        diagnostics.write(sql("bench_diagnostics.sql",
                                              batch=int(os.environ.get("WATCHER_BATCH_SIZE", 1000))) + "\n")
                    next_diagnostic = time.monotonic() + 10
                print("Waiting for " + str(pending) + " unfinished runs", flush=True)
                time.sleep(5)
        drain_wait = time.monotonic() - drain_start
        start = metric(data, "measurement_start_seconds", "min")
        end = metric(data, "measurement_end_seconds", "max")
        if scenario != "herd" and not overloaded:
            end = max(end, start + values["BENCH_DURATION"])
        db = json.loads(sql("bench.sql", prefix="m7/" + run_id + "/", scenario=scenario,
                            window_start=start, window_end=end))
        save(directory / "reconciliation.json", db)
        summary = summarize(data, db, scenario)
        summary.update(profile=profile, k6_exit_code=status, unfinished_runs=pending,
                       drain_collection_seconds=time.monotonic() - drain_start,
                       aborted_for_overload=overloaded, completed_measurement=not overloaded)
        if pending:
            summary["drain_seconds"] = drain_wait
        if status and not overloaded:
            summary["errors"].append("k6 exited with status " + str(status))
            summary["reconciled"] = False
        # Capture final counter increments before services stop.
        time.sleep(10)
        finished = time.time()
        series = {key: prometheus(expression, began, finished) for key, expression in QUERIES.items()}
        save(directory / "prometheus.json", series)
        summary["execution_counter"] = execution_counter_rate(
            series["succeeded"], summary["window_start"], summary["window_end"])
        histogram = None
        if scenario != "ingest" and summary["window_seconds"] > 0:
            seconds = math.ceil(summary["window_seconds"])
            buckets = 'sum by (le) (increase(start_latency_seconds_bucket{job="executor"}[' + str(seconds) + 's]))'
            histogram = {key: prometheus(expression, query_time=summary["window_end"])
                         for key, expression in {"buckets": buckets,
                             "p50": "histogram_quantile(0.5, " + buckets + ")",
                             "p95": "histogram_quantile(0.95, " + buckets + ")",
                             "p99": "histogram_quantile(0.99, " + buckets + ")"}.items()}
        save(directory / "histogram.json", histogram)
        save(directory / "summary.json", summary)
        if os.environ.get("BENCH_SCREENSHOTS") == "1":
            from bench_screenshot import capture
            # Capture after measurement so browser work cannot affect the load.
            grafana = json.loads(command(COMPOSE + ["config", "--format", "json"]))["services"]["grafana"]["environment"]
            capture(directory, summary["window_start"] - 30, finished,
                    grafana["GF_SECURITY_ADMIN_USER"], grafana["GF_SECURITY_ADMIN_PASSWORD"])
        print(json.dumps(summary, indent=2), flush=True)
        return 0 if summary["reconciled"] else 1
    except Exception as error:
        save(directory / "error.json", {"error": str(error)})
        raise
    finally:
        if process is not None and process.poll() is None:
            try:
                stop_load(control_id, api_key)
            except (OSError, subprocess.SubprocessError):
                subprocess.run(["docker", "kill", "--signal", "SIGINT", "scheduler-bench-generator"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10, check=False)
            try:
                process.wait(timeout=35)
            except subprocess.TimeoutExpired:
                subprocess.run(["docker", "kill", "scheduler-bench-generator"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10, check=False)
        stop.set()
        sampler.join(timeout=40)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=("durable", "bench"))
    parser.add_argument("scenario", choices=("ingest", "herd", "soak"))
    parser.add_argument("--validate", action="store_true")
    args = parser.parse_args()
    try:
        values = settings(args.scenario)
        if os.environ.get("BENCH_SCREENSHOTS", "0") not in ("0", "1"):
            raise ValueError("BENCH_SCREENSHOTS must be 0 or 1")
        if os.environ.get("BENCH_SCREENSHOTS") == "1" and not (shutil.which("chromium") or shutil.which("google-chrome")):
            raise ValueError("BENCH_SCREENSHOTS requires Chromium or Google Chrome")
        return 0 if args.validate else run(args.profile, args.scenario, values)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(str(error), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
