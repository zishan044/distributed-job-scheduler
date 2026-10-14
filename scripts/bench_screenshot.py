"""Capture the real Grafana dashboard with installed Chromium and stdlib CDP."""
import base64
import http.cookiejar
import json
import os
from pathlib import Path
import select
import shutil
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request


def capture(directory, start, end, username, password):
    browser = shutil.which("chromium") or shutil.which("google-chrome")
    if not browser:
        raise RuntimeError("BENCH_SCREENSHOTS requires Chromium or Google Chrome")
    # Authenticate over localhost; credentials never enter argv or artifacts.
    cookies = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookies))
    request = urllib.request.Request("http://127.0.0.1:13000/login",
                                     data=json.dumps({"user": username, "password": password}).encode(),
                                     headers={"Content-Type": "application/json"})
    with opener.open(request, timeout=10) as response:
        if json.load(response).get("message") != "Logged in":
            raise RuntimeError("Grafana login failed")
    if not any(cookie.name == "grafana_session" for cookie in cookies):
        raise RuntimeError("Grafana did not issue a session cookie")
    with tempfile.TemporaryDirectory(prefix="chromium-", dir=directory) as profile:
        profile = str(Path(profile).resolve())
        env = dict(os.environ, XDG_CACHE_HOME=profile + "/cache", XDG_CONFIG_HOME=profile + "/config")
        with (directory / "chromium.log").open("w") as log:
            # CDP uses null-terminated JSON on fd 3/4. The shell only maps pipes.
            process = subprocess.Popen(["sh", "-c", 'exec "$@" 3<&0 4>&1', "sh", browser,
                                        "--headless", "--disable-accelerated-2d-canvas", "--no-first-run",
                                        "--no-default-browser-check", "--disable-background-networking",
                                        "--remote-debugging-pipe", "--user-data-dir=" + profile],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, env=env)
            sequence, buffer = 0, b""

            def call(method, params=None, session=None):
                nonlocal sequence, buffer
                sequence += 1
                message = dict(id=sequence, method=method, params=params or {})
                if session:
                    message["sessionId"] = session
                process.stdin.write(json.dumps(message).encode() + b"\0")
                process.stdin.flush()
                deadline = time.monotonic() + 30
                while True:
                    while b"\0" in buffer:
                        raw, buffer = buffer.split(b"\0", 1)
                        result = json.loads(raw)
                        if result.get("id") == sequence:
                            if "error" in result:
                                raise RuntimeError("Chromium command failed: " + method)
                            return result["result"]
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not select.select([process.stdout], [], [], remaining)[0]:
                        raise RuntimeError("Chromium command timed out: " + method)
                    chunk = os.read(process.stdout.fileno(), 65536)
                    if not chunk:
                        raise RuntimeError("Chromium closed its debugging pipe")
                    buffer += chunk

            try:
                version = call("Browser.getVersion")["product"]
                target = call("Target.createTarget", {"url": "about:blank"})["targetId"]
                session = call("Target.attachToTarget", {"targetId": target, "flatten": True})["sessionId"]
                call("Network.enable", session=session)
                for cookie in cookies:
                    call("Network.setCookie", dict(name=cookie.name, value=cookie.value,
                                                   url="http://127.0.0.1:13000", httpOnly=True), session)
                call("Emulation.setDeviceMetricsOverride", dict(width=1800, height=1900,
                                                                deviceScaleFactor=1, mobile=False), session)
                call("Page.enable", session=session)
                url = "http://127.0.0.1:13000/d/job-scheduler?" + urllib.parse.urlencode({
                    "from": int(start * 1000), "to": int(end * 1000), "timezone": "utc"})
                call("Page.navigate", {"url": url}, session)
                call("Page.bringToFront", session=session)
                deadline = time.monotonic() + 45
                expression = "JSON.stringify({text:document.body.innerText,canvases:document.querySelectorAll('canvas').length})"
                while True:
                    state = call("Runtime.evaluate", {"expression": expression, "returnByValue": True}, session)
                    value = state.get("result", {}).get("value")
                    state = json.loads(value) if value else {"text": "", "canvases": 0}
                    if all(title in state["text"] for title in (
                            "Scheduler throughput", "DB pool saturation", "Start latency", "Kafka consumer-group lag")) and state["canvases"] >= 6:
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError("Grafana panels did not render; see chromium.log")
                    time.sleep(0.5)
                # Titles precede query completion; allow rendering after load.
                time.sleep(10)
                # Software canvas and view capture preserve headless chart layers.
                data = call("Page.captureScreenshot", {"format": "png", "fromSurface": False}, session)
                (directory / "grafana.png").write_bytes(base64.b64decode(data["data"]))
                (directory / "grafana.json").write_text(json.dumps({
                    "browser": version, "url": url, "window_start": start, "window_end": end,
                    "after_load": True, "panels": ["throughput", "start latency", "Kafka lag", "pool saturation"],
                }, indent=2) + "\n")
            finally:
                try:
                    call("Browser.close")
                except (OSError, RuntimeError):
                    process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
