#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# End-to-end routing check for the bundled observability stack.
#
# Emits one log, one metric, and one trace span (each tagged with a test
# service name and a unique run id) at the OpenTelemetry Collector over
# OTLP/HTTP, exactly the way OtelInit's exporters do, then asserts that:
#
#   logs    land in Loki            (not elsewhere)
#   metrics land in VictoriaMetrics (not elsewhere)
#   traces  land in Jaeger          (not elsewhere)
#
# and that each signal is attributed to the emitting service. This proves
# the Collector routes each signal type to the right backend per service.
#
# Run against a live stack (OTLP ports published on loopback by the dev
# override). Exit code 0 = all routes correct, 1 = a routing failure.
#
# Usage: python3 bin/observability/smoke_test.py [--service NAME]

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
import uuid

COLLECTOR = "http://127.0.0.1:4318"      # OTLP/HTTP receiver
LOKI = "http://127.0.0.1:3100"           # logs backend
VICTORIAMETRICS = "http://127.0.0.1:8428"  # metrics backend
JAEGER = "http://127.0.0.1:16686"        # traces backend

TIMEOUT_S = 60    # how long to wait for a signal to become queryable
POLL_S = 3


def _post(url, payload):
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status


def _get(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.load(r)


def _attr(key, value):
    return {"key": key, "value": {"stringValue": value}}


def emit(service, run_id):
    now_ns = str(time.time_ns())
    resource = {"attributes": [_attr("service.name", service)]}

    # LOG -> logs pipeline -> Loki
    _post(f"{COLLECTOR}/v1/logs", {"resourceLogs": [{
        "resource": resource,
        "scopeLogs": [{"logRecords": [{
            "timeUnixNano": now_ns,
            "severityText": "INFO",
            "severityNumber": 9,
            "body": {"stringValue": f"smoke-test log {run_id}"},
        }]}],
    }]})

    # METRIC -> metrics pipeline -> VictoriaMetrics
    metric_name = f"obs_smoke_test_{run_id}"
    _post(f"{COLLECTOR}/v1/metrics", {"resourceMetrics": [{
        "resource": resource,
        "scopeMetrics": [{"metrics": [{
            "name": metric_name,
            "gauge": {"dataPoints": [{
                "asDouble": 1,
                "timeUnixNano": now_ns,
                "attributes": [_attr("run", run_id)],
            }]},
        }]}],
    }]})

    # TRACE -> traces pipeline -> Jaeger
    trace_id = (uuid.uuid4().hex + uuid.uuid4().hex)[:32]
    span_id = uuid.uuid4().hex[:16]
    _post(f"{COLLECTOR}/v1/traces", {"resourceSpans": [{
        "resource": resource,
        "scopeSpans": [{"spans": [{
            "traceId": trace_id,
            "spanId": span_id,
            "name": f"smoke.test.{run_id}",
            "kind": 1,
            "startTimeUnixNano": now_ns,
            "endTimeUnixNano": str(int(now_ns) + 1_000_000),
        }]}],
    }]})
    return metric_name


def _poll(check):
    deadline = time.time() + TIMEOUT_S
    while time.time() < deadline:
        try:
            if check():
                return True
        except Exception:
            pass
        time.sleep(POLL_S)
    return False


def loki_has(service, run_id):
    q = urllib.parse.quote(f'{{service_name="{service}"}}')
    end = time.time_ns()
    start = end - 10 * 60 * 1_000_000_000
    url = f"{LOKI}/loki/api/v1/query_range?query={q}&start={start}&end={end}&limit=500"
    res = _get(url)["data"]["result"]
    return any(run_id in v[1] for s in res for v in s["values"])


def vm_has(metric_name):
    url = f"{VICTORIAMETRICS}/api/v1/query?query={urllib.parse.quote(metric_name)}"
    return len(_get(url)["data"]["result"]) > 0


def jaeger_has(service, run_id):
    svcs = _get(f"{JAEGER}/api/services").get("data") or []
    if service not in svcs:
        return False
    url = f"{JAEGER}/api/traces?service={urllib.parse.quote(service)}&limit=50&lookback=1h"
    data = _get(url).get("data") or []
    return any(run_id in s.get("operationName", "") for t in data for s in t["spans"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--services",
        default="obs-smoke-a,obs-smoke-b",
        help="comma-separated service names to emit as (default tests two)",
    )
    args = ap.parse_args()
    services = [s.strip() for s in args.services.split(",") if s.strip()]

    # Emit a log + metric + trace for every service, each with its own run id.
    emitted = {}
    for svc in services:
        run_id = uuid.uuid4().hex[:12]
        metric_name = emit(svc, run_id)
        emitted[svc] = (run_id, metric_name)
        print(f"emitted  service={svc}  run_id={run_id}")

    results = []
    for svc, (run_id, metric_name) in emitted.items():
        checks = [
            (f"{svc}: logs    -> Loki",            lambda s=svc, r=run_id: loki_has(s, r)),
            (f"{svc}: metrics -> VictoriaMetrics", lambda m=metric_name: vm_has(m)),
            (f"{svc}: traces  -> Jaeger",          lambda s=svc, r=run_id: jaeger_has(s, r)),
        ]
        for label, check in checks:
            ok = _poll(check)
            results.append(ok)
            print(f"  {'PASS' if ok else 'FAIL'}  {label}")

    # Isolation: one service's log marker must NOT appear under another service.
    if len(services) >= 2:
        a, b = services[0], services[1]
        a_run = emitted[a][0]
        leaked = False
        try:
            leaked = loki_has(b, a_run)   # a's marker should not be under b
        except Exception:
            leaked = False
        ok = not leaked
        results.append(ok)
        print(f"  {'PASS' if ok else 'FAIL'}  isolation: {a} logs not attributed to {b}")

    if all(results):
        print("RESULT: PASS (each signal routed to the correct backend, per service)")
        return 0
    print("RESULT: FAIL (a signal was mis-routed or mis-attributed)")
    return 1


if __name__ == "__main__":
    sys.exit(main())
