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
"""
Persistent worker for the Path B (standalone) verify path.

Motivation: forking a fresh interpreter per operator pays the pandas/plotly
import cost (~260-310 ms) on every spawn, while the operator's actual compute
on the tiny canonical fixtures is ~4 ms. Imports dominate ~96% of the per-spawn
cost. This worker imports those heavy libraries ONCE at startup, then executes
many operators' generated scripts over its lifetime — so the import cost is
paid once, not once per operator.

It is a drop-in replacement for `python <script.py>`: it runs the exact same
rendered script `StandaloneRunner` already produces (imports + prologue + body
+ epilogue). The script's own top-of-file `import pandas` becomes a ~0 ms
`sys.modules` cache hit.

Protocol (line-delimited JSON, both directions):

  startup   worker -> parent:  {"ready": true}
  request   parent -> worker:  {"scriptPath": "<abs>", "workDir": "<abs>"}\n
  response  worker -> parent:  {"exit": 0, "stdout": "...", "stderr": "..."}\n

`exit` is 0 on success or 1 if the script raised; on 1, `stderr` carries the
traceback — mirroring a nonzero subprocess exit so the Scala side's
StandaloneExecutionException path is unchanged. The worker keeps running after
a script error (only a hard interpreter crash ends it); parent closes stdin
(EOF) to shut it down.

Isolation trade-off (accepted, per design discussion): all jobs share one
interpreter, so module-level state (e.g. pandas display options) can leak
between operators. Each job is exec'd in a FRESH namespace and chdir'd to its
own workDir to contain the common cases; this is weaker than the old
process-per-operator isolation.
"""
from __future__ import annotations

import io
import json
import os
import sys
import traceback
from contextlib import redirect_stderr, redirect_stdout

# --- Pay the heavy import cost ONCE, here, at startup. ----------------------
# These mirror the imports StandaloneRunner injects at the top of every
# rendered script. Pre-importing them populates sys.modules, so each executed
# script's own `import pandas as pd` / `import plotly...` is a cache hit.
# numpy is intentionally NOT imported (see StandaloneRunner.renderScript: the
# production translator only provides pandas + plotly, so an operator needing
# numpy must import it itself — we must not mask that).
import pandas as pd  # noqa: F401
import plotly.express as px  # noqa: F401
import plotly.graph_objects as go  # noqa: F401
import plotly.io  # noqa: F401


def _run_one(script_path: str, work_dir: str) -> "dict[str, object]":
    """Execute one rendered standalone script and capture its output.

    Runs in a fresh namespace with cwd = work_dir (generated code may use
    relative paths, e.g. CSVScan's `pd.read_csv("sample.csv")`; absolute paths
    written by the prologue/epilogue are unaffected). The script's stdout /
    stderr are redirected into buffers so they never corrupt the protocol
    channel on real stdout.
    """
    out_buf, err_buf = io.StringIO(), io.StringIO()
    # __name__ = "__main__" so scripts with a `if __name__ == "__main__"` guard
    # still run their body (the translator does not emit one, but it is free
    # insurance and matches `python script.py` semantics).
    namespace = {"__name__": "__main__", "__file__": script_path}
    try:
        with open(script_path, "r", encoding="utf-8") as f:
            source = f.read()
        os.chdir(work_dir)
        code = compile(source, script_path, "exec")
        with redirect_stdout(out_buf), redirect_stderr(err_buf):
            exec(code, namespace)  # noqa: S102 (running generated verify code by design)
        return {"exit": 0, "stdout": out_buf.getvalue(), "stderr": err_buf.getvalue()}
    except BaseException:  # noqa: BLE001 — a script error must NOT kill the worker
        # Match a nonzero subprocess exit: traceback goes to stderr, exit = 1.
        err = err_buf.getvalue() + traceback.format_exc()
        return {"exit": 1, "stdout": out_buf.getvalue(), "stderr": err}


def main() -> None:
    # Signal readiness only after the heavy imports above have completed, so the
    # parent can warm a pool and attribute startup cost deterministically.
    sys.stdout.write(json.dumps({"ready": True}) + "\n")
    sys.stdout.flush()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            result = _run_one(req["scriptPath"], req["workDir"])
        except Exception:  # malformed request — report, keep serving
            result = {"exit": 1, "stdout": "", "stderr": traceback.format_exc()}
        sys.stdout.write(json.dumps(result) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
