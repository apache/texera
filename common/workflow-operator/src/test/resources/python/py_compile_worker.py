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
Persistent worker that syntax-checks generated operator code.

Replaces one `python -I -S -B -m py_compile <file>` spawn per operator
descriptor. There is no import cost to amortize here — the parent launches this
with the same `-I -S` isolation, so nothing outside the stdlib is loaded — but
the interpreter boot is the entire cost of a check whose actual work is under a
millisecond. One worker turns N boots into one.

`compile(source, path, "exec")` is what `py_compile` does before writing a
`.pyc`, and it raises the same SyntaxError. Skipping the write makes the `-B` the
one-shot path needed unnecessary, and takes the per-descriptor temp file with it.

Protocol (line-delimited JSON, both directions):

  startup   worker -> parent:  {"ready": true}
  request   parent -> worker:  {"source": "<generated code>", "name": "<label>"}\n
  response  worker -> parent:  {"exit": 0, "stdout": "...", "stderr": "..."}\n

`exit` is 0 when the source compiles and 1 when it does not, with the formatted
SyntaxError in `stderr` — mirroring the nonzero exit and captured output of the
spawn this replaces, so the parent's reporting is unchanged. A source that fails
to compile does not end the worker; only a hard interpreter crash does.
"""
from __future__ import annotations

import json
import sys
import traceback


def _compile_one(source: str, name: str) -> "dict[str, object]":
    """Compile one generated module, reporting failure the way a spawn would.

    `name` is only the filename shown in the traceback, so a report names the
    descriptor rather than a temp path.
    """
    try:
        compile(source, name, "exec")
        return {"exit": 0, "stdout": "", "stderr": ""}
    except (SyntaxError, ValueError):
        # ValueError covers sources compile() rejects outright, e.g. an embedded
        # NUL — a defect in the generated code, not a worker fault.
        return {"exit": 1, "stdout": "", "stderr": traceback.format_exc()}


def main() -> None:
    sys.stdout.write(json.dumps({"ready": True}) + "\n")
    sys.stdout.flush()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            result = _compile_one(req["source"], req.get("name", "<generated>"))
        except Exception:  # malformed request — report, keep serving
            result = {"exit": 1, "stdout": "", "stderr": traceback.format_exc()}
        sys.stdout.write(json.dumps(result) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
