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
Persistent worker that syntax-checks generated operator code, replacing one
`python -I -S -B -m py_compile <file>` spawn per operator descriptor.

`compile(source, path, "exec", dont_inherit=True)` is what `py_compile` does
before writing a `.pyc` and raises the same SyntaxError; skipping that write is
why neither `-B` nor a temp file is needed here. `dont_inherit` is the half a
pooled worker cannot leave out: without it this module's own `__future__`
import applies to every source it checks, which on CPython 3.12 rejects a
walrus inside an annotation that the spawn accepts.

Protocol (line-delimited JSON, both directions):

  startup   worker -> parent:  {"ready": true}
  request   parent -> worker:  {"source": "<code>", "name": "<label>"}\n
  response  worker -> parent:  {"exit": 0, "stdout": "...", "stderr": "..."}\n

`exit` is 1 when the source does not compile, with the SyntaxError in `stderr`,
mirroring the spawn it replaces so the parent's reporting is unchanged. That
does not end the worker; only a hard interpreter crash does.
"""
from __future__ import annotations

import json
import sys
import traceback


def _compile_one(source: str, name: str) -> "dict[str, object]":
    """Compile one generated module. `name` is the filename the traceback shows,
    so a report names the descriptor rather than a temp path.
    """
    try:
        compile(source, name, "exec", dont_inherit=True)
        return {"exit": 0, "stdout": "", "stderr": ""}
    except (SyntaxError, ValueError):
        # ValueError: sources compile() rejects outright, e.g. an embedded NUL.
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
