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

"""Dry-run a generated UDFSourceOperator class against a real file so the Generate
endpoint can return useful errors at design time instead of letting the workflow
crash at run time.

Args (sys.argv):
  1. path to file containing the generated Python code
  2. python-source-path to add to sys.path (so `from pytexera import *` resolves)
  3. max rows to consume before stopping (positive int)

Output:
  Single JSON line on stdout. Keys:
    {"ok": true,  "rowsSeen": <int>, "samples": [<up to 3 rows>]}
  or
    {"ok": false, "error": "<exception message>", "traceback": "<full traceback>"}

Exit code is always 0 — failures are reported via JSON, not via exit codes, so the
caller can distinguish "dry-run subprocess failed to launch" (non-zero) from "user
code threw at runtime" (zero + ok=false).
"""

import json
import sys
import traceback


def main() -> None:
    if len(sys.argv) < 4:
        print(json.dumps({"ok": False, "error": "dry_run harness called with wrong arg count"}))
        return
    code_path, python_src_path, max_rows_str = sys.argv[1], sys.argv[2], sys.argv[3]
    sys.path.insert(0, python_src_path)
    try:
        max_rows = max(1, int(max_rows_str))
    except ValueError:
        max_rows = 5

    try:
        with open(code_path, "r", encoding="utf-8") as fh:
            code = fh.read()
    except OSError as e:
        print(json.dumps({"ok": False, "error": f"could not read code file: {e}"}))
        return

    try:
        # Bring pytexera names into the module so the user code's `from pytexera import *`
        # works the same way it would in the real worker.
        import pytexera  # noqa: F401

        # Give the exec'd module a stable __name__ / __module__; without this the
        # `@overrides` decorator's signature compatibility check trips on a None module
        # attribute (`callable.__module__.split(...)`).
        namespace: dict = {"__name__": "llm_source_generated", "__module__": "llm_source_generated"}
        exec(compile(code, "<llm-source-generated>", "exec"), namespace)
        op_cls = namespace.get("GenerateOperator")
        if op_cls is None:
            print(json.dumps({
                "ok": False,
                "error": "generated code did not define `GenerateOperator`",
            }))
            return

        op = op_cls()
        if hasattr(op, "open"):
            op.open()
        rows = []
        try:
            for row in op.produce():
                if row is None:
                    continue
                rows.append(row)
                if len(rows) >= max_rows:
                    break
        finally:
            if hasattr(op, "close"):
                op.close()

        # Normalize each row to a plain dict for JSON serialization.
        samples = []
        for r in rows[:3]:
            try:
                if hasattr(r, "as_dict"):
                    samples.append(r.as_dict())
                elif hasattr(r, "_field_data"):
                    # pytexera Tuple stores fields in _field_data (OrderedDict)
                    samples.append({k: _to_json_safe(v) for k, v in r._field_data.items()})
                elif isinstance(r, dict):
                    samples.append({k: _to_json_safe(v) for k, v in r.items()})
                else:
                    samples.append({"_repr": repr(r)})
            except Exception as inner:
                samples.append({"_repr_error": str(inner)})

        print(json.dumps({"ok": True, "rowsSeen": len(rows), "samples": samples}))
    except Exception as e:
        print(json.dumps({
            "ok": False,
            "error": f"{type(e).__name__}: {e}",
            "traceback": traceback.format_exc(),
        }))


def _to_json_safe(value):
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    try:
        return str(value)
    except Exception:
        return repr(value)


if __name__ == "__main__":
    main()
