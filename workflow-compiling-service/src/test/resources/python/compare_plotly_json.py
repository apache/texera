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
Compare a Plotly visualization tuple JSONL file against a Plotly JSON file.

Usage: compare_plotly_json.py <actual.jsonl> <expected.json>

The actual file is the Texera path output: a one-row JSONL file with either
`html-content` or `json-content`. For `html-content`, the script extracts the
first `Plotly.newPlot(...)` payload and compares its data/layout to the
standalone path's `fig.write_json(...)` output.
"""
import json
import math
import sys
from pathlib import Path
from typing import Any


def _load_actual_plot(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        line = next((raw for raw in fh if raw.strip()), None)
    if line is None:
        raise AssertionError(f"{path} is empty")

    row = json.loads(line)
    if "json-content" in row and row["json-content"]:
        value = row["json-content"]
        return json.loads(value) if isinstance(value, str) else value
    if "html-content" in row and row["html-content"]:
        return _plotly_payload_from_html(row["html-content"])
    raise AssertionError(f"{path} has neither html-content nor json-content")


def _plotly_payload_from_html(html: str) -> dict[str, Any]:
    marker = "Plotly.newPlot("
    start = html.find(marker)
    if start < 0:
        raise AssertionError("html-content does not contain Plotly.newPlot(...)")

    decoder = json.JSONDecoder()
    index = start + len(marker)
    args: list[Any] = []
    while len(args) < 4:
        while index < len(html) and html[index] in " \t\r\n,":
            index += 1
        value, consumed = decoder.raw_decode(html[index:])
        args.append(value)
        index += consumed

    return {"data": args[1], "layout": args[2]}


def _load_expected_plot(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        value = json.load(fh)
    return {"data": value.get("data", []), "layout": value.get("layout", {})}


def _strip_unstable(value: Any) -> Any:
    """Remove display-only fields that are unrelated to chart semantics."""
    if isinstance(value, dict):
        return {
            key: _strip_unstable(child)
            for key, child in value.items()
            if key not in {"uid"}
        }
    if isinstance(value, list):
        return [_strip_unstable(child) for child in value]
    return value


def _equal(actual: Any, expected: Any) -> bool:
    if isinstance(actual, (int, float)) and isinstance(expected, (int, float)):
        return math.isclose(float(actual), float(expected), rel_tol=1e-9, abs_tol=1e-12)
    if isinstance(actual, dict) and isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(
            _equal(actual[key], expected[key]) for key in actual.keys()
        )
    if isinstance(actual, list) and isinstance(expected, list):
        return len(actual) == len(expected) and all(
            _equal(left, right) for left, right in zip(actual, expected)
        )
    return actual == expected


def main() -> None:
    if len(sys.argv) != 3:
        print(
            f"usage: {sys.argv[0]} <actual.jsonl> <expected.json>",
            file=sys.stderr,
        )
        sys.exit(2)

    actual = _strip_unstable(_load_actual_plot(Path(sys.argv[1])))
    expected = _strip_unstable(_load_expected_plot(Path(sys.argv[2])))
    if not _equal(actual, expected):
        print("Plotly JSON mismatch", file=sys.stderr)
        print("--- actual ---", file=sys.stderr)
        print(json.dumps(actual, indent=2, sort_keys=True), file=sys.stderr)
        print("--- expected ---", file=sys.stderr)
        print(json.dumps(expected, indent=2, sort_keys=True), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
