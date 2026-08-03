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
Compare the two paths' outputs for one operator: JSONL DataFrames, or the Plotly
figure a visualization operator renders.

Usage: compare.py [--unordered] [--ignore-cols c1,c2]
                  [--model-cols c1,c2 --probe features.jsonl]
                  <actual.jsonl> <expected.jsonl>
       compare.py --plotly <actual.jsonl> <expected.json>

  --unordered   Sort both DataFrames lexicographically by all columns before
                comparing, so rows match as a set/bag rather than positionally.
                This is the norm: the engine runs operators across parallel
                workers, so output row order is not part of the contract.
                Without this flag the comparator matches rows positionally
                (after reset_index(drop=True)) — used only for the sort family,
                whose output order IS meaningful.

  --ignore-cols Comma-separated column names to drop from both frames before
                comparing. For opaque columns whose value isn't compared.

  --model-cols  Comma-separated columns holding a base64(pickle) sklearn model.
                Rather than byte-compare them (two independently-trained models
                are functionally equal but not bit-identical), the comparator
                unpickles both sides, has each model predict on the --probe
                feature set, and asserts the predictions match — verifying the
                two code paths produce behaviorally-equivalent models. The raw
                model columns are then dropped before the frame comparison.

  --probe       JSONL feature set the --model-cols models predict on. Each
                model uses its own feature_names_in_ to select columns, so the
                probe may include extra columns (e.g. the training target).

  --plotly      Compare Plotly figures instead of DataFrames. The actual side is
                a one-row JSONL with `html-content` or `json-content`; for
                `html-content` the first `Plotly.newPlot(...)` payload is
                extracted. The expected side is the standalone path's
                `fig.write_json(...)`. Only data and layout are compared, with
                display-only `uid` fields stripped and floats matched by
                tolerance. Takes none of the DataFrame flags.

Exit 0  - Outputs equal (and model predictions match, if --model-cols)
Exit 1  - Outputs differ; detail on stderr
Exit 2  - Bad invocation

Persistent mode: `compare.py --serve` imports pandas once and then serves many
comparisons over its lifetime, reading one JSON job per line on stdin and
writing one JSON result per line on stdout. This avoids paying the ~214 ms
pandas import on every comparison (the comparison itself is ~ms). It reuses the
exact same functions the CLI calls, so behavior is identical.

  request   {"kind": "dataframe", "actual": "<abs>", "expected": "<abs>",
             "unordered": false, "ignoreCols": [], "modelCols": [],
             "probe": null}\n
            {"kind": "plotly", "actual": "<abs>", "expected": "<abs>"}\n
  response  {"exit": 0|1, "stdout": "", "stderr": "<diff on mismatch>"}\n

`kind` defaults to "dataframe". Both kinds are served by the same worker so a
run needs one comparison pool rather than one per output shape; the Plotly side
needs nothing pandas does not already pull in.

A mismatch is exit 1 with the diff on `stderr`, mirroring the CLI's nonzero
exit so the Scala side's ComparatorMismatchException path is unchanged. A
comparison error never kills the server; only closing stdin (EOF) ends it.
"""
import sys

# pandas is imported where it is used, not here: the --plotly comparison needs
# nothing from it, and a module-level import would make that one-shot invocation
# pay ~500 ms for an interpreter that then compares two JSON documents. `serve()`
# imports it eagerly at startup instead, so a pooled worker still pays it once
# rather than once per DataFrame comparison.


def _compare_model_predictions(actual, expected, model_cols, probe_path) -> None:
    """For each model column, unpickle both sides and assert their predictions
    on the probe set match. Raises AssertionError on any divergence."""
    import base64
    import pickle

    import numpy as np
    import pandas as pd

    if probe_path is None:
        raise AssertionError("--model-cols requires --probe with a feature set")
    probe = pd.read_json(probe_path, lines=True)

    for col in model_cols:
        if col not in actual.columns or col not in expected.columns:
            continue
        if len(actual) != len(expected):
            raise AssertionError(
                f"model column {col!r}: row count differs "
                f"({len(actual)} vs {len(expected)})"
            )
        for i in range(len(actual)):
            m_actual = pickle.loads(base64.b64decode(actual[col].iloc[i]))
            m_expected = pickle.loads(base64.b64decode(expected[col].iloc[i]))

            # A model with feature_names_in_ selects its (numeric) feature
            # columns from the probe, naturally dropping the training target the
            # probe may still carry. A model WITHOUT it was fitted on a 1-D input
            # rather than a named frame — i.e. a text pipeline (e.g.
            # CountVectorizer) trained on a single text Series — so feed the
            # probe's first column as a Series, not the whole frame (predicting
            # on a DataFrame would make CountVectorizer iterate column labels).
            names = getattr(m_actual, "feature_names_in_", None)
            x_a = probe[list(names)] if names is not None else probe.iloc[:, 0]
            names_e = getattr(m_expected, "feature_names_in_", None)
            x_e = probe[list(names_e)] if names_e is not None else probe.iloc[:, 0]

            pred_a = np.asarray(m_actual.predict(x_a))
            pred_e = np.asarray(m_expected.predict(x_e))

            if pred_a.shape != pred_e.shape:
                raise AssertionError(
                    f"model column {col!r} row {i}: prediction shape differs "
                    f"({pred_a.shape} vs {pred_e.shape})"
                )
            numeric = np.issubdtype(pred_a.dtype, np.number) and np.issubdtype(
                pred_e.dtype, np.number
            )
            ok = (
                np.allclose(pred_a, pred_e, rtol=1e-5, atol=1e-8)
                if numeric
                else np.array_equal(pred_a, pred_e)
            )
            if not ok:
                raise AssertionError(
                    f"model column {col!r} row {i}: predictions differ\n"
                    f"  actual:   {pred_a}\n"
                    f"  expected: {pred_e}"
                )


def _run_comparison(
    actual_path: str,
    expected_path: str,
    unordered: bool,
    ignore_cols: list,
    model_cols: list,
    probe_path,
) -> "str | None":
    """Compare two JSONL DataFrames. Returns None if they match, or a human
    diff string if they differ (exit-1 condition). Unexpected errors (e.g. a
    bad input file) propagate to the caller. This is the single source of
    comparison truth shared by the CLI and the --serve loop."""
    import pandas as pd

    actual = pd.read_json(actual_path, lines=True)
    expected = pd.read_json(expected_path, lines=True)

    # Model columns: compare behavior (predictions) rather than bytes, then drop
    # the raw columns so the frame comparison covers everything else exactly.
    if model_cols:
        try:
            _compare_model_predictions(actual, expected, model_cols, probe_path)
        except AssertionError as exc:
            return str(exc)
        actual = actual.drop(columns=model_cols, errors="ignore")
        expected = expected.drop(columns=model_cols, errors="ignore")

    if ignore_cols:
        actual = actual.drop(columns=ignore_cols, errors="ignore")
        expected = expected.drop(columns=ignore_cols, errors="ignore")

    if unordered:
        # Sort both sides by the same column key so set-equal frames collapse
        # to the same row sequence. assert_frame_equal still does the actual
        # value diff and respects rtol/check_dtype. Mergesort = stable, so
        # rows that are tied on all columns keep their relative order — not
        # strictly necessary for set equality (no ties → no duplicates after
        # the op's dedup step) but cheap insurance.
        cols = list(actual.columns)
        if cols:
            actual = actual.sort_values(
                by=cols, kind="mergesort", na_position="last"
            ).reset_index(drop=True)
            expected = expected.sort_values(
                by=cols, kind="mergesort", na_position="last"
            ).reset_index(drop=True)

    try:
        pd.testing.assert_frame_equal(
            actual,
            expected,
            check_like=True,
            check_dtype=False,
            rtol=1e-5,
        )
    except AssertionError as exc:
        return str(exc)
    return None


def _load_actual_plot(path) -> dict:
    import json

    with open(path, "r", encoding="utf-8") as fh:
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


def _plotly_payload_from_html(html: str) -> dict:
    """Pull the data/layout arguments out of the first Plotly.newPlot(...) call.

    Scanned with a JSON decoder rather than a regex because the payload is
    arbitrary nested JSON that no bracket-matching pattern handles reliably.
    """
    import json

    marker = "Plotly.newPlot("
    start = html.find(marker)
    if start < 0:
        raise AssertionError("html-content does not contain Plotly.newPlot(...)")

    decoder = json.JSONDecoder()
    index = start + len(marker)
    args: list = []
    while len(args) < 4:
        while index < len(html) and html[index] in " \t\r\n,":
            index += 1
        value, consumed = decoder.raw_decode(html[index:])
        args.append(value)
        index += consumed

    return {"data": args[1], "layout": args[2]}


def _load_expected_plot(path) -> dict:
    import json

    with open(path, "r", encoding="utf-8") as fh:
        value = json.load(fh)
    return {"data": value.get("data", []), "layout": value.get("layout", {})}


def _strip_unstable(value):
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


def _plots_equal(actual, expected) -> bool:
    import math

    if isinstance(actual, (int, float)) and isinstance(expected, (int, float)):
        return math.isclose(float(actual), float(expected), rel_tol=1e-9, abs_tol=1e-12)
    if isinstance(actual, dict) and isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(
            _plots_equal(actual[key], expected[key]) for key in actual.keys()
        )
    if isinstance(actual, list) and isinstance(expected, list):
        return len(actual) == len(expected) and all(
            _plots_equal(left, right) for left, right in zip(actual, expected)
        )
    return actual == expected


def _run_plotly_comparison(actual_path, expected_path) -> "str | None":
    """Compare two Plotly figures. Returns None if they match, or a human diff
    string if they differ — the same contract as `_run_comparison`, so the CLI
    and the --serve loop treat both kinds identically."""
    import json

    actual = _strip_unstable(_load_actual_plot(actual_path))
    expected = _strip_unstable(_load_expected_plot(expected_path))
    if _plots_equal(actual, expected):
        return None
    return "\n".join(
        [
            "Plotly JSON mismatch",
            "--- actual ---",
            json.dumps(actual, indent=2, sort_keys=True),
            "--- expected ---",
            json.dumps(expected, indent=2, sort_keys=True),
        ]
    )


def main() -> None:
    args = sys.argv[1:]
    unordered = False
    ignore_cols: list = []
    model_cols: list = []
    probe_path = None

    if args and args[0] == "--plotly":
        if len(args) != 3:
            print(
                f"usage: {sys.argv[0]} --plotly <actual.jsonl> <expected.json>",
                file=sys.stderr,
            )
            sys.exit(2)
        msg = _run_plotly_comparison(args[1], args[2])
        if msg is not None:
            print(msg, file=sys.stderr)
            sys.exit(1)
        return

    while args and args[0].startswith("--"):
        if args[0] == "--unordered":
            unordered = True
            args = args[1:]
        elif args[0] == "--ignore-cols":
            if len(args) < 2:
                print("--ignore-cols requires an argument", file=sys.stderr)
                sys.exit(2)
            ignore_cols = [c for c in args[1].split(",") if c]
            args = args[2:]
        elif args[0] == "--model-cols":
            if len(args) < 2:
                print("--model-cols requires an argument", file=sys.stderr)
                sys.exit(2)
            model_cols = [c for c in args[1].split(",") if c]
            args = args[2:]
        elif args[0] == "--probe":
            if len(args) < 2:
                print("--probe requires an argument", file=sys.stderr)
                sys.exit(2)
            probe_path = args[1]
            args = args[2:]
        else:
            print(f"unknown flag: {args[0]}", file=sys.stderr)
            sys.exit(2)
    if len(args) != 2:
        print(
            f"usage: {sys.argv[0]} [--unordered] [--ignore-cols c1,c2] "
            f"[--model-cols c1,c2 --probe features.jsonl] "
            f"<actual.jsonl> <expected.jsonl>",
            file=sys.stderr,
        )
        sys.exit(2)

    msg = _run_comparison(
        args[0], args[1], unordered, ignore_cols, model_cols, probe_path
    )
    if msg is not None:
        print(msg, file=sys.stderr)
        sys.exit(1)


def serve() -> None:
    """Persistent comparison server. See the module docstring for the protocol.

    Each job runs the same function the CLI calls for its kind. A comparison
    error is reported as exit 1 with the diff on `stderr`; only closing stdin
    ends the loop.
    """
    import io
    import json
    import traceback
    from contextlib import redirect_stderr, redirect_stdout

    # Eagerly, before signalling ready: the point of a persistent worker is that
    # this cost is paid once per worker instead of once per comparison, and
    # `ready` should mean the worker is warm.
    import pandas  # noqa: F401

    sys.stdout.write(json.dumps({"ready": True}) + "\n")
    sys.stdout.flush()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        out_buf, err_buf = io.StringIO(), io.StringIO()
        try:
            job = json.loads(line)
            with redirect_stdout(out_buf), redirect_stderr(err_buf):
                if job.get("kind", "dataframe") == "plotly":
                    msg = _run_plotly_comparison(job["actual"], job["expected"])
                else:
                    msg = _run_comparison(
                        job["actual"],
                        job["expected"],
                        job.get("unordered", False),
                        job.get("ignoreCols", []),
                        job.get("modelCols", []),
                        job.get("probe"),
                    )
            resp = {
                "exit": 0 if msg is None else 1,
                "stdout": out_buf.getvalue(),
                "stderr": err_buf.getvalue() + ("" if msg is None else msg),
            }
        except BaseException:  # noqa: BLE001 — a bad job must not kill the server
            resp = {
                "exit": 1,
                "stdout": out_buf.getvalue(),
                "stderr": err_buf.getvalue() + traceback.format_exc(),
            }
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--serve":
        serve()
    else:
        main()
