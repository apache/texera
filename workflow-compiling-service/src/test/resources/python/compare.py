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
                a JSONL with `html-content` or `json-content`, a chart per row
                or a page of them in one row; every `Plotly.newPlot(...)`
                payload counts. The expected side is the standalone path's
                `fig.write_json(...)`, one figure or a list of them, or its page
                when that is where the whole set landed. The two are compared in
                order, so a chart one path drew and the other did not is a
                mismatch. Only data and layout are compared, with display-only
                `uid` fields stripped and floats matched by tolerance. Takes
                none of the DataFrame flags.

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

    # A model column holds an sklearn estimator, so the unpickling below imports
    # sklearn anyway; asking it what kind of estimator it handed back costs
    # nothing more. Both are function-local so a run with no model column, which
    # is most of them, never pays for either.
    from sklearn.base import is_regressor

    if probe_path is None:
        raise AssertionError("--model-cols requires --probe with a feature set")
    probe = pd.read_json(probe_path, lines=True)
    # The probe is the operator's own input table, so under the nulls scenario it
    # carries the holes that scenario punched. What is under test is whether the
    # two models agree, and an estimator that refuses a NaN at predict time would
    # end the comparison over the probe rather than over either model. Drop those
    # rows: both models are asked the same questions either way.
    probe = probe.dropna()
    if probe.empty:
        raise AssertionError(
            "probe has no complete row to predict on; the two models cannot be compared"
        )

    for col in model_cols:
        # A requested column is one the engine declared as a model, so a side
        # that never emitted it IS the divergence. Skipping it here would hide
        # that: the column is dropped from both frames afterwards, and a path
        # that produced no model at all would compare equal.
        missing = [
            side
            for side, frame in (("actual", actual), ("expected", expected))
            if col not in frame.columns
        ]
        if missing:
            raise AssertionError(
                f"model column {col!r} missing from {' and '.join(missing)}"
            )
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
            names_e = getattr(m_expected, "feature_names_in_", None)

            # What a model was fitted on is part of the model, and each side was
            # being asked about its own features. Two models fitted on different
            # columns still answer alike on a probe that carries both, so the
            # predictions agreed while the models did not. One side having names
            # where the other has none is the same divergence: it says the two
            # were fitted on differently shaped input.
            listed = None if names is None else list(names)
            listed_e = None if names_e is None else list(names_e)
            if listed != listed_e:
                raise AssertionError(
                    f"model column {col!r} row {i}: fitted feature names differ\n"
                    f"  actual:   {listed}\n"
                    f"  expected: {listed_e}"
                )

            x_a = probe[listed] if listed is not None else probe.iloc[:, 0]
            x_e = probe[listed_e] if listed_e is not None else probe.iloc[:, 0]

            pred_a = np.asarray(m_actual.predict(x_a))
            pred_e = np.asarray(m_expected.predict(x_e))

            if pred_a.shape != pred_e.shape:
                raise AssertionError(
                    f"model column {col!r} row {i}: prediction shape differs "
                    f"({pred_a.shape} vs {pred_e.shape})"
                )
            # Only a regressor predicts a measurement, where the last bits are
            # arithmetic and a tolerance belongs. Every other estimator predicts
            # a label, a class or a cluster, and a different label is a
            # different answer however near the two numbers sit: classes 100000
            # and 100001 are one part in 1e5 apart, which allclose accepts. The
            # estimator says which it is; the prediction's dtype cannot, since a
            # numeric label is a number too. Anything sklearn does not call a
            # regressor, including a non-sklearn pickle, is compared exactly.
            tolerant = (
                is_regressor(m_actual)
                and is_regressor(m_expected)
                and np.issubdtype(pred_a.dtype, np.number)
                and np.issubdtype(pred_e.dtype, np.number)
            )
            ok = (
                np.allclose(pred_a, pred_e, rtol=1e-5, atol=1e-8)
                if tolerant
                else np.array_equal(pred_a, pred_e)
            )
            if not ok:
                raise AssertionError(
                    f"model column {col!r} row {i}: predictions differ\n"
                    f"  actual:   {pred_a}\n"
                    f"  expected: {pred_e}"
                )


def _declared_types(actual_path: str) -> dict:
    """What the engine declared each output column as, read off the schema it
    writes beside its output. Empty when there is no sidecar, which leaves the
    inference in place rather than guessing."""
    import json
    import os

    sidecar = actual_path + ".schema.json"
    if not os.path.exists(sidecar):
        return {}
    with open(sidecar) as fh:
        schema = json.load(fh)
    return {a["attributeName"]: a.get("attributeType") for a in schema.get("attributes", [])}


def _string_columns(actual_path: str) -> dict:
    """The declared STRING columns, as a `read_json` dtype map."""
    return {
        name: str for name, kind in _declared_types(actual_path).items() if kind == "string"
    }


def _declared_strings(path: str, columns: list) -> dict:
    """The named columns re-read with Python's json, which keeps a string a
    string and leaves a number a number.

    Reading them with `dtype=str` settles the two sides on one spelling, which
    is what a text column needs, but it settles too much: it turns the JSON
    number 6 into "6" as readily as it leaves "6" alone, so a side that wrote a
    number where the engine declared text compared equal to the text. The
    engine's own output is always text here, so a number is the divergence.
    """
    import json

    import pandas as pd

    values = {column: [] for column in columns}
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            for column in columns:
                cell = row.get(column)
                if cell is None:
                    values[column].append(pd.NA)
                elif isinstance(cell, str):
                    values[column].append(cell)
                else:
                    raise AssertionError(
                        f"column '{column}' is declared text but holds {cell!r} in {path}"
                    )
    return {column: pd.array(cells, dtype="string") for column, cells in values.items()}


def _raw_column(path: str, columns: list) -> dict:
    """The named columns as Python's json read them, where an integer is exact
    and a float is still a float."""
    import json

    values = {column: [] for column in columns}
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            for column in columns:
                values[column].append(row.get(column))
    return values


def _json_kind(cell) -> str:
    if isinstance(cell, bool):
        return "boolean"
    if isinstance(cell, int):
        return "integer"
    if isinstance(cell, float):
        return "float"
    return type(cell).__name__


def _assert_sides_agree(actual: dict, expected: dict, columns: list, declared: str) -> None:
    """Fail where the two sides wrote a column as different JSON kinds.

    What the comparison is asked is whether the paths match, not whether pandas
    kept the declared type: where the engine's own path is a Python operator its
    table goes through pandas too, so a hole widens the column on both paths
    alike and the two still agree. One side widening on its own is the
    divergence, and it is the one the digits hide.
    """
    for column in columns:
        kinds_a = {_json_kind(c) for c in actual[column] if c is not None}
        kinds_e = {_json_kind(c) for c in expected[column] if c is not None}
        if kinds_a != kinds_e:
            raise AssertionError(
                f"column '{column}' is declared {declared} and the two sides wrote it "
                f"differently\n"
                f"  actual:   {sorted(kinds_a) or ['all null']}\n"
                f"  expected: {sorted(kinds_e) or ['all null']}"
            )


def _exact_integers(actual_path: str, expected_path: str, columns: list) -> tuple:
    """Both sides' named columns re-read with Python's json, whose integers are
    exact, having first agreed on what each column holds.

    `read_json` parses a column holding a null through float64, so a LONG of
    9007199254740993 is already 9007199254740992 by the time anything compares
    it. Pinning the dtype does not help: the rounding happens on the way in.

    One side writing a float where the other wrote an integer is the divergence
    the digits hide. 6.0 was read as the 6 the schema asked for, on the grounds
    that both spell the same integer, but they do not survive the same: a later
    cast to text writes "6" from one and "6.0" from the other.

    Both sides writing a float is not a divergence; see [[_assert_sides_agree]].
    """
    import pandas as pd

    actual = _raw_column(actual_path, columns)
    expected = _raw_column(expected_path, columns)
    _assert_sides_agree(actual, expected, columns, "integral")

    def arrays(values):
        out = {}
        for column, cells in values.items():
            # A column either side widened is compared as the float it now is,
            # exactly rather than by tolerance; one both sides kept integral is
            # carried in the nullable integer, where a long stays exact.
            widened = any(isinstance(c, float) for c in cells)
            dtype = "float64" if widened else "Int64"
            out[column] = pd.array(
                [pd.NA if c is None else c for c in cells], dtype=dtype
            )
        return out

    return arrays(actual), arrays(expected)


def _declared_booleans(actual_path: str, expected_path: str, columns: list) -> tuple:
    """Both sides' declared BOOLEAN columns, having first agreed on what each
    one holds.

    numpy counts True as 1, and `assert_frame_equal` is asked not to check
    dtypes, so a column of booleans compared equal to a column of ones. The
    engine says of a BOOLEAN column that it holds true and false, and a side
    that wrote a number there has changed the type of the answer.

    As with the integers, both sides widening alike is not a divergence: a hole
    costs a pandas boolean column its type on whichever path went through
    pandas, and if both did they still agree.
    """
    import pandas as pd

    actual = _raw_column(actual_path, columns)
    expected = _raw_column(expected_path, columns)
    _assert_sides_agree(actual, expected, columns, "boolean")

    def arrays(values):
        out = {}
        for column, cells in values.items():
            kept = all(c is None or isinstance(c, bool) for c in cells)
            cells = [pd.NA if c is None else c for c in cells]
            out[column] = pd.array(cells, dtype="boolean" if kept else "float64")
        return out

    return arrays(actual), arrays(expected)


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

    # A string column has to be READ as one on both sides. Left to itself,
    # `read_json` infers a type per file, so a column the engine wrote as "6"
    # and the script wrote as "6.0" both arrive as the number 6, and a null
    # beside the text "nan" both arrive as NaN -- two genuinely different
    # answers compared as one. The engine writes a schema next to its output;
    # it names which columns are strings, and both sides are read that way.
    declared = _declared_types(actual_path)
    str_cols = {name: str for name, kind in declared.items() if kind == "string"}
    actual = pd.read_json(actual_path, lines=True, dtype=str_cols or None)
    expected = pd.read_json(expected_path, lines=True, dtype=str_cols or None)

    # `dtype=str` above settles the spelling, which a text column needs, but it
    # also turns a JSON number into text, so a side that wrote 6 where the engine
    # declared text compared equal to "6". Re-read those columns from the raw
    # JSON, where a number is still a number and so still a divergence.
    raw_str_cols = [
        name for name in str_cols if name in actual.columns and name in expected.columns
    ]
    if raw_str_cols:
        try:
            for frame, path in ((actual, actual_path), (expected, expected_path)):
                for column, values in _declared_strings(path, raw_str_cols).items():
                    frame[column] = values
        except AssertionError as exc:
            return str(exc)

    # Both sides take the ENGINE's declared type, which also settles a column a
    # hole widened to float on one path and not the other.
    int_cols = [
        name
        for name, kind in declared.items()
        if kind in ("integer", "long") and name in actual.columns and name in expected.columns
    ]
    if int_cols:
        try:
            exact_a, exact_e = _exact_integers(actual_path, expected_path, int_cols)
        except AssertionError as exc:
            return str(exc)
        for column in int_cols:
            actual[column] = exact_a[column]
            expected[column] = exact_e[column]

    # numpy counts True as 1 and the dtype check is off, so a column of booleans
    # compared equal to a column of ones. The engine says a BOOLEAN column holds
    # true and false; a side that wrote a number there changed the answer's type.
    bool_cols = [
        name
        for name, kind in declared.items()
        if kind == "boolean" and name in actual.columns and name in expected.columns
    ]
    if bool_cols:
        try:
            bool_a, bool_e = _declared_booleans(actual_path, expected_path, bool_cols)
        except AssertionError as exc:
            return str(exc)
        for column in bool_cols:
            actual[column] = bool_a[column]
            expected[column] = bool_e[column]

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

    # The column list is part of the answer, and neither half of it was being
    # checked. The comparison took its columns from `actual` and selected those
    # out of `expected`, so a column only `expected` carried was never looked
    # at. Order went unchecked too, under `check_like`, and order is what a
    # positional UDF reads by, what a file export writes out, and what code
    # asking for the first column gets.
    if list(actual.columns) != list(expected.columns):
        only_actual = [c for c in actual.columns if c not in set(expected.columns)]
        only_expected = [c for c in expected.columns if c not in set(actual.columns)]
        detail = []
        if only_actual:
            detail.append(f"  only in actual:   {only_actual}")
        if only_expected:
            detail.append(f"  only in expected: {only_expected}")
        if not detail:
            detail.append("  the same columns in a different order")
        return "\n".join(
            [
                "column mismatch",
                f"  actual:   {list(actual.columns)}",
                f"  expected: {list(expected.columns)}",
            ]
            + detail
        )

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

    # The tolerance was letting integers through with it: at rtol=1e-5, LONG
    # 100000 and 100001 compare equal. Integer columns are compared exactly.
    exact_cols = [c for c in int_cols if c in actual.columns]
    loose_cols = [c for c in actual.columns if c not in exact_cols]
    # No `check_like`: it sorts both frames' columns before comparing, which is
    # what let a reordering through. The lists are known equal by here, so each
    # of these two selections holds the same columns in the same order.
    try:
        if exact_cols:
            pd.testing.assert_frame_equal(
                actual[exact_cols],
                expected[exact_cols],
                check_dtype=False,
                check_exact=True,
            )
        pd.testing.assert_frame_equal(
            actual[loose_cols],
            expected[loose_cols],
            check_dtype=False,
            rtol=1e-5,
        )
    except AssertionError as exc:
        return str(exc)
    return None


def _load_actual_plots(path) -> list:
    """Every chart the run drew: a row per chart, or a row holding a page of
    them, which is how an operator that draws a chart per input row hands them
    over."""
    import json

    with open(path, "r", encoding="utf-8") as fh:
        lines = [raw for raw in fh if raw.strip()]
    if not lines:
        raise AssertionError(f"{path} is empty")

    plots = []
    for number, line in enumerate(lines, start=1):
        row = json.loads(line)
        if "json-content" in row and row["json-content"]:
            value = row["json-content"]
            plots.append(json.loads(value) if isinstance(value, str) else value)
        elif "html-content" in row and row["html-content"]:
            plots.extend(_plotly_payloads_from_html(row["html-content"]))
        else:
            raise AssertionError(
                f"{path} row {number} has neither html-content nor json-content"
            )
    return plots


def _plotly_payloads_from_html(html: str) -> list:
    """Pull the data/layout arguments out of every Plotly.newPlot(...) call.

    Every call, because a page can hold a chart per input row and reading the
    first would leave the rest of them uncompared.

    Scanned with a JSON decoder rather than a regex because the payload is
    arbitrary nested JSON that no bracket-matching pattern handles reliably.
    """
    import json

    marker = "Plotly.newPlot("
    index = html.find(marker)
    if index < 0:
        raise AssertionError("html-content does not contain Plotly.newPlot(...)")

    decoder = json.JSONDecoder()
    plots: list = []
    while index >= 0:
        index += len(marker)
        args: list = []
        while len(args) < 4:
            while index < len(html) and html[index] in " \t\r\n,":
                index += 1
            value, consumed = decoder.raw_decode(html[index:])
            args.append(value)
            index += consumed
        plots.append({"data": args[1], "layout": args[2]})
        index = html.find(marker, index)

    return plots


def _load_expected_plots(path) -> list:
    """Every chart the exported script drew. An operator that draws one writes a
    lone figure; one that draws a chart per row writes the sequence, or writes
    them to its page and only the first as a figure, in which case the page is
    what this is handed."""
    import json

    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()
    if str(path).endswith(".html"):
        return _plotly_payloads_from_html(text)
    value = json.loads(text)
    figures = value if isinstance(value, list) else [value]
    return [
        {"data": fig.get("data", []), "layout": fig.get("layout", {})}
        for fig in figures
    ]


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

    # Before the numeric branch, because `bool` is a subclass of `int` and a
    # boolean fell into it: True and 1 went through math.isclose and compared
    # equal. A trace says things with booleans that a number does not, and a
    # column whose declared type changed from boolean to integer reaches the
    # figure as exactly this difference.
    if isinstance(actual, bool) or isinstance(expected, bool):
        return isinstance(actual, bool) and isinstance(expected, bool) and actual == expected
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

    actual = [_strip_unstable(p) for p in _load_actual_plots(actual_path)]
    expected = [_strip_unstable(p) for p in _load_expected_plots(expected_path)]

    if len(actual) != len(expected):
        return (
            f"Plotly chart count mismatch: the run drew {len(actual)} and the "
            f"exported script drew {len(expected)}"
        )

    for index, (one, other) in enumerate(zip(actual, expected)):
        if _plots_equal(one, other):
            continue
        return "\n".join(
            [
                f"Plotly JSON mismatch on chart {index + 1} of {len(actual)}",
                "--- actual ---",
                json.dumps(one, indent=2, sort_keys=True),
                "--- expected ---",
                json.dumps(other, indent=2, sort_keys=True),
            ]
        )
    return None


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
