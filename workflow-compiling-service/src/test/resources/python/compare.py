#!/usr/bin/env python3
"""
Compare two JSONL DataFrames produced by the translation validation harness.

Usage: compare.py [--unordered] [--ignore-cols c1,c2]
                  [--model-cols c1,c2 --probe features.jsonl]
                  <actual.jsonl> <expected.jsonl>

  --unordered   Sort both DataFrames lexicographically by all columns before
                comparing — for set-semantics operators (Intersect, Difference,
                SymmetricDifference) and joins whose JVM emit order differs
                from the pandas equivalent. Without this flag, the comparator
                matches rows positionally after reset_index(drop=True).

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

Exit 0  - DataFrames equal (and model predictions match, if --model-cols)
Exit 1  - DataFrames differ / model predictions differ; detail on stderr
Exit 2  - Bad invocation
"""
import sys

import pandas as pd


def _compare_model_predictions(actual, expected, model_cols, probe_path) -> None:
    """For each model column, unpickle both sides and assert their predictions
    on the probe set match. Raises AssertionError on any divergence."""
    import base64
    import pickle

    import numpy as np

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


def main() -> None:
    args = sys.argv[1:]
    unordered = False
    ignore_cols: list = []
    model_cols: list = []
    probe_path = None
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

    actual_path, expected_path = args[0], args[1]
    actual = pd.read_json(actual_path, lines=True)
    expected = pd.read_json(expected_path, lines=True)

    # Model columns: compare behavior (predictions) rather than bytes, then drop
    # the raw columns so the frame comparison covers everything else exactly.
    if model_cols:
        try:
            _compare_model_predictions(actual, expected, model_cols, probe_path)
        except AssertionError as exc:
            print(str(exc), file=sys.stderr)
            sys.exit(1)
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
        print(str(exc), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
