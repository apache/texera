#!/usr/bin/env python3
"""
Compare two JSONL DataFrames produced by the translation validation harness.

Usage: compare.py [--unordered] <actual.jsonl> <expected.jsonl>

  --unordered   Sort both DataFrames lexicographically by all columns before
                comparing — for set-semantics operators (Intersect, Difference,
                SymmetricDifference) and joins whose JVM emit order differs
                from the pandas equivalent. Without this flag, the comparator
                matches rows positionally after reset_index(drop=True).

Exit 0  - DataFrames equal (modulo row/column order if --unordered; column
          order ignored either way; dtype loose; rtol=1e-5 for floats)
Exit 1  - DataFrames differ; pandas diff message on stderr
Exit 2  - Bad invocation
"""
import sys

import pandas as pd


def main() -> None:
    args = sys.argv[1:]
    unordered = False
    if args and args[0] == "--unordered":
        unordered = True
        args = args[1:]
    if len(args) != 2:
        print(
            f"usage: {sys.argv[0]} [--unordered] <actual.jsonl> <expected.jsonl>",
            file=sys.stderr,
        )
        sys.exit(2)

    actual_path, expected_path = args[0], args[1]
    actual = pd.read_json(actual_path, lines=True)
    expected = pd.read_json(expected_path, lines=True)

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
