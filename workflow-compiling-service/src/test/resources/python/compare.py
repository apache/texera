#!/usr/bin/env python3
"""
Compare two JSONL DataFrames produced by the translation validation harness.

Usage: compare.py <actual.jsonl> <expected.jsonl>

Exit 0  - DataFrames equal (modulo row/column order and dtype; rtol=1e-5 for floats)
Exit 1  - DataFrames differ; pandas diff message on stderr
Exit 2  - Bad invocation
"""
import sys

import pandas as pd


def main() -> None:
    if len(sys.argv) != 3:
        print(
            f"usage: {sys.argv[0]} <actual.jsonl> <expected.jsonl>",
            file=sys.stderr,
        )
        sys.exit(2)

    actual_path, expected_path = sys.argv[1], sys.argv[2]
    actual = pd.read_json(actual_path, lines=True)
    expected = pd.read_json(expected_path, lines=True)

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
