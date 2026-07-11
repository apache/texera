/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { describe, expect, test } from "bun:test";
import { formatOperatorResult } from "./result-formatting";
import {
  ConsoleMessageType,
  OperatorState,
  WorkflowFatalErrorType,
  type IndexedTuple,
  type OperatorExecutionSummary,
  type WebOutputMode,
  type WorkflowFatalError,
  type Tuple,
} from "../../types/execution";

// Build an engine-style Tuple with an all-STRING schema from a column->value record,
// matching the adapter's representation of backend-truncated samples.
function recordToTuple(record: Record<string, any>): Tuple {
  return {
    schema: { attributes: Object.keys(record).map(name => ({ attributeName: name, attributeType: "string" })) },
    fields: Object.values(record),
  };
}

function toSampleTuples(records: ReadonlyArray<Record<string, any>>): ReadonlyArray<IndexedTuple> {
  return records.map((record, rowIndex) => [rowIndex, recordToTuple(record)]);
}

interface OpInfoOverrides {
  readonly state?: OperatorState;
  readonly error?: string;
  readonly outputTuples?: number;
  readonly totalTuplesCount?: number;
  readonly warnings?: ReadonlyArray<string>;
  readonly result?: ReadonlyArray<Record<string, any>>;
  readonly sampleTuples?: ReadonlyArray<IndexedTuple>;
  readonly resultMode?: WebOutputMode;
}

function makeExecutionFailure(message: string): WorkflowFatalError {
  return {
    type: { name: WorkflowFatalErrorType.EXECUTION_FAILURE },
    timestamp: { seconds: 0, nanos: 0 },
    message,
    details: "",
    operatorId: "",
    workerId: "",
  };
}

function makeOpInfo(overrides: OpInfoOverrides = {}): OperatorExecutionSummary {
  const hasResult = overrides.result !== undefined || overrides.sampleTuples !== undefined;
  const resultSummary = hasResult
    ? {
        resultMode: overrides.resultMode ?? ({ type: "PaginationMode" } as const),
        // Non-arrays are passed through to exercise the "(no result data)" guard.
        sampleTuples:
          overrides.sampleTuples ??
          (Array.isArray(overrides.result) ? toSampleTuples(overrides.result) : (overrides.result as any)),
        totalTuplesCount: overrides.totalTuplesCount ?? overrides.outputTuples ?? 0,
      }
    : undefined;
  const consoleMessages = overrides.warnings?.map(warning => ({
    msgType: ConsoleMessageType.PRINT,
    title: warning,
    message: "",
  }));

  return {
    state: overrides.state ?? OperatorState.COMPLETED,
    errorMessages: overrides.error ? [makeExecutionFailure(overrides.error)] : [],
    ...(resultSummary ? { resultSummary } : {}),
    // Warnings are derived from console messages whose title is "WARNING: ...".
    ...(consoleMessages ? { consoleMessages } : {}),
  };
}

describe("formatOperatorResult - early returns", () => {
  test("returns [ERROR] prefix when error field is set", () => {
    const out = formatOperatorResult("op1", makeOpInfo({ error: "boom" }));
    expect(out).toBe("[ERROR] boom");
  });

  test("treats empty-string error as falsy and continues to result path", () => {
    const out = formatOperatorResult("op1", makeOpInfo({ error: "" }));
    expect(out).not.toContain("[ERROR]");
    expect(out).toContain("(no result data)");
  });

  test("returns (no result data) when result is undefined", () => {
    const out = formatOperatorResult("op1", makeOpInfo());
    expect(out).toBe("(no result data)");
  });

  test("returns (no result data) when result is not an array", () => {
    const out = formatOperatorResult("op1", makeOpInfo({ result: { rows: [] } as unknown as Record<string, any>[] }));
    expect(out).toBe("(no result data)");
  });

  test("empty array result emits brief summary plus zero-column shape only", () => {
    const out = formatOperatorResult("op1", makeOpInfo({ result: [], outputTuples: 0 }));
    expect(out.split("\n")).toEqual(["Executed operator op1", "Output table shape: (0, 0)"]);
  });
});

describe("formatOperatorResult - table shape and metadata", () => {
  test("uses outputTuples for row count when totalTuplesCount missing", () => {
    const out = formatOperatorResult("op1", makeOpInfo({ outputTuples: 7, result: [{ a: 1, b: 2 }] }));
    expect(out).toContain("Output table shape: (7, 2)");
  });

  test("totalTuplesCount overrides outputTuples in output shape", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({ outputTuples: 7, totalTuplesCount: 999, result: [{ a: 1, b: 2 }] })
    );
    expect(out).toContain("Output table shape: (999, 2)");
  });

  test("counts every result tuple key as a column", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 1,
        result: [{ "html-content": "<x/>", label: "chart" }],
      })
    );
    expect(out).toContain("Output table shape: (1, 2)");
  });

  test("appends warnings after metadata lines", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 1,
        result: [{ a: 1 }],
        warnings: ["WARNING: truncated to 1 row", "WARNING: something else"],
      })
    );
    const lines = out.split("\n");
    expect(lines[0]).toBe("Executed operator op1");
    expect(lines[1]).toBe("Output table shape: (1, 1)");
    expect(lines[2]).toBe("WARNING: truncated to 1 row");
    expect(lines[3]).toBe("WARNING: something else");
  });
});

describe("formatOperatorResult - visualization rows", () => {
  test("strips html-content and json-content payloads when result mode is visualization", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 1,
        resultMode: { type: "SetSnapshotMode" },
        result: [
          {
            "html-content": "<div>hidden</div>",
            "json-content": '{"big":1}',
            label: "chart",
          },
        ],
      })
    );
    expect(out).toContain("<skipped: visualization content>");
    expect(out).not.toContain("<div>hidden</div>");
    expect(out).not.toContain('{"big":1}');
    expect(out).toContain("chart");
  });

  test("table result mode leaves visualization payload fields untouched", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 1,
        resultMode: { type: "PaginationMode" },
        result: [{ "html-content": "<keep/>" }],
      })
    );
    expect(out).toContain("<keep/>");
    expect(out).not.toContain("<skipped: visualization content>");
  });

  test("table rows render all tuple columns and shape agrees", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 1,
        result: [{ value: 1 }],
      })
    );
    const lines = out.split("\n");
    expect(out).toContain("Output table shape: (1, 1)");
    // Header line is the third line (after brief summary and shape line).
    expect(lines[2]).toBe("\tvalue");
    expect(lines[3]).toBe("0\t1");
  });
});

describe("jsonToTableFormat - cell coercion via formatOperatorResult", () => {
  function tableLines(opInfo: OpInfoOverrides): string[] {
    const out = formatOperatorResult("op1", makeOpInfo({ outputTuples: 1, ...opInfo }));
    // Skip brief summary + shape line.
    return out.split("\n").slice(2);
  }

  test("null is rendered as NaN, undefined as empty cell", () => {
    const [header, row] = tableLines({ result: [{ a: null, b: undefined }] });
    expect(header).toBe("\ta\tb");
    expect(row).toBe("0\tNaN\t");
  });

  test('string "NULL" sentinel is normalized to NaN', () => {
    const [, row] = tableLines({ result: [{ x: "NULL" }] });
    expect(row).toBe("0\tNaN");
  });

  test("number and boolean cells are stringified directly", () => {
    const [, row] = tableLines({ result: [{ n: 3.5, b: true, f: false }] });
    expect(row).toBe("0\t3.5\ttrue\tfalse");
  });

  test("tabs and newlines inside string cells are escape-encoded", () => {
    const [, row] = tableLines({ result: [{ s: "a\tb\nc" }] });
    expect(row).toBe("0\ta\\tb\\nc");
  });

  test("object and array cells are JSON-stringified", () => {
    const [, row] = tableLines({ result: [{ obj: { k: 1 }, arr: [1, 2] }] });
    expect(row).toBe('0\t{"k":1}\t[1,2]');
  });
});

describe("jsonToTableFormat - row index gaps", () => {
  test("inserts ... separator when rowIndex skips ahead", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 2,
        sampleTuples: [
          [0, recordToTuple({ v: "a" })],
          [5, recordToTuple({ v: "b" })],
        ],
      })
    );
    const lines = out.split("\n");
    // header, row0, gap marker, row5
    expect(lines[lines.length - 4]).toBe("\tv");
    expect(lines[lines.length - 3]).toBe("0\ta");
    expect(lines[lines.length - 2]).toBe("...\t...");
    expect(lines[lines.length - 1]).toBe("5\tb");
  });

  test("no separator is emitted between consecutive rowIndex values", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 2,
        sampleTuples: [
          [0, recordToTuple({ v: "a" })],
          [1, recordToTuple({ v: "b" })],
        ],
      })
    );
    expect(out).not.toContain("...\t...");
  });

  test("non-zero starting rowIndex does not emit a leading gap marker", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({ outputTuples: 1, sampleTuples: [[9, recordToTuple({ v: "z" })]] })
    );
    expect(out).not.toContain("...\t...");
    expect(out.endsWith("9\tz")).toBe(true);
  });
});
