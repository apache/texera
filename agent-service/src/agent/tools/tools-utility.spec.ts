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
import {
  createToolResult,
  createErrorResult,
  formatAddOperatorResult,
  formatModifyOperatorResult,
  formatExecuteOperatorResult,
  formatOperatorError,
  formatOperatorOutput,
  formatSampleTuplesAsTsv,
  getOperatorErrorText,
  tupleColumns,
  tupleToRecord,
} from "./tools-utility";
import {
  OperatorState,
  WorkflowFatalErrorType,
  type IndexedTuple,
  type OperatorExecutionSummary,
  type Tuple,
  type WebOutputMode,
  type WorkflowFatalError,
} from "../../types/execution";

function makeFatal(message: string): WorkflowFatalError {
  return {
    type: { name: WorkflowFatalErrorType.EXECUTION_FAILURE },
    timestamp: { seconds: 0, nanos: 0 },
    message,
    details: "",
    operatorId: "",
    workerId: "",
  };
}

// Build an engine-style Tuple with an all-STRING schema from a column->value record.
function recordToTuple(record: Record<string, unknown>): Tuple {
  return {
    schema: { attributes: Object.keys(record).map(name => ({ attributeName: name, attributeType: "string" })) },
    fields: Object.values(record),
  };
}

function toSampleTuples(records: ReadonlyArray<Record<string, unknown>>): ReadonlyArray<IndexedTuple> {
  return records.map((record, rowIndex) => [rowIndex, recordToTuple(record)]);
}

function makeResultOpInfo(
  records: ReadonlyArray<Record<string, unknown>>,
  resultMode: WebOutputMode = { type: "PaginationMode" }
): OperatorExecutionSummary {
  return {
    state: OperatorState.COMPLETED,
    errorMessages: [],
    resultSummary: { resultMode, sampleTuples: toSampleTuples(records), totalTuplesCount: records.length },
  };
}

describe("getOperatorErrorText", () => {
  test("joins the messages of every fatal error", () => {
    const opInfo = { state: OperatorState.FAILED, errorMessages: [makeFatal("e1"), makeFatal("e2")] };
    expect(getOperatorErrorText(opInfo)).toBe("e1; e2");
  });

  test("ignores errors whose message is empty", () => {
    const opInfo = { state: OperatorState.FAILED, errorMessages: [makeFatal(""), makeFatal("real")] };
    expect(getOperatorErrorText(opInfo)).toBe("real");
  });

  test("returns an empty string when every error message is empty", () => {
    const opInfo = { state: OperatorState.COMPLETED, errorMessages: [makeFatal("")] };
    expect(getOperatorErrorText(opInfo)).toBe("");
  });

  test("returns an empty string when there are no errors", () => {
    const opInfo = { state: OperatorState.COMPLETED, errorMessages: [] };
    expect(getOperatorErrorText(opInfo)).toBe("");
  });
});

describe("tupleColumns / tupleToRecord", () => {
  const tuple: Tuple = {
    schema: {
      attributes: [
        { attributeName: "z", attributeType: "string" },
        { attributeName: "a", attributeType: "string" },
      ],
    },
    fields: [1, 2],
  };

  test("tupleColumns returns column names in schema order", () => {
    expect(tupleColumns(tuple)).toEqual(["z", "a"]);
  });

  test("tupleToRecord projects fields back onto their column names", () => {
    expect(tupleToRecord(tuple)).toEqual({ z: 1, a: 2 });
  });

  test("tupleColumns is empty for a schema with no attributes", () => {
    expect(tupleColumns({ schema: { attributes: [] }, fields: [] })).toEqual([]);
  });
});

describe("formatSampleTuplesAsTsv", () => {
  test("returns empty string for no tuples or tuples with no columns", () => {
    expect(formatSampleTuplesAsTsv([])).toBe("");
    expect(formatSampleTuplesAsTsv([[0, { schema: { attributes: [] }, fields: [] }]])).toBe("");
  });

  test("formats tab-separated header and row values with index column", () => {
    const rows = [
      { colA: "val1", colB: 10, colC: null, colD: true },
      { colA: "line\nbreak", colB: 20, colC: "NULL", colD: false },
    ];
    const lines = formatSampleTuplesAsTsv(toSampleTuples(rows)).split("\n");
    expect(lines[0]).toBe("\tcolA\tcolB\tcolC\tcolD");
    expect(lines[1]).toBe("0\tval1\t10\tNaN\ttrue");
    expect(lines[2]).toBe("1\tline\\nbreak\t20\tNaN\tfalse");
  });
});

describe("formatOperatorOutput - charLimit truncation", () => {
  const rows = Array.from({ length: 100 }, (_, i) => ({ a: i, b: "x".repeat(20) }));
  const opInfo = makeResultOpInfo(rows);

  test("without charLimit keeps every row", () => {
    const out = formatOperatorOutput("op1", opInfo);
    expect(out).toContain("\n0\t0\t");
    expect(out).toContain("\n50\t50\t");
    expect(out).toContain("\n99\t99\t");
  });

  test("charLimit keeps head and tail rows and drops the middle", () => {
    const out = formatOperatorOutput("op1", opInfo, 300);
    expect(out).toContain("Output table shape: (100, 2)");
    expect(out).toContain("\ta\tb");
    expect(out).toContain("\n0\t0\t");
    expect(out).toContain("\n99\t99\t");
    expect(out).not.toContain("\n50\t50\t");
  });

  test("charLimit larger than the table leaves it untouched", () => {
    expect(formatOperatorOutput("op1", opInfo, 1_000_000)).toBe(formatOperatorOutput("op1", opInfo));
  });

  test("tiny charLimit still keeps at least one row", () => {
    const out = formatOperatorOutput("op1", opInfo, 1);
    expect(out).toContain("\n0\t0\t");
  });

  test("visualization content is skipped even when charLimit is set", () => {
    const html = "<html>" + "y".repeat(5000) + "</html>";
    const vizInfo = makeResultOpInfo([{ "html-content": html }], { type: "SetSnapshotMode" });
    const out = formatOperatorOutput("op1", vizInfo, 100_000);
    expect(out).not.toContain("<html>");
    expect(out).toContain("<skipped: visualization content>");
  });
});

describe("createToolResult", () => {
  test("returns the message unchanged", () => {
    expect(createToolResult("ok")).toBe("ok");
  });

  test("preserves an empty string", () => {
    expect(createToolResult("")).toBe("");
  });
});

describe("createErrorResult", () => {
  test("wraps the error with the [ERROR] prefix", () => {
    expect(createErrorResult("boom")).toBe("[ERROR] boom");
  });

  test("keeps the prefix even for empty error text", () => {
    expect(createErrorResult("")).toBe("[ERROR] ");
  });
});

describe("formatExecuteOperatorResult", () => {
  test("renders the operator id in the standard executed message", () => {
    expect(formatExecuteOperatorResult("op-1")).toBe("Executed operator op-1");
  });
});

describe("formatOperatorError", () => {
  test("includes both operator id and error text", () => {
    expect(formatOperatorError("op-1", "bad input")).toBe("Error on operator op-1: bad input");
  });

  test("retains the trailing colon and space when error is empty", () => {
    expect(formatOperatorError("op-1", "")).toBe("Error on operator op-1: ");
  });
});

describe("formatAddOperatorResult", () => {
  test("emits only the summary when no links are provided", () => {
    expect(formatAddOperatorResult("op-1", 2, 1)).toBe("Added operator op-1, input ports: 2, output ports: 1");
  });

  test("appends created links after the summary when only createdLinks is provided", () => {
    const out = formatAddOperatorResult("op-1", 1, 1, [{ source: "u", target: "op-1" }]);
    expect(out).toBe("Added operator op-1, input ports: 1, output ports: 1, created links: [u --> op-1]");
  });

  test("appends deleted links after the summary when only deletedLinks is provided", () => {
    const out = formatAddOperatorResult("op-1", 1, 1, undefined, [{ source: "u", target: "op-1" }]);
    expect(out).toBe("Added operator op-1, input ports: 1, output ports: 1, deleted links: [u --> op-1]");
  });

  test("places deleted-links segment before created-links segment when both are provided", () => {
    const out = formatAddOperatorResult(
      "op-1",
      1,
      1,
      [{ source: "u", target: "op-1" }],
      [{ source: "old", target: "op-1" }]
    );
    expect(out).toBe(
      "Added operator op-1, input ports: 1, output ports: 1" +
        ", deleted links: [old --> op-1]" +
        ", created links: [u --> op-1]"
    );
    expect(out.indexOf("deleted links")).toBeLessThan(out.indexOf("created links"));
  });

  test("treats empty link arrays as absent (length-0 short-circuit)", () => {
    const out = formatAddOperatorResult("op-1", 0, 0, [], []);
    expect(out).toBe("Added operator op-1, input ports: 0, output ports: 0");
    expect(out).not.toContain("links:");
  });

  test("joins multiple link descriptions with a comma separator", () => {
    const out = formatAddOperatorResult("op-1", 1, 1, [
      { source: "a", target: "op-1" },
      { source: "b", target: "op-1" },
    ]);
    expect(out).toContain("created links: [a --> op-1, b --> op-1]");
  });
});

describe("formatModifyOperatorResult", () => {
  test("emits the bare modified summary when no links are provided", () => {
    expect(formatModifyOperatorResult("op-1")).toBe("Operator op-1 modified");
  });

  test("appends created links when only createdLinks is provided", () => {
    const out = formatModifyOperatorResult("op-1", [{ source: "u", target: "op-1" }]);
    expect(out).toBe("Operator op-1 modified, created links: [u --> op-1]");
  });

  test("appends deleted links when only deletedLinks is provided", () => {
    const out = formatModifyOperatorResult("op-1", undefined, [{ source: "u", target: "op-1" }]);
    expect(out).toBe("Operator op-1 modified, deleted links: [u --> op-1]");
  });

  test("places deleted-links segment before created-links segment when both are provided", () => {
    const out = formatModifyOperatorResult(
      "op-1",
      [{ source: "new", target: "op-1" }],
      [{ source: "old", target: "op-1" }]
    );
    expect(out).toBe("Operator op-1 modified, deleted links: [old --> op-1], created links: [new --> op-1]");
    expect(out.indexOf("deleted links")).toBeLessThan(out.indexOf("created links"));
  });

  test("treats empty link arrays as absent", () => {
    const out = formatModifyOperatorResult("op-1", [], []);
    expect(out).toBe("Operator op-1 modified");
    expect(out).not.toContain("links:");
  });
});
