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
import { WorkflowState } from "../workflow-state";
import type { OperatorInfo } from "../../types/execution";
import type { OperatorPredicate, OperatorLink, PortDescription } from "../../types/workflow";

function makeOpInfo(overrides: Partial<OperatorInfo> = {}): OperatorInfo {
  return {
    state: "completed",
    inputTuples: 0,
    outputTuples: 0,
    resultMode: "table",
    ...overrides,
  };
}

function makeOperator(id: string, inputPortIDs: string[] = []): OperatorPredicate {
  const inputPorts: PortDescription[] = inputPortIDs.map((portID, i) => ({
    portID,
    displayName: `Input ${i}`,
  }));
  return {
    operatorID: id,
    operatorType: "TestOp",
    operatorVersion: "1.0",
    operatorProperties: {},
    inputPorts,
    outputPorts: [{ portID: "output-0", displayName: "Output 0" }],
    showAdvanced: false,
  };
}

function makeLink(linkID: string, source: [string, string], target: [string, string]): OperatorLink {
  return {
    linkID,
    source: { operatorID: source[0], portID: source[1] },
    target: { operatorID: target[0], portID: target[1] },
  };
}

const EMPTY_STATE = new WorkflowState();

describe("formatOperatorResult - early returns", () => {
  test("returns [ERROR] prefix when error field is set", () => {
    const out = formatOperatorResult("op1", makeOpInfo({ error: "boom" }), EMPTY_STATE);
    expect(out).toBe("[ERROR] boom");
  });

  test("treats empty-string error as falsy and continues to result path", () => {
    const out = formatOperatorResult("op1", makeOpInfo({ error: "" }), EMPTY_STATE);
    expect(out).not.toContain("[ERROR]");
    expect(out).toContain("(no result data)");
  });

  test("returns (no result data) when result is undefined", () => {
    const out = formatOperatorResult("op1", makeOpInfo(), EMPTY_STATE);
    expect(out).toBe("(no result data)");
  });

  test("returns (no result data) when result is not an array", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({ result: { rows: [] } as unknown as Record<string, any>[] }),
      EMPTY_STATE
    );
    expect(out).toBe("(no result data)");
  });

  test("empty array result emits brief summary plus zero-column shape only", () => {
    const out = formatOperatorResult("op1", makeOpInfo({ result: [], outputTuples: 0 }), EMPTY_STATE);
    expect(out.split("\n")).toEqual(["Executed operator op1", "Output table shape: (0, 0)"]);
  });
});

describe("formatOperatorResult - table shape and metadata", () => {
  test("uses outputTuples for row count when totalRowCount missing", () => {
    const out = formatOperatorResult("op1", makeOpInfo({ outputTuples: 7, result: [{ a: 1, b: 2 }] }), EMPTY_STATE);
    expect(out).toContain("Output table shape: (7, 2)");
  });

  test("totalRowCount overrides outputTuples in output shape", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({ outputTuples: 7, totalRowCount: 999, result: [{ a: 1, b: 2 }] }),
      EMPTY_STATE
    );
    expect(out).toContain("Output table shape: (999, 2)");
  });

  test("filters internal __is_visualization__ key from outer column count", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 1,
        result: [{ __is_visualization__: true, "html-content": "<x/>" }],
      }),
      EMPTY_STATE
    );
    // 1 visible column ("html-content") since __is_visualization__ is filtered.
    expect(out).toContain("Output table shape: (1, 1)");
  });

  test("appends warnings after metadata lines", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 1,
        result: [{ a: 1 }],
        warnings: ["truncated to 1 row", "something else"],
      }),
      EMPTY_STATE
    );
    const lines = out.split("\n");
    expect(lines[0]).toBe("Executed operator op1");
    expect(lines[1]).toBe("Output table shape: (1, 1)");
    expect(lines[2]).toBe("truncated to 1 row");
    expect(lines[3]).toBe("something else");
  });
});

describe("formatOperatorResult - input port metadata", () => {
  test("omits input metadata when inputPortShapes is missing", () => {
    const out = formatOperatorResult("op1", makeOpInfo({ outputTuples: 1, result: [{ a: 1 }] }), EMPTY_STATE);
    expect(out).not.toContain("Input operator");
  });

  test("omits input metadata when inputPortShapes is empty", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({ outputTuples: 1, result: [{ a: 1 }], inputPortShapes: [] }),
      EMPTY_STATE
    );
    expect(out).not.toContain("Input operator");
  });

  test("falls back to inputN placeholder when no upstream link matches the port", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 1,
        result: [{ a: 1 }],
        inputPortShapes: [{ portIndex: 0, rows: 5, columns: 3 }],
      }),
      EMPTY_STATE
    );
    expect(out).toContain("Input operator(table shape): input0(5, 3)");
  });

  test("uses upstream operator id when an input link matches the port", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("upstream"));
    state.addOperator(makeOperator("op1", ["input-0"]));
    state.addLink(makeLink("l1", ["upstream", "output-0"], ["op1", "input-0"]));

    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 4,
        result: [{ a: 1, b: 2 }],
        inputPortShapes: [{ portIndex: 0, rows: 10, columns: 2 }],
      }),
      state
    );
    expect(out).toContain("Input operator(table shape): upstream(10, 2)");
  });

  test("sorts multiple input ports by portIndex regardless of input order", () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("up0"));
    state.addOperator(makeOperator("up1"));
    state.addOperator(makeOperator("op1", ["input-0", "input-1"]));
    state.addLink(makeLink("l0", ["up0", "output-0"], ["op1", "input-0"]));
    state.addLink(makeLink("l1", ["up1", "output-0"], ["op1", "input-1"]));

    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 1,
        result: [{ a: 1 }],
        inputPortShapes: [
          { portIndex: 1, rows: 2, columns: 2 },
          { portIndex: 0, rows: 1, columns: 1 },
        ],
      }),
      state
    );
    expect(out).toContain("Input operator(table shape): up0(1, 1), up1(2, 2)");
  });
});

describe("formatOperatorResult - visualization rows", () => {
  test("strips html-content and json-content payloads when row is flagged as visualization", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 1,
        result: [
          {
            __is_visualization__: true,
            "html-content": "<div>hidden</div>",
            "json-content": '{"big":1}',
            label: "chart",
          },
        ],
      }),
      EMPTY_STATE
    );
    expect(out).toContain("<skipped: visualization content>");
    expect(out).not.toContain("<div>hidden</div>");
    expect(out).not.toContain('{"big":1}');
    expect(out).toContain("chart");
  });

  test("__is_visualization__ false leaves the visualization-only fields untouched", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 1,
        result: [{ __is_visualization__: false, "html-content": "<keep/>" }],
      }),
      EMPTY_STATE
    );
    expect(out).toContain("<keep/>");
    expect(out).not.toContain("<skipped: visualization content>");
  });
});

describe("jsonToTableFormat - cell coercion via formatOperatorResult", () => {
  function tableLines(opInfo: Partial<OperatorInfo>): string[] {
    const out = formatOperatorResult("op1", makeOpInfo({ outputTuples: 1, ...opInfo }), EMPTY_STATE);
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
  test("inserts ... separator when __row_index__ skips ahead", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 2,
        result: [
          { __row_index__: 0, v: "a" },
          { __row_index__: 5, v: "b" },
        ],
      }),
      EMPTY_STATE
    );
    const lines = out.split("\n");
    // header, row0, gap marker, row5
    expect(lines[lines.length - 4]).toBe("\tv");
    expect(lines[lines.length - 3]).toBe("0\ta");
    expect(lines[lines.length - 2]).toBe("...\t...");
    expect(lines[lines.length - 1]).toBe("5\tb");
  });

  test("no separator is emitted between consecutive __row_index__ values", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({
        outputTuples: 2,
        result: [
          { __row_index__: 0, v: "a" },
          { __row_index__: 1, v: "b" },
        ],
      }),
      EMPTY_STATE
    );
    expect(out).not.toContain("...\t...");
  });

  test("non-zero starting __row_index__ does not emit a leading gap marker", () => {
    const out = formatOperatorResult(
      "op1",
      makeOpInfo({ outputTuples: 1, result: [{ __row_index__: 9, v: "z" }] }),
      EMPTY_STATE
    );
    expect(out).not.toContain("...\t...");
    expect(out.endsWith("9\tz")).toBe(true);
  });
});
