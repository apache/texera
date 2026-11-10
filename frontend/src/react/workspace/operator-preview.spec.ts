/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { describe, expect, it } from "vitest";
import { formatPreviewCell, describePreview } from "./operator-preview";
import { ExecutionState } from "../../core/workspace/types/execute-workflow.interface";
const base = { state: ExecutionState.Uninitialized, errors: [], console: {}, results: {}, materializedResults: {} };
describe("operator hover preview", () => {
  it("preserves zero, Unicode, objects and the24-character preview boundary", () => {
    expect(formatPreviewCell(0)).toBe("0");
    expect(formatPreviewCell(null)).toBe("");
    expect(formatPreviewCell({ a: "日本語" })).toBe('{"a":"日本語"}');
    expect(formatPreviewCell("x".repeat(24))).toHaveLength(24);
    expect(formatPreviewCell("x".repeat(25))).toBe("x".repeat(23) + "…");
  });
  it("shows first-port schema/counts and gives errors priority over results", () => {
    const snapshot = {
      ...base,
      results: { a: { mode: { type: "PaginationMode" as const }, totalNumTuples: 0, dirtyPageIndices: [] } },
      compilation: {
        operatorOutputSchemas: {
          a: {
            "0_false": Array.from({ length: 8 }, (_, i) => ({
              attributeName: `c${i}`,
              attributeType: "string" as const,
            })),
          },
        },
        operatorErrors: {},
      },
    };
    const preview = describePreview("a", snapshot, 2);
    expect(preview).toMatchObject({ kind: "table", columns: ["c0", "c1", "c2", "c3", "c4", "c5"], moreColumns: 2 });
    expect(preview.summary).toBe("0 rows · 8 columns · first of 2 outputs");
    const failure = { ...snapshot, console: { a: [{ msgType: { name: "ERROR" }, title: "Bad tuple" }] } };
    expect(describePreview("a", failure as unknown as typeof snapshot, 2)).toMatchObject({
      kind: "error",
      detail: "Bad tuple",
    });
  });
  it("distinguishes a chart, no run and a result not kept by a completed run", () => {
    expect(describePreview("a", base, 1).detail).toMatch(/Run the workflow/);
    expect(describePreview("a", { ...base, state: ExecutionState.Completed }, 1).detail).toMatch(/eye icon/);
    expect(
      describePreview("a", { ...base, results: { a: { mode: { type: "SetSnapshotMode" }, table: [] } } }, 1).kind
    ).toBe("chart");
  });
});
