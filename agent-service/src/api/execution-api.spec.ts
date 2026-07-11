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
import { adaptLegacySyncExecutionResult } from "./execution-api";
import {
  ConsoleMessageType,
  OperatorResultMode,
  OperatorState,
  WorkflowExecutionState,
  WorkflowFatalErrorType,
} from "../types/execution";

describe("adaptLegacySyncExecutionResult", () => {
  test("normalizes sampled table rows, row gaps, counts, and console messages", () => {
    const summary = adaptLegacySyncExecutionResult({
      success: true,
      state: "Completed",
      operators: {
        target: {
          state: "Completed",
          inputTuples: 10,
          outputTuples: 10,
          resultMode: "table",
          result: [
            { __row_index__: 2, name: "alice", score: 1, active: true },
            { __row_index__: 9, name: null, score: { nested: "value" }, active: false },
          ],
          totalRowCount: 10,
          displayedRows: 2,
          truncated: true,
          consoleLogs: [{ msgType: "PRINT", title: "WARNING: sampled", message: "sampled" }],
          warnings: ["WARNING: sampled"],
        },
      },
    });

    expect(summary).toMatchObject({
      success: true,
      state: WorkflowExecutionState.COMPLETED,
      errors: [],
      operators: {
        target: {
          state: OperatorState.COMPLETED,
          errorMessages: [],
          consoleMessages: [{ msgType: ConsoleMessageType.PRINT, title: "WARNING: sampled", message: "sampled" }],
          resultSummary: {
            resultMode: OperatorResultMode.TABLE,
            totalTuplesCount: 10,
          },
        },
      },
    });
    expect(summary.operators.target.resultSummary?.sampleTuples).toEqual([
      [
        2,
        {
          schema: {
            attributes: [
              { attributeName: "name", attributeType: "string" },
              { attributeName: "score", attributeType: "string" },
              { attributeName: "active", attributeType: "string" },
            ],
          },
          fields: ["alice", "1", "true"],
        },
      ],
      [
        9,
        {
          schema: {
            attributes: [
              { attributeName: "name", attributeType: "string" },
              { attributeName: "score", attributeType: "string" },
              { attributeName: "active", attributeType: "string" },
            ],
          },
          fields: [null, '{"nested":"value"}', "false"],
        },
      ],
    ]);
  });

  test("normalizes visualization markers without exposing backend metadata columns", () => {
    const summary = adaptLegacySyncExecutionResult({
      success: true,
      state: "Completed",
      operators: {
        chart: {
          state: "Completed",
          inputTuples: 1,
          outputTuples: 1,
          resultMode: "visualization",
          result: [{ __is_visualization__: true, "html-content": "<p>chart</p>" }],
          totalRowCount: 1,
        },
      },
    });

    expect(summary.operators.chart.resultSummary).toEqual({
      resultMode: OperatorResultMode.VISUALIZATION,
      sampleTuples: [
        [
          0,
          {
            schema: { attributes: [{ attributeName: "html-content", attributeType: "string" }] },
            fields: ["<p>chart</p>"],
          },
        ],
      ],
      totalTuplesCount: 1,
    });
  });

  test("turns operator and workflow error strings into execution failures", () => {
    const summary = adaptLegacySyncExecutionResult({
      success: false,
      state: "Failed",
      operators: {
        broken: {
          state: "Failed",
          inputTuples: 0,
          outputTuples: 0,
          resultMode: "table",
          error: "operator exploded",
        },
      },
      errors: ["workflow stopped"],
    });

    expect(summary.operators.broken.errorMessages[0]).toMatchObject({
      type: { name: WorkflowFatalErrorType.EXECUTION_FAILURE },
      message: "operator exploded",
      operatorId: "broken",
    });
    expect(summary.errors).toHaveLength(1);
    expect(summary.errors[0]).toMatchObject({
      type: { name: WorkflowFatalErrorType.EXECUTION_FAILURE },
      message: "workflow stopped",
      operatorId: "",
    });
  });

  test("deduplicates the legacy compilation error projections", () => {
    const summary = adaptLegacySyncExecutionResult({
      success: false,
      state: "CompilationFailed",
      operators: {},
      compilationErrors: { error: "bad schema" },
      errors: ["bad schema"],
    });

    expect(summary.state).toBe(WorkflowExecutionState.COMPILATION_FAILED);
    expect(summary.errors).toHaveLength(1);
    expect(summary.errors[0]).toMatchObject({
      type: { name: WorkflowFatalErrorType.COMPILATION_ERROR },
      message: "bad schema",
    });
  });

  test("keeps an absent materialized result absent and defaults optional errors to empty", () => {
    const summary = adaptLegacySyncExecutionResult({
      success: true,
      state: "Completed",
      operators: {
        sink: {
          state: "Completed",
          inputTuples: 0,
          outputTuples: 0,
          resultMode: "table",
        },
      },
    });

    expect(summary.errors).toEqual([]);
    expect(summary.operators.sink.resultSummary).toBeUndefined();
    expect(summary.operators.sink.consoleMessages).toBeUndefined();
  });

  test("rejects a malformed response instead of trusting an unchecked cast", () => {
    expect(() => adaptLegacySyncExecutionResult({ success: "yes", state: "Completed", operators: {} })).toThrow(
      "Invalid legacy sync-execution response"
    );
  });

  test("rejects malformed row-index metadata", () => {
    expect(() =>
      adaptLegacySyncExecutionResult({
        success: true,
        state: "Completed",
        operators: {
          target: {
            state: "Completed",
            inputTuples: 1,
            outputTuples: 1,
            resultMode: "table",
            result: [{ __row_index__: "first", value: "x" }],
          },
        },
      })
    ).toThrow("Invalid __row_index__ for sampled row 0");
  });
});
