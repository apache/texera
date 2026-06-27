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

import { describe, expect, test, beforeAll, afterEach } from "bun:test";
import { executeOperatorAndFormat } from "./workflow-execution-tools";
import { WorkflowState } from "../workflow-state";
import { WorkflowSystemMetadata } from "../util/workflow-system-metadata";
import type { ExecutionRequestParams } from "../../types/agent";
import type { OperatorInfo, SyncExecutionResult } from "../../types/execution";
import type { OperatorPredicate } from "../../types/workflow";

const realFetch = globalThis.fetch;

let captured: { url: string; init: any } | undefined;

function mockFetch(responder: () => Response): void {
  globalThis.fetch = (async (input: any, init: any) => {
    captured = { url: String(input), init };
    return responder();
  }) as unknown as typeof fetch;
}

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), { status: 200 });
}

afterEach(() => {
  globalThis.fetch = realFetch;
  captured = undefined;
});

const PARAMS: ExecutionRequestParams = { userToken: "tok", workflowId: 9, computingUnitId: 0 };

function sourceOperator(): OperatorPredicate {
  return {
    operatorID: "op1",
    operatorType: "CSVFileScan",
    operatorVersion: "1",
    operatorProperties: {},
    inputPorts: [],
    outputPorts: [{ portID: "output-0", displayName: "", disallowMultiInputs: false, isDynamicPort: false }],
    showAdvanced: false,
  } as OperatorPredicate;
}

beforeAll(() => {
  // Seed the singleton so schema/connection validation passes for CSVFileScan.
  WorkflowSystemMetadata.getInstance().loadFromMetadata({
    operators: [
      {
        operatorType: "CSVFileScan",
        jsonSchema: { type: "object", properties: { fileName: { type: "string" } }, required: [] },
        additionalMetadata: { userFriendlyName: "CSV", operatorGroupName: "source", inputPorts: [], outputPorts: [{}] },
        operatorVersion: "1",
      },
    ],
    groups: [],
  });
});

describe("executeOperatorAndFormat", () => {
  test("formats a successful execution result with table shape and rows", async () => {
    const ws = new WorkflowState();
    ws.addOperator(sourceOperator());

    const opInfo: OperatorInfo = {
      state: "Completed",
      inputTuples: 0,
      outputTuples: 2,
      resultMode: "table",
      result: [{ a: 1 }, { a: 2 }],
      totalRowCount: 2,
    };
    mockFetch(() =>
      jsonResponse({ success: true, state: "Completed", operators: { op1: opInfo } } as SyncExecutionResult)
    );

    const seen: string[] = [];
    const output = await executeOperatorAndFormat(ws, PARAMS, "op1", { onResult: opId => seen.push(opId) });

    expect(captured?.url).toBe("http://localhost:8085/api/execution/9/0/run");
    expect(output).toContain("Executed operator op1");
    expect(output).toContain("Output table shape: (2, 1)");
    expect(output).toContain("a"); // header
    expect(seen).toContain("op1");
  });

  test("returns a validation error without calling the backend for an unconnected operator", async () => {
    const ws = new WorkflowState();
    // An operator type that requires an input port but has none connected.
    ws.addOperator({
      ...sourceOperator(),
      operatorType: "CSVFileScan",
      inputPorts: [{ portID: "input-0", displayName: "in", disallowMultiInputs: true, isDynamicPort: false }],
    } as OperatorPredicate);

    let fetched = false;
    mockFetch(() => {
      fetched = true;
      return jsonResponse({ success: true, state: "Completed", operators: {} });
    });

    const output = await executeOperatorAndFormat(ws, PARAMS, "op1", {});

    expect(output).toContain("[ERROR]");
    expect(fetched).toBe(false);
  });

  test("surfaces a backend execution failure as an error result", async () => {
    const ws = new WorkflowState();
    ws.addOperator(sourceOperator());

    mockFetch(() =>
      jsonResponse({
        success: false,
        state: "Failed",
        operators: { op1: { state: "Failed", inputTuples: 0, outputTuples: 0, resultMode: "table", error: "boom" } },
      } as SyncExecutionResult)
    );

    const output = await executeOperatorAndFormat(ws, PARAMS, "op1", {});

    expect(output).toContain("[ERROR]");
    expect(output).toContain("boom");
  });
});
