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

import { afterEach, beforeAll, describe, expect, test } from "bun:test";
import { executeOperatorAndFormat, type ExecutionConfig } from "./workflow-execution-tools";
import { WorkflowState } from "../workflow-state";
import { WorkflowUtilService } from "../util/workflow-utils";
import { WorkflowSystemMetadata } from "../util/workflow-system-metadata";
import { FIXTURE_METADATA } from "../util/metadata-fixture";
import type { OperatorInfo, SyncExecutionResult } from "../../types/execution";

const realFetch = globalThis.fetch;
let lastUrl = "";
let lastInit: RequestInit | undefined;

function stubFetch(result: SyncExecutionResult, status = 200): void {
  globalThis.fetch = (async (url: unknown, init?: RequestInit) => {
    lastUrl = String(url);
    lastInit = init;
    return new Response(JSON.stringify(result), { status });
  }) as unknown as typeof fetch;
}

// validateWorkflow / schema validation read from the process-wide singleton.
beforeAll(() => {
  WorkflowSystemMetadata.getInstance().loadFromMetadata(FIXTURE_METADATA);
});

afterEach(() => {
  globalThis.fetch = realFetch;
  lastUrl = "";
  lastInit = undefined;
});

function csvScanState(): { state: WorkflowState; operatorId: string } {
  const state = new WorkflowState();
  const util = new WorkflowUtilService(WorkflowSystemMetadata.getInstance(), state);
  let op = util.getNewOperatorPredicate("CSVFileScan");
  op = { ...op, operatorProperties: { ...op.operatorProperties, fileName: "data.csv" } };
  state.addOperator(op);
  return { state, operatorId: op.operatorID };
}

const baseConfig: ExecutionConfig = { userToken: "tok-abc", workflowId: 7, maxOperatorResultCharLimit: 2000 };

describe("executeOperatorAndFormat - guards", () => {
  test("errors when the workflow has no operators", async () => {
    const out = await executeOperatorAndFormat(new WorkflowState(), baseConfig, "missing");
    expect(out).toContain("[ERROR]");
    expect(out).toContain("workflow has no operators");
  });

  test("blocks execution on the target operator's validation errors", async () => {
    // A Filter with no inbound links fails the input-connection check.
    const state = new WorkflowState();
    const util = new WorkflowUtilService(WorkflowSystemMetadata.getInstance(), state);
    const filter = util.getNewOperatorPredicate("Filter");
    state.addOperator(filter);

    const out = await executeOperatorAndFormat(state, baseConfig, filter.operatorID);
    expect(out).toContain("[ERROR]");
    expect(out).toContain(`Operator ${filter.operatorID}`);
    expect(out).toContain("inputs");
  });
});

describe("executeOperatorAndFormat - successful run", () => {
  test("posts to the per-cuid run endpoint with a bearer token", async () => {
    const { state, operatorId } = csvScanState();
    const op: OperatorInfo = { state: "Completed", inputTuples: 0, outputTuples: 2, resultMode: "table", result: [] };
    stubFetch({ success: true, state: "Completed", operators: { [operatorId]: op } });

    await executeOperatorAndFormat(state, baseConfig, operatorId);

    expect(lastUrl).toMatch(/\/api\/execution\/7\/0\/run$/);
    expect(lastInit?.method).toBe("POST");
    expect((lastInit?.headers as Record<string, string>).Authorization).toBe("Bearer tok-abc");
    const body = JSON.parse(String(lastInit?.body));
    expect(body.logicalPlan.operators).toHaveLength(1);
    expect(body.targetOperatorIds).toContain(operatorId);
  });

  test("formats the result table and reports the output shape", async () => {
    const { state, operatorId } = csvScanState();
    const op: OperatorInfo = {
      state: "Completed",
      inputTuples: 0,
      outputTuples: 2,
      resultMode: "table",
      totalRowCount: 2,
      result: [
        { a: 1, b: "x" },
        { a: 2, b: "y" },
      ],
    };
    stubFetch({ success: true, state: "Completed", operators: { [operatorId]: op } });

    const received: string[] = [];
    const out = await executeOperatorAndFormat(state, baseConfig, operatorId, {
      onResult: opId => received.push(opId),
    });

    expect(out).toContain(`Executed operator ${operatorId}`);
    expect(out).toContain("Output table shape: (2, 2)");
    expect(out).toContain("a\tb");
    expect(out).toContain("1\tx");
    // onResult is invoked for each non-errored operator in the execution.
    expect(received).toContain(operatorId);
  });

  test("returns a placeholder when the operator has no result rows", async () => {
    const { state, operatorId } = csvScanState();
    const op: OperatorInfo = { state: "Completed", inputTuples: 0, outputTuples: 0, resultMode: "table" };
    stubFetch({ success: true, state: "Completed", operators: { [operatorId]: op } });

    const out = await executeOperatorAndFormat(state, baseConfig, operatorId);
    expect(out).toBe("(no result data)");
  });
});

describe("executeOperatorAndFormat - failures", () => {
  test("surfaces a per-operator execution error and notifies onResult", async () => {
    const { state, operatorId } = csvScanState();
    const op: OperatorInfo = {
      state: "Failed",
      inputTuples: 0,
      outputTuples: 0,
      resultMode: "table",
      error: "division by zero",
    };
    stubFetch({ success: false, state: "Failed", operators: { [operatorId]: op } });

    let notified: OperatorInfo | undefined;
    const out = await executeOperatorAndFormat(state, baseConfig, operatorId, {
      onResult: (_opId, info) => {
        notified = info;
      },
    });

    expect(out).toContain("[ERROR]");
    expect(out).toContain("Execution failed");
    expect(out).toContain("division by zero");
    expect(notified?.state).toBe("Failed");
  });

  test("reports a timeout kill as a general error", async () => {
    const { state, operatorId } = csvScanState();
    stubFetch({ success: false, state: "Killed", operators: {} });

    const out = await executeOperatorAndFormat(state, baseConfig, operatorId);
    expect(out).toContain("[ERROR]");
    expect(out).toContain("killed (timeout)");
  });

  test("maps a non-ok HTTP response to an Error execution result", async () => {
    const { state, operatorId } = csvScanState();
    globalThis.fetch = (async () => new Response("upstream boom", { status: 502, statusText: "Bad Gateway" })) as any;

    const out = await executeOperatorAndFormat(state, baseConfig, operatorId);
    expect(out).toContain("[ERROR]");
    expect(out).toContain("Execution failed");
  });
});
