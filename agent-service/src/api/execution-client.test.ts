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

import { describe, expect, test, afterEach } from "bun:test";
import { executeWorkflowHttp } from "./execution-client";
import { DEFAULT_AGENT_SETTINGS, type ExecutionRequestParams } from "../types/agent";
import type { LogicalPlan } from "../types/workflow";
import { env } from "../config/env";

const realFetch = globalThis.fetch;

interface CapturedRequest {
  url: string;
  init: any;
}

let captured: CapturedRequest | undefined;

function mockFetch(responder: (req: CapturedRequest) => Response | Promise<Response>): void {
  globalThis.fetch = (async (input: any, init: any) => {
    captured = { url: String(input), init };
    return responder(captured);
  }) as unknown as typeof fetch;
}

function okResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });
}

afterEach(() => {
  globalThis.fetch = realFetch;
  captured = undefined;
});

const PLAN: LogicalPlan = {
  operators: [{ operatorID: "op1", operatorType: "CSVFileScan" }],
  links: [],
  opsToViewResult: ["op1"],
  opsToReuseResult: [],
};

const PARAMS: ExecutionRequestParams = {
  userToken: "test-token",
  workflowId: 42,
  computingUnitId: 7,
  executionTimeoutMs: 60_000,
  maxOperatorResultCharLimit: 1000,
  maxOperatorResultCellCharLimit: 500,
};

describe("executeWorkflowHttp", () => {
  test("posts to the per-CU execution endpoint with auth and a complete request body", async () => {
    mockFetch(() => okResponse({ success: true, state: "Completed", operators: {} }));

    const result = await executeWorkflowHttp(PARAMS, PLAN);

    expect(result).toEqual({ success: true, state: "Completed", operators: {} });
    expect(captured?.url).toBe("http://localhost:8085/api/execution/42/7/run");
    expect(captured?.init.method).toBe("POST");
    expect(captured?.init.headers.Authorization).toBe("Bearer test-token");

    const body = JSON.parse(captured!.init.body);
    expect(body.executionName).toBe("agent-execution");
    expect(body.logicalPlan.operators).toEqual(PLAN.operators);
    expect(body.logicalPlan.opsToReuseResult).toEqual([]);
    expect(body.targetOperatorIds).toEqual(["op1"]);
    expect(body.timeoutSeconds).toBe(60);
    expect(body.maxOperatorResultCharLimit).toBe(1000);
    expect(body.maxOperatorResultCellCharLimit).toBe(500);
  });

  test("defaults computingUnitId to 0 and falls back to default limits/timeout", async () => {
    mockFetch(() => okResponse({ success: true, state: "Completed", operators: {} }));

    await executeWorkflowHttp({ userToken: "t", workflowId: 42 }, PLAN);

    expect(captured?.url).toBe("http://localhost:8085/api/execution/42/0/run");
    const body = JSON.parse(captured!.init.body);
    expect(body.timeoutSeconds).toBe(Math.ceil(DEFAULT_AGENT_SETTINGS.executionTimeoutMs / 1000));
    expect(body.maxOperatorResultCharLimit).toBe(DEFAULT_AGENT_SETTINGS.maxOperatorResultCharLimit);
    expect(body.maxOperatorResultCellCharLimit).toBe(DEFAULT_AGENT_SETTINGS.maxOperatorResultCellCharLimit);
  });

  test("returns an error-state result when the backend responds non-OK", async () => {
    mockFetch(() => new Response("boom", { status: 500, statusText: "Server Error" }));

    const result = await executeWorkflowHttp(PARAMS, PLAN);

    expect(result.success).toBe(false);
    expect(result.state).toBe("Error");
    expect(result.operators).toEqual({});
    expect(result.errors?.[0]).toContain("500");
    expect(result.errors?.[0]).toContain("boom");
  });

  test("returns an error-state result on a network failure", async () => {
    mockFetch(() => {
      throw new Error("network down");
    });

    const result = await executeWorkflowHttp(PARAMS, PLAN);

    expect(result).toEqual({ success: false, state: "Error", operators: {}, errors: ["network down"] });
  });

  test("re-throws AbortError so callers can detect cancellation", async () => {
    mockFetch(() => {
      const err = new Error("aborted");
      err.name = "AbortError";
      throw err;
    });

    await expect(executeWorkflowHttp(PARAMS, PLAN)).rejects.toThrow("aborted");
  });

  test("uses EXECUTION_ENDPOINT_TEMPLATE with {cuid} substituted when configured", async () => {
    const previous = env.EXECUTION_ENDPOINT_TEMPLATE;
    (env as any).EXECUTION_ENDPOINT_TEMPLATE = "http://cu-{cuid}.svc:8085";
    try {
      mockFetch(() => okResponse({ success: true, state: "Completed", operators: {} }));
      await executeWorkflowHttp(PARAMS, PLAN);
      expect(captured?.url).toBe("http://cu-7.svc:8085/api/execution/42/7/run");
    } finally {
      (env as any).EXECUTION_ENDPOINT_TEMPLATE = previous;
    }
  });
});
