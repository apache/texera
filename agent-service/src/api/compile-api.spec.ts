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

import { afterEach, describe, expect, mock, test } from "bun:test";
import { compileWorkflowAsync } from "./compile-api";
import type { LogicalPlan } from "../types/workflow";

const realFetch = globalThis.fetch;
afterEach(() => {
  globalThis.fetch = realFetch;
});

const plan: LogicalPlan = {
  operators: [{ operatorID: "op1", operatorType: "Filter" }],
  links: [],
};

describe("compileWorkflowAsync", () => {
  function mockJson(body: unknown) {
    const fn = mock(async () => ({
      ok: true,
      status: 200,
      statusText: "OK",
      json: async () => body,
      text: async () => "",
    }));
    globalThis.fetch = fn as unknown as typeof fetch;
    return fn;
  }

  test("POSTs the logical plan to /api/compile", async () => {
    const fn = mockJson({ type: "success", physicalPlan: { nodes: [] }, operatorOutputSchemas: {} });

    await compileWorkflowAsync(plan);

    const [url, init] = fn.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toEndWith("/api/compile");
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body as string)).toEqual({
      operators: plan.operators,
      links: plan.links,
      opsToReuseResult: [],
      opsToViewResult: [],
    });
  });

  test("parses a success response", async () => {
    mockJson({ type: "success", physicalPlan: { nodes: [] }, operatorOutputSchemas: {} });

    const result = await compileWorkflowAsync(plan);

    expect(result).not.toBeNull();
    const compiled = result!;
    expect(compiled.type).toBe("success");
    if (compiled.type === "success") {
      expect(compiled.physicalPlan).toEqual({ nodes: [] });
    }
  });

  test("parses a failure response with proto-accurate operator errors", async () => {
    // operatorErrors uses the WorkflowFatalError shape (type is the enum name string).
    mockJson({
      type: "failure",
      operatorOutputSchemas: {},
      operatorErrors: {
        op1: {
          type: "COMPILATION_ERROR",
          message: "bad attribute",
          details: "stack",
          operatorId: "op1",
          workerId: "",
          timestamp: { seconds: 1, nanos: 0 },
        },
      },
    });

    const result = await compileWorkflowAsync(plan);

    expect(result).not.toBeNull();
    const compiled = result!;
    expect(compiled.type).toBe("failure");
    if (compiled.type === "failure") {
      expect(compiled.operatorErrors.op1.type).toBe("COMPILATION_ERROR");
      expect(compiled.operatorErrors.op1.message).toBe("bad attribute");
    }
  });

  test("returns null on an unrecognized response shape", async () => {
    mockJson({ somethingElse: true });
    expect(await compileWorkflowAsync(plan)).toBeNull();
  });

  test("returns null on a non-ok response", async () => {
    const fn = mock(async () => ({
      ok: false,
      status: 400,
      statusText: "Bad Request",
      json: async () => ({}),
      text: async () => "compile error",
    }));
    globalThis.fetch = fn as unknown as typeof fetch;

    expect(await compileWorkflowAsync(plan)).toBeNull();
  });

  test("returns null when the request throws", async () => {
    const fn = mock(async () => {
      throw new Error("network down");
    });
    globalThis.fetch = fn as unknown as typeof fetch;

    expect(await compileWorkflowAsync(plan)).toBeNull();
  });
});
