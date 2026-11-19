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

import { afterEach, describe, expect, mock, spyOn, test } from "bun:test";
import { compileWorkflowAsync, type WorkflowCompilationResponse } from "./compile-api";
import { macroWorkflow } from "../agent/macro-test-data";
import type { LogicalPlan } from "../types/workflow";

const plan = {
  operators: [{ operatorID: "opX" }],
  links: [],
} as unknown as LogicalPlan;

describe("compileWorkflowAsync", () => {
  afterEach(() => {
    mock.restore();
  });

  test("POSTs the plan to the compile endpoint and returns the parsed response on ok", async () => {
    const compilation: WorkflowCompilationResponse = { operatorOutputSchemas: {}, operatorErrors: {} };
    const fetchSpy = spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(compilation), { status: 200 })
    );

    const result = await compileWorkflowAsync(plan);

    expect(result).toEqual(compilation);
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
    expect(url).toMatch(/\/api\/compile$/);
    expect(init.method).toBe("POST");
    expect(init.headers).toEqual({ "Content-Type": "application/json" });
    expect(JSON.parse(init.body as string)).toEqual({
      operators: [{ operatorID: "opX" }],
      links: [],
      opsToReuseResult: [],
      opsToViewResult: [],
    });
  });

  test.each([
    [500, undefined],
    [401, "expired-token"],
  ] as const)("returns null for HTTP %d", async (status, token) => {
    spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, { status }));
    expect(await compileWorkflowAsync(plan, token)).toBeNull();
  });

  test("sends the authenticated pinned bundle and preserves the compiler's boundary schemas and nested diagnostics", async () => {
    const macroContent = macroWorkflow();
    const compilation: WorkflowCompilationResponse = {
      operatorOutputSchemas: { one: { "0_false": [{ attributeName: "value", attributeType: "string" }] } },
      operatorErrors: { one: { operatorId: "one", message: "Outer / Inner / Scan: invalid column", type: "schema" } },
      macroExpansion: { content: { ...macroContent, operators: [], links: [] }, paths: {}, ports: {} },
    };
    const fetchSpy = spyOn(globalThis, "fetch").mockResolvedValue(Response.json(compilation));
    const request = { ...plan, macroContent, executeOperatorIds: ["one"] };
    expect(await compileWorkflowAsync(request, "user-token")).toEqual(compilation);
    const [, init] = fetchSpy.mock.calls[0];
    expect(init?.headers).toEqual({ Authorization: "Bearer user-token", "Content-Type": "application/json" });
    expect(JSON.parse(init!.body as string)).toMatchObject({ macroContent, executeOperatorIds: ["one"] });
  });

  test("returns null when fetch rejects", async () => {
    spyOn(globalThis, "fetch").mockRejectedValue(new Error("network down"));
    expect(await compileWorkflowAsync(plan)).toBeNull();
  });
});
