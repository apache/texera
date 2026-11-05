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

  test("sends the user's token as a Bearer Authorization header when given", async () => {
    const fetchSpy = spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ operatorOutputSchemas: {}, operatorErrors: {} }), { status: 200 })
    );

    await compileWorkflowAsync(plan, "user-jwt");

    const [, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
    expect(init.headers).toEqual({ Authorization: "Bearer user-jwt", "Content-Type": "application/json" });
  });

  test("omits the Authorization header when the token is empty", async () => {
    const fetchSpy = spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ operatorOutputSchemas: {}, operatorErrors: {} }), { status: 200 })
    );

    await compileWorkflowAsync(plan, "");

    const [, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
    expect(init.headers).toEqual({ "Content-Type": "application/json" });
  });

  test("against a JWT-protected endpoint, returns schemas only when the token is passed", async () => {
    const compilation: WorkflowCompilationResponse = {
      operatorOutputSchemas: { opX: { "output-0": [{ attributeName: "a", attributeType: "string" }] } },
      operatorErrors: {},
    };
    // Mirrors JwtAuthFilter: 401 unless a Bearer token is present.
    spyOn(globalThis, "fetch").mockImplementation((async (_url: string, init?: RequestInit) => {
      const auth = (init?.headers as Record<string, string> | undefined)?.Authorization ?? "";
      return /^Bearer \S+$/.test(auth)
        ? new Response(JSON.stringify(compilation), { status: 200 })
        : new Response("", { status: 401, headers: { "WWW-Authenticate": 'Bearer realm="texera"' } });
    }) as unknown as typeof fetch);

    expect(await compileWorkflowAsync(plan)).toBeNull();
    expect(await compileWorkflowAsync(plan, "user-jwt")).toEqual(compilation);
  });

  test("returns null on a non-ok response", async () => {
    spyOn(globalThis, "fetch").mockResolvedValue(new Response("boom", { status: 500 }));
    expect(await compileWorkflowAsync(plan)).toBeNull();
  });

  test("returns null when fetch rejects", async () => {
    spyOn(globalThis, "fetch").mockRejectedValue(new Error("network down"));
    expect(await compileWorkflowAsync(plan)).toBeNull();
  });
});
