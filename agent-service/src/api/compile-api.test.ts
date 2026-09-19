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

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { compileWorkflowAsync } from "./compile-api";
import type { LogicalPlan } from "../types/workflow";

const realFetch = globalThis.fetch;
let lastBody: unknown;
let lastUrl = "";

function stubFetch(handler: () => Response | Promise<Response>): void {
  globalThis.fetch = (async (url: unknown, init?: RequestInit) => {
    lastUrl = String(url);
    lastBody = init?.body ? JSON.parse(String(init.body)) : undefined;
    return handler();
  }) as unknown as typeof fetch;
}

const plan: LogicalPlan = {
  operators: [{ operatorID: "op1", operatorType: "CSVFileScan" }],
  links: [],
};

beforeEach(() => {
  lastBody = undefined;
  lastUrl = "";
});

afterEach(() => {
  globalThis.fetch = realFetch;
});

describe("compileWorkflowAsync", () => {
  test("POSTs to /api/compile and returns the parsed response on success", async () => {
    const payload = { operatorOutputSchemas: {}, operatorErrors: {} };
    stubFetch(() => new Response(JSON.stringify(payload), { status: 200 }));

    const result = await compileWorkflowAsync(plan);

    expect(result).toEqual(payload);
    expect(lastUrl).toMatch(/\/api\/compile$/);
    // request body forwards operators/links and pins the result-selection fields
    expect((lastBody as any).operators).toHaveLength(1);
    expect((lastBody as any).opsToReuseResult).toEqual([]);
    expect((lastBody as any).opsToViewResult).toEqual([]);
  });

  test("returns null (not throw) on a non-ok response", async () => {
    stubFetch(() => new Response("bad plan", { status: 400, statusText: "Bad Request" }));
    expect(await compileWorkflowAsync(plan)).toBeNull();
  });

  test("returns null when fetch itself rejects", async () => {
    stubFetch(() => {
      throw new Error("network down");
    });
    expect(await compileWorkflowAsync(plan)).toBeNull();
  });
});
