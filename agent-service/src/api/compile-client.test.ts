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
import { compileWorkflowAsync } from "./compile-client";
import type { LogicalPlan } from "../types/workflow";

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

afterEach(() => {
  globalThis.fetch = realFetch;
  captured = undefined;
});

const PLAN: LogicalPlan = {
  operators: [{ operatorID: "op1", operatorType: "CSVFileScan" }],
  links: [],
};

describe("compileWorkflowAsync", () => {
  test("POSTs the plan to the compiling service and returns the parsed response", async () => {
    const payload = { operatorOutputSchemas: {}, operatorErrors: {} };
    mockFetch(() => new Response(JSON.stringify(payload), { status: 200 }));

    const result = await compileWorkflowAsync(PLAN);

    expect(result).toEqual(payload);
    expect(captured?.url).toBe("http://localhost:9090/api/compile");
    expect(captured?.init.method).toBe("POST");
    const body = JSON.parse(captured!.init.body);
    expect(body.operators).toEqual(PLAN.operators);
    expect(body.links).toEqual([]);
    expect(body.opsToReuseResult).toEqual([]);
    expect(body.opsToViewResult).toEqual([]);
  });

  test("returns null when the compiling service responds non-OK", async () => {
    mockFetch(() => new Response("compile error", { status: 400, statusText: "Bad Request" }));

    expect(await compileWorkflowAsync(PLAN)).toBeNull();
  });

  test("returns null on a network failure", async () => {
    mockFetch(() => {
      throw new Error("connection refused");
    });

    expect(await compileWorkflowAsync(PLAN)).toBeNull();
  });
});
