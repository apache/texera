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
import { persistWorkflow, retrieveWorkflow } from "./workflow-api";
import type { WorkflowContent } from "../types/workflow";

const realFetch = globalThis.fetch;
afterEach(() => {
  globalThis.fetch = realFetch;
});

interface FakeResponseInit {
  ok: boolean;
  status?: number;
  statusText?: string;
  json?: unknown;
  text?: string;
}

function mockFetch(init: FakeResponseInit) {
  const fn = mock(async () => ({
    ok: init.ok,
    status: init.status ?? (init.ok ? 200 : 500),
    statusText: init.statusText ?? "",
    json: async () => init.json,
    text: async () => init.text ?? "",
  }));
  globalThis.fetch = fn as unknown as typeof fetch;
  return fn;
}

const content: WorkflowContent = {
  operators: [],
  operatorPositions: {},
  links: [],
  commentBoxes: [],
  settings: { dataTransferBatchSize: 400 },
};

function lastCall(fn: ReturnType<typeof mockFetch>): [string, RequestInit] {
  return fn.mock.calls[0] as unknown as [string, RequestInit];
}

describe("persistWorkflow", () => {
  test("POSTs to /workflow/persist with bearer auth and a stringified content body", async () => {
    const fn = mockFetch({ ok: true, json: { wid: 1, name: "wf", content: JSON.stringify(content) } });

    const result = await persistWorkflow("tok", 1, "wf", content, "desc");

    const [url, init] = lastCall(fn);
    expect(url).toEndWith("/api/workflow/persist");
    expect(init.method).toBe("POST");
    expect((init.headers as Record<string, string>).Authorization).toBe("Bearer tok");
    expect(JSON.parse(init.body as string)).toEqual({
      wid: 1,
      name: "wf",
      description: "desc",
      content: JSON.stringify(content),
      isPublic: false,
    });
    // The stringified content in the response is parsed back into an object.
    expect(result.content).toEqual(content);
  });

  test("throws with status detail on a non-ok response", () => {
    mockFetch({ ok: false, status: 500, statusText: "Server Error", text: "boom" });
    expect(persistWorkflow("tok", 1, "wf", content)).rejects.toThrow("Failed to persist workflow: 500");
  });
});

describe("retrieveWorkflow", () => {
  test("GETs /workflow/:wid with bearer auth and parses stringified content", async () => {
    const fn = mockFetch({ ok: true, json: { wid: 7, name: "wf", content: JSON.stringify(content) } });

    const result = await retrieveWorkflow("tok", 7);

    const [url, init] = lastCall(fn);
    expect(url).toEndWith("/api/workflow/7");
    expect(init.method).toBe("GET");
    expect((init.headers as Record<string, string>).Authorization).toBe("Bearer tok");
    expect(result.content).toEqual(content);
  });

  test("leaves an already-parsed content object untouched", async () => {
    mockFetch({ ok: true, json: { wid: 7, name: "wf", content } });
    const result = await retrieveWorkflow("tok", 7);
    expect(result.content).toEqual(content);
  });

  test("throws with status detail on a non-ok response", () => {
    mockFetch({ ok: false, status: 404, statusText: "Not Found", text: "missing" });
    expect(retrieveWorkflow("tok", 7)).rejects.toThrow("Failed to retrieve workflow: 404");
  });
});
