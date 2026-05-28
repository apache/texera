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
import { persistWorkflow, retrieveWorkflow } from "./workflow-api";
import type { WorkflowContent } from "../types/workflow";

interface RecordedCall {
  url: string;
  init?: RequestInit;
}

const realFetch = globalThis.fetch;
let calls: RecordedCall[] = [];

function stubFetch(handler: (url: string, init?: RequestInit) => Response): void {
  calls = [];
  globalThis.fetch = (async (url: unknown, init?: RequestInit) => {
    calls.push({ url: String(url), init });
    return handler(String(url), init);
  }) as unknown as typeof fetch;
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

const emptyContent: WorkflowContent = {
  operators: [],
  operatorPositions: {},
  links: [],
  commentBoxes: [],
  settings: { dataTransferBatchSize: 400 },
};

beforeEach(() => {
  calls = [];
});

afterEach(() => {
  globalThis.fetch = realFetch;
});

describe("retrieveWorkflow", () => {
  test("GETs the workflow by id with a Bearer token and parses string content", async () => {
    stubFetch(() => jsonResponse({ wid: 5, name: "wf", content: JSON.stringify({ operators: [], links: [] }) }));

    const wf = await retrieveWorkflow("tok-123", 5);

    expect(wf.wid).toBe(5);
    // content arrives as a JSON string and must be parsed into an object
    expect(wf.content).toEqual({ operators: [], links: [] } as unknown as WorkflowContent);
    expect(calls).toHaveLength(1);
    expect(calls[0].url).toMatch(/\/api\/workflow\/5$/);
    expect(calls[0].init?.method).toBe("GET");
    expect((calls[0].init?.headers as Record<string, string>).Authorization).toBe("Bearer tok-123");
  });

  test("leaves already-parsed object content untouched", async () => {
    stubFetch(() => jsonResponse({ wid: 1, name: "wf", content: { operators: [], links: [] } }));
    const wf = await retrieveWorkflow("t", 1);
    expect(wf.content).toEqual({ operators: [], links: [] } as unknown as WorkflowContent);
  });

  test("throws with status text on a non-ok response", async () => {
    stubFetch(() => new Response("nope", { status: 404, statusText: "Not Found" }));
    await expect(retrieveWorkflow("t", 99)).rejects.toThrow(/Failed to retrieve workflow: 404/);
  });
});

describe("persistWorkflow", () => {
  test("POSTs the workflow with stringified content and auth header", async () => {
    stubFetch(() => jsonResponse({ wid: 8, name: "saved", content: { operators: [], links: [] } }));

    const result = await persistWorkflow("tok-xyz", 8, "saved", emptyContent, "desc");

    expect(result.wid).toBe(8);
    expect(calls[0].url).toMatch(/\/api\/workflow\/persist$/);
    expect(calls[0].init?.method).toBe("POST");

    const sentBody = JSON.parse(String(calls[0].init?.body));
    expect(sentBody.wid).toBe(8);
    expect(sentBody.name).toBe("saved");
    expect(sentBody.description).toBe("desc");
    // content must be serialized to a string in the request body
    expect(typeof sentBody.content).toBe("string");
    expect(JSON.parse(sentBody.content)).toEqual(emptyContent);
  });

  test("defaults description to empty string when omitted", async () => {
    stubFetch(() => jsonResponse({ wid: 2, name: "n", content: { operators: [], links: [] } }));
    await persistWorkflow("t", 2, "n", emptyContent);
    const sentBody = JSON.parse(String(calls[0].init?.body));
    expect(sentBody.description).toBe("");
  });

  test("throws on a non-ok response", async () => {
    stubFetch(() => new Response("boom", { status: 500, statusText: "Server Error" }));
    await expect(persistWorkflow("t", 1, "n", emptyContent)).rejects.toThrow(/Failed to persist workflow: 500/);
  });
});
