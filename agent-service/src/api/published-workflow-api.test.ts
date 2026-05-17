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

import { beforeEach, describe, expect, test } from "bun:test";
import { buildApp } from "../server";
import { _resetPublishedStoreForTests } from "./published-workflow-api";
import { env } from "../config/env";

const API = env.API_PREFIX;
const app = buildApp();

function url(path: string): string {
  return `http://localhost${path}`;
}

function postJson(path: string, body: unknown, extraHeaders: Record<string, string> = {}): Promise<Response> {
  return app.handle(
    new Request(url(path), {
      method: "POST",
      headers: { "Content-Type": "application/json", ...extraHeaders },
      body: JSON.stringify(body),
    })
  );
}

const sampleResults = {
  "sink-1": {
    columns: ["name", "count"],
    rows: [
      ["alpha", 1],
      ["beta", 2],
    ],
    timestamp: "2026-05-16T00:00:00.000Z",
  },
};

describe("published workflow API", () => {
  beforeEach(() => {
    _resetPublishedStoreForTests();
  });

  test("register stores workflow and /run returns cached results when API key matches", async () => {
    const registerRes = await postJson(`${API}/published/register`, {
      workflowId: 42,
      workflowName: "demo",
      apiKey: "test-key-1234",
      results: sampleResults,
    });
    expect(registerRes.status).toBe(200);
    const reg = (await registerRes.json()) as { workflowId: number; operatorCount: number };
    expect(reg.workflowId).toBe(42);
    expect(reg.operatorCount).toBe(1);

    const runRes = await postJson(
      `${API}/published/42/run`,
      {},
      { "X-API-Key": "test-key-1234" }
    );
    expect(runRes.status).toBe(200);
    const body = (await runRes.json()) as {
      workflowId: number;
      results: typeof sampleResults;
    };
    expect(body.workflowId).toBe(42);
    expect(body.results["sink-1"].rows).toEqual(sampleResults["sink-1"].rows);
  });

  test("/run returns 404 for unpublished workflow", async () => {
    const res = await postJson(`${API}/published/999/run`, {}, { "X-API-Key": "anything" });
    expect(res.status).toBe(404);
  });

  test("/run returns 401 when API key header is missing", async () => {
    await postJson(`${API}/published/7/register`, {});
    await postJson(`${API}/published/register`, {
      workflowId: 7,
      workflowName: "demo",
      apiKey: "right-key-aaaa",
      results: sampleResults,
    });
    const res = await postJson(`${API}/published/7/run`, {});
    expect(res.status).toBe(401);
  });

  test("/run returns 403 when API key is wrong", async () => {
    await postJson(`${API}/published/register`, {
      workflowId: 8,
      workflowName: "demo",
      apiKey: "right-key-aaaa",
      results: sampleResults,
    });
    const res = await postJson(`${API}/published/8/run`, {}, { "X-API-Key": "wrong-key-bbbb" });
    expect(res.status).toBe(403);
  });

  test("re-registering the same workflowId replaces the entry", async () => {
    await postJson(`${API}/published/register`, {
      workflowId: 5,
      workflowName: "demo",
      apiKey: "key-one-12345",
      results: sampleResults,
    });
    await postJson(`${API}/published/register`, {
      workflowId: 5,
      workflowName: "demo",
      apiKey: "key-two-12345",
      results: { other: { columns: ["x"], rows: [[1]] } },
    });

    // Old key should no longer work
    const oldKey = await postJson(`${API}/published/5/run`, {}, { "X-API-Key": "key-one-12345" });
    expect(oldKey.status).toBe(403);

    // New key returns updated results
    const newKey = await postJson(`${API}/published/5/run`, {}, { "X-API-Key": "key-two-12345" });
    expect(newKey.status).toBe(200);
    const body = (await newKey.json()) as { results: Record<string, { columns: string[] }> };
    expect(Object.keys(body.results)).toEqual(["other"]);
  });
});
