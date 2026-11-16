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
import { LlmRecorder } from "./llm-recorder";

/** A stand-in LLM endpoint that echoes what it got and answers with fixed usage. */
let upstream: ReturnType<typeof Bun.serve>;
let seen: Array<{ path: string; auth: string | null; body: string }>;
let recorder: LlmRecorder;
let url: string;

beforeEach(() => {
  seen = [];
  upstream = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch: async request => {
      const path = new URL(request.url).pathname;
      seen.push({ path, auth: request.headers.get("authorization"), body: await request.text() });
      if (path === "/api/chat/completions")
        return Response.json({ choices: [], usage: { prompt_tokens: 120, completion_tokens: 8 } });
      if (path === "/api/broken") return new Response("not json", { status: 502 });
      return Response.json({ data: [] });
    },
  });
  recorder = new LlmRecorder(`http://127.0.0.1:${upstream.port}/`);
  url = recorder.start();
});

afterEach(() => {
  recorder.stop();
  upstream.stop(true);
});

describe("LlmRecorder", () => {
  test("passes a chat completion through unchanged and records its size and usage", async () => {
    const body = JSON.stringify({ model: "m", messages: [{ role: "user", content: "hi" }] });
    const response = await fetch(`${url}/api/chat/completions`, {
      method: "POST",
      headers: { Authorization: "Bearer user-jwt", "Content-Type": "application/json" },
      body,
    });
    expect(await response.json()).toEqual({ choices: [], usage: { prompt_tokens: 120, completion_tokens: 8 } });
    expect(seen).toEqual([{ path: "/api/chat/completions", auth: "Bearer user-jwt", body }]);
    expect(recorder.calls).toEqual([
      { requestBytes: new TextEncoder().encode(body).length, status: 200, inputTokens: 120, outputTokens: 8 },
    ]);
  });

  test("passes other requests through without recording them", async () => {
    const response = await fetch(`${url}/api/models?x=1`);
    expect(response.status).toBe(200);
    expect(seen.map(s => s.path)).toEqual(["/api/models"]);
    expect(recorder.calls).toEqual([]);
  });

  test("keeps an upstream failure as it is, and records a completion with no usage as zero tokens", async () => {
    const broken = await fetch(`${url}/api/broken`);
    expect(broken.status).toBe(502);
    expect(await broken.text()).toBe("not json");

    upstream.reload({ fetch: async () => new Response("<html>bad gateway</html>", { status: 502 }) });
    const failed = await fetch(`${url}/api/chat/completions`, { method: "POST", body: "{}" });
    expect(failed.status).toBe(502);
    expect(recorder.calls).toEqual([{ requestBytes: 2, status: 502, inputTokens: 0, outputTokens: 0 }]);
  });
});
