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
import { createHmac } from "node:crypto";
import { buildApp, _resetAgentStoreForTests } from "./server";
import { env } from "./config/env";
import { JWT_SECRET } from "./config/jwt";

const API = env.API_PREFIX;
const app = buildApp();

function url(path: string): string {
  return `http://localhost${path}`;
}

// Mint a valid HS256 token (same shape JwtAuth issues) so requests pass the
// agent-service auth guard; tests that probe the guard itself omit/forge it.
function b64url(input: string | Buffer): string {
  return Buffer.from(input).toString("base64url");
}
function signToken(claims: Record<string, unknown> = {}): string {
  const header = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const payload = b64url(
    JSON.stringify({
      sub: "tester",
      userId: 1,
      email: "tester@example.com",
      role: "REGULAR",
      exp: Math.floor(Date.now() / 1000) + 3600,
      ...claims,
    })
  );
  const signature = b64url(
    createHmac("sha256", new TextEncoder().encode(JWT_SECRET)).update(`${header}.${payload}`).digest()
  );
  return `${header}.${payload}.${signature}`;
}
const VALID_TOKEN = signToken();
function authHeaders(extra: Record<string, string> = {}): Record<string, string> {
  return { Authorization: `Bearer ${VALID_TOKEN}`, ...extra };
}

async function postJson(path: string, body: unknown): Promise<Response> {
  return app.handle(
    new Request(url(path), {
      method: "POST",
      headers: authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(body),
    })
  );
}

async function patchJson(path: string, body: unknown): Promise<Response> {
  return app.handle(
    new Request(url(path), {
      method: "PATCH",
      headers: authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(body),
    })
  );
}

async function getJson(path: string): Promise<Response> {
  return app.handle(new Request(url(path), { headers: authHeaders() }));
}

async function del(path: string): Promise<Response> {
  return app.handle(new Request(url(path), { method: "DELETE", headers: authHeaders() }));
}

async function readJson<T = unknown>(res: Response): Promise<T> {
  return (await res.json()) as T;
}

beforeEach(() => {
  _resetAgentStoreForTests();
});

describe(`GET ${API}/healthcheck`, () => {
  test("returns 200 with status ok", async () => {
    const res = await getJson(`${API}/healthcheck`);
    expect(res.status).toBe(200);
    const body = await readJson<{ status: string; timestamp: string }>(res);
    expect(body.status).toBe("ok");
    expect(typeof body.timestamp).toBe("string");
  });

  test("does not require a token (readiness probe)", async () => {
    const res = await app.handle(new Request(url(`${API}/healthcheck`)));
    expect(res.status).toBe(200);
  });
});

describe("auth guard on agent routes", () => {
  test("rejects a request with no Authorization header (401)", async () => {
    const res = await app.handle(new Request(url(`${API}/agents`)));
    expect(res.status).toBe(401);
    expect(await readJson<{ error: string }>(res)).toEqual({ error: "Unauthorized" });
  });

  test("rejects an invalid / mis-signed token (401)", async () => {
    const res = await app.handle(
      new Request(url(`${API}/agents`), { headers: { Authorization: "Bearer not-a-valid-jwt" } })
    );
    expect(res.status).toBe(401);
  });

  test("rejects an expired token (401)", async () => {
    const expired = signToken({ exp: Math.floor(Date.now() / 1000) - 3600 });
    const res = await app.handle(
      new Request(url(`${API}/agents`), { headers: { Authorization: `Bearer ${expired}` } })
    );
    expect(res.status).toBe(401);
  });

  test("accepts a valid token", async () => {
    const res = await app.handle(new Request(url(`${API}/agents`), { headers: authHeaders() }));
    expect(res.status).toBe(200);
  });

  test("guards the models endpoint too", async () => {
    const res = await app.handle(new Request(url(`${API}/agents/models`)));
    expect(res.status).toBe(401);
  });
});

describe(`POST ${API}/agents`, () => {
  test("creates an agent with no delegate", async () => {
    const res = await postJson(`${API}/agents`, { modelType: "test-model", name: "Tester" });
    expect(res.status).toBe(200);

    const agent = await readJson<{
      id: string;
      name: string;
      modelType: string;
      state: string;
      delegate: unknown;
    }>(res);
    expect(agent.id).toMatch(/^agent-\d+$/);
    expect(agent.name).toBe("Tester");
    expect(agent.modelType).toBe("test-model");
    expect(agent.state).toBe("AVAILABLE");
    expect(agent.delegate).toBeUndefined();
  });

  test("auto-numbers agent ids monotonically", async () => {
    const a = await readJson<{ id: string }>(await postJson(`${API}/agents`, { modelType: "m" }));
    const b = await readJson<{ id: string }>(await postJson(`${API}/agents`, { modelType: "m" }));

    const aNum = Number(a.id.split("-")[1]);
    const bNum = Number(b.id.split("-")[1]);
    expect(bNum).toBe(aNum + 1);
  });

  test("rejects invalid token", async () => {
    const res = await postJson(`${API}/agents`, {
      modelType: "m",
      userToken: "obviously-not-a-jwt",
    });
    expect(res.status).toBe(401);
    const body = await readJson<{ error: string }>(res);
    expect(body.error).toBe("Invalid or expired token");
  });

  test("rejects missing modelType", async () => {
    const res = await postJson(`${API}/agents`, { name: "no-model" });
    // Body schema violation; the exact status depends on the Elysia version but
    // it is always a 4xx or 5xx, never a successful 2xx.
    expect(res.status).toBeGreaterThanOrEqual(400);
  });
});

describe(`GET ${API}/agents`, () => {
  test("empty store returns no agents", async () => {
    const res = await getJson(`${API}/agents`);
    expect(res.status).toBe(200);
    const body = await readJson<{ agents: unknown[] }>(res);
    expect(body.agents).toEqual([]);
  });

  test("lists every created agent", async () => {
    await postJson(`${API}/agents`, { modelType: "m", name: "one" });
    await postJson(`${API}/agents`, { modelType: "m", name: "two" });

    const res = await getJson(`${API}/agents`);
    const body = await readJson<{ agents: { name: string }[] }>(res);
    expect(body.agents).toHaveLength(2);
    expect(body.agents.map(a => a.name).sort()).toEqual(["one", "two"]);
  });
});

describe(`GET ${API}/agents/:id`, () => {
  test("returns the agent plus its workflow snapshot", async () => {
    const created = await readJson<{ id: string }>(await postJson(`${API}/agents`, { modelType: "m" }));

    const res = await getJson(`${API}/agents/${created.id}`);
    expect(res.status).toBe(200);
    const body = await readJson<{ id: string; workflow: unknown; stepCount: number }>(res);
    expect(body.id).toBe(created.id);
    expect(body.workflow).toBeDefined();
    expect(typeof body.stepCount).toBe("number");
  });

  test("returns 404 for an unknown id", async () => {
    const res = await getJson(`${API}/agents/agent-does-not-exist`);
    expect(res.status).toBe(404);
    const body = await readJson<{ error: string }>(res);
    expect(body.error).toBe("Agent not found");
  });
});

describe(`DELETE ${API}/agents/:id`, () => {
  test("destroys the agent and a follow-up GET returns 404", async () => {
    const created = await readJson<{ id: string }>(await postJson(`${API}/agents`, { modelType: "m" }));

    const delRes = await del(`${API}/agents/${created.id}`);
    expect(delRes.status).toBe(200);
    expect(await readJson<unknown>(delRes)).toEqual({ deleted: true });

    const getRes = await getJson(`${API}/agents/${created.id}`);
    expect(getRes.status).toBe(404);
  });

  test("returns 404 when deleting an unknown agent", async () => {
    const res = await del(`${API}/agents/missing`);
    expect(res.status).toBe(404);
  });
});

describe("Agent control routes", () => {
  test("POST /:id/stop returns stopping", async () => {
    const created = await readJson<{ id: string }>(await postJson(`${API}/agents`, { modelType: "m" }));
    const res = await postJson(`${API}/agents/${created.id}/stop`, {});
    expect(res.status).toBe(200);
    expect(await readJson<unknown>(res)).toEqual({ status: "stopping" });
  });

  test("POST /:id/clear resets history", async () => {
    const created = await readJson<{ id: string }>(await postJson(`${API}/agents`, { modelType: "m" }));
    const res = await postJson(`${API}/agents/${created.id}/clear`, {});
    expect(res.status).toBe(200);
    expect(await readJson<unknown>(res)).toEqual({ status: "cleared" });
  });

  test("GET /:id/operator-results returns an empty map on the framework build", async () => {
    const created = await readJson<{ id: string }>(await postJson(`${API}/agents`, { modelType: "m" }));
    const res = await getJson(`${API}/agents/${created.id}/operator-results`);
    expect(res.status).toBe(200);
    expect(await readJson<unknown>(res)).toEqual({ results: {} });
  });
});

describe(`GET ${API}/agents/models`, () => {
  // The endpoint fetches the model list from LiteLLM directly (the agent
  // service holds the master key). Stub global fetch so no real LiteLLM is
  // needed, and capture the outgoing request to assert URL + auth header.
  const realFetch = globalThis.fetch;
  let calls: Array<{ url: string; authorization: string | null }>;

  function mockLiteLLM(responder: (reqUrl: string) => Response | Promise<Response>): void {
    globalThis.fetch = (async (
      input: Parameters<typeof fetch>[0],
      init?: Parameters<typeof fetch>[1]
    ): Promise<Response> => {
      const reqUrl = String(input);
      const authorization = new Headers(init?.headers).get("Authorization");
      calls.push({ url: reqUrl, authorization });
      return responder(reqUrl);
    }) as typeof globalThis.fetch;
  }

  beforeEach(() => {
    calls = [];
  });

  afterEach(() => {
    globalThis.fetch = realFetch;
  });

  test("forwards the LiteLLM model list and calls it directly with the master key", async () => {
    const upstream = {
      data: [{ id: "gpt-5-mini", object: "model", created: 0, owned_by: "openai" }],
      object: "list",
    };
    mockLiteLLM(
      () => new Response(JSON.stringify(upstream), { status: 200, headers: { "Content-Type": "application/json" } })
    );

    const res = await getJson(`${API}/agents/models`);
    expect(res.status).toBe(200);
    expect(await readJson<typeof upstream>(res)).toEqual(upstream);

    // One call, straight to <litellm-base>/models, carrying the master key.
    expect(calls).toHaveLength(1);
    expect(calls[0].url).toBe(`${env.LITELLM_BASE_URL}/models`);
    expect(calls[0].authorization).toBe(`Bearer ${env.LITELLM_MASTER_KEY}`);
  });

  test("returns 502 when LiteLLM responds with a non-OK status", async () => {
    mockLiteLLM(() => new Response("unauthorized", { status: 401 }));

    const res = await getJson(`${API}/agents/models`);
    expect(res.status).toBe(502);
    const body = await readJson<{ error: string }>(res);
    expect(body.error).toContain("Failed to fetch models from LiteLLM");
  });

  test("returns 502 when LiteLLM is unreachable", async () => {
    mockLiteLLM(() => {
      throw new Error("connect ECONNREFUSED 127.0.0.1:4000");
    });

    const res = await getJson(`${API}/agents/models`);
    expect(res.status).toBe(502);
    const body = await readJson<{ error: string }>(res);
    expect(body.error).toContain("Failed to fetch models from LiteLLM");
  });
});

describe(`PATCH ${API}/agents/:id/settings`, () => {
  test("updates settings and returns the new values", async () => {
    const created = await readJson<{ id: string }>(await postJson(`${API}/agents`, { modelType: "m" }));

    const res = await patchJson(`${API}/agents/${created.id}/settings`, {
      maxSteps: 7,
      toolTimeoutSeconds: 30,
    });
    expect(res.status).toBe(200);
    const body = await readJson<{ maxSteps: number; toolTimeoutSeconds: number }>(res);
    expect(body.maxSteps).toBe(7);
    expect(body.toolTimeoutSeconds).toBe(30);

    // A follow-up GET reflects the same values.
    const reread = await readJson<{ maxSteps: number; toolTimeoutSeconds: number }>(
      await getJson(`${API}/agents/${created.id}/settings`)
    );
    expect(reread.maxSteps).toBe(7);
    expect(reread.toolTimeoutSeconds).toBe(30);
  });
});
