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

import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { createHmac } from "crypto";
import { InMemoryAgentMetadataStore } from "./api/agent-metadata-store";
import { buildApp, _resetAgentStoreForTests, _setAgentMetadataStoreForTests } from "./server";
import { env } from "./config/env";

const API = env.API_PREFIX;
const SECRET = "test-secret-key-for-agent-service-access-control";
const prevSecret = process.env.AUTH_JWT_SECRET;
const metadataStore = new InMemoryAgentMetadataStore();
const app = buildApp();

function url(path: string): string {
  return `http://localhost${path}`;
}

function b64url(input: string | Buffer): string {
  return Buffer.from(input).toString("base64").replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

function signJwt(payload: Record<string, unknown>, secret = SECRET): string {
  const header = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = b64url(JSON.stringify(payload));
  const sig = b64url(createHmac("sha256", secret).update(`${header}.${body}`).digest());
  return `${header}.${body}.${sig}`;
}

function tokenFor(uid: number, secret = SECRET): string {
  return signJwt({ sub: `user-${uid}`, userId: uid, exp: Math.floor(Date.now() / 1000) + 3600 }, secret);
}

function authHeaders(token: string | null, extra: Record<string, string> = {}): Record<string, string> {
  const headers = { ...extra };
  if (token) headers.Authorization = `Bearer ${token}`;
  return headers;
}

async function postJson(path: string, body: unknown, token: string | null = tokenFor(1)): Promise<Response> {
  return app.handle(
    new Request(url(path), {
      method: "POST",
      headers: authHeaders(token, { "Content-Type": "application/json" }),
      body: JSON.stringify(body),
    })
  );
}

async function patchJson(path: string, body: unknown, token: string | null = tokenFor(1)): Promise<Response> {
  return app.handle(
    new Request(url(path), {
      method: "PATCH",
      headers: authHeaders(token, { "Content-Type": "application/json" }),
      body: JSON.stringify(body),
    })
  );
}

async function getJson(path: string, token: string | null = tokenFor(1)): Promise<Response> {
  return app.handle(new Request(url(path), { headers: authHeaders(token) }));
}

async function del(path: string, token: string | null = tokenFor(1)): Promise<Response> {
  return app.handle(new Request(url(path), { method: "DELETE", headers: authHeaders(token) }));
}

async function readJson<T = unknown>(res: Response): Promise<T> {
  return (await res.json()) as T;
}

beforeAll(() => {
  process.env.AUTH_JWT_SECRET = SECRET;
});

afterAll(() => {
  if (prevSecret === undefined) delete process.env.AUTH_JWT_SECRET;
  else process.env.AUTH_JWT_SECRET = prevSecret;
});

beforeEach(() => {
  metadataStore.clear();
  _setAgentMetadataStoreForTests(metadataStore);
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
});

describe(`POST ${API}/agents`, () => {
  test("creates an agent without workflow binding", async () => {
    const res = await postJson(`${API}/agents`, { modelType: "test-model", name: "Tester" });
    expect(res.status).toBe(200);

    const agent = await readJson<{
      id: string;
      name: string;
      modelType: string;
      state: string;
      delegate?: unknown;
    }>(res);
    expect(agent.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    expect(agent.name).toBe("Tester");
    expect(agent.modelType).toBe("test-model");
    expect(agent.state).toBe("AVAILABLE");
    expect(agent.delegate).toBeUndefined();
  });

  test("assigns a unique, non-guessable id to each agent", async () => {
    const a = await readJson<{ id: string }>(await postJson(`${API}/agents`, { modelType: "m" }));
    const b = await readJson<{ id: string }>(await postJson(`${API}/agents`, { modelType: "m" }));

    expect(a.id).not.toBe(b.id);
    // UUID-based ids are not enumerable, unlike the previous sequential counter.
    expect(a.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  });

  test("persists ownership metadata for the creating user", async () => {
    const agent = await readJson<{ id: string }>(
      await postJson(`${API}/agents`, { modelType: "m", name: "owned" }, tokenFor(7))
    );

    expect(await metadataStore.getAgent(agent.id)).toMatchObject({
      id: agent.id,
      ownerUid: 7,
      name: "owned",
      modelType: "m",
      config: {
        settings: expect.objectContaining({
          maxSteps: 100,
        }),
        tools: expect.any(Array),
      },
      reactSteps: [],
    });
  });

  test("rejects invalid token", async () => {
    const res = await postJson(
      `${API}/agents`,
      {
        modelType: "m",
      },
      "obviously-not-a-jwt"
    );
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

  test("lists persisted agents after the runtime cache is cleared", async () => {
    const created = await readJson<{ id: string }>(
      await postJson(`${API}/agents`, { modelType: "m", name: "persisted" })
    );

    _resetAgentStoreForTests();

    const res = await getJson(`${API}/agents`);
    expect(res.status).toBe(200);
    const body = await readJson<{ agents: { id: string; name: string }[] }>(res);
    expect(body.agents).toHaveLength(1);
    expect(body.agents[0]).toMatchObject({ id: created.id, name: "persisted" });
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

  test("GET /:id/react-steps restores persisted ReAct state", async () => {
    const created = await readJson<{ id: string }>(await postJson(`${API}/agents`, { modelType: "m" }));
    await metadataStore.updateAgentReActSteps(created.id, [
      {
        id: "step-persisted",
        messageId: "msg-persisted",
        stepId: 0,
        timestamp: Date.now(),
        role: "user",
        content: "persisted step",
        isBegin: true,
        isEnd: true,
      },
    ]);

    _resetAgentStoreForTests();

    const res = await getJson(`${API}/agents/${created.id}/react-steps`);
    expect(res.status).toBe(200);
    const body = await readJson<{ steps: { content: string }[] }>(res);
    expect(body.steps.map(step => step.content)).toEqual(["persisted step"]);
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

    const persisted = await metadataStore.getAgent(created.id);
    expect(persisted?.config.settings).toMatchObject({
      maxSteps: 7,
      toolTimeoutSeconds: 30,
    });
  });
});

describe("access control", () => {
  async function createOwnedAgent(uid: number): Promise<string> {
    const res = await postJson(`${API}/agents`, { modelType: "m" }, tokenFor(uid));
    expect(res.status).toBe(200);
    return (await readJson<{ id: string }>(res)).id;
  }

  test("rejects agent creation without a token", async () => {
    const res = await postJson(`${API}/agents`, { modelType: "m" }, null);
    expect(res.status).toBe(401);
  });

  test("rejects a forged token (bad signature) at creation", async () => {
    const forged = tokenFor(1, "the-wrong-secret");
    const res = await postJson(`${API}/agents`, { modelType: "m" }, forged);
    expect(res.status).toBe(401);
  });

  test("rejects an expired token at creation", async () => {
    const expired = signJwt({ sub: "user-1", userId: 1, exp: Math.floor(Date.now() / 1000) - 3600 });
    const res = await postJson(`${API}/agents`, { modelType: "m" }, expired);
    expect(res.status).toBe(401);
  });

  test("an owner can read its own agent", async () => {
    const id = await createOwnedAgent(1);
    const res = await getJson(`${API}/agents/${id}`, tokenFor(1));
    expect(res.status).toBe(200);
  });

  test("a different user cannot read someone else's agent (403)", async () => {
    const id = await createOwnedAgent(1);
    const res = await getJson(`${API}/agents/${id}`, tokenFor(2));
    expect(res.status).toBe(403);
  });

  test("a request without a token is rejected (401)", async () => {
    const id = await createOwnedAgent(1);
    const res = await getJson(`${API}/agents/${id}`, null);
    expect(res.status).toBe(401);
  });

  test("a control route is also guarded (stop -> 403 for non-owner)", async () => {
    const id = await createOwnedAgent(1);
    const res = await app.handle(
      new Request(url(`${API}/agents/${id}/stop`), {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${tokenFor(2)}` },
        body: "{}",
      })
    );
    expect(res.status).toBe(403);
  });

  test("listing is scoped to the caller's own agents", async () => {
    const mine = await createOwnedAgent(1);
    await createOwnedAgent(2);

    const res = await getJson(`${API}/agents`, tokenFor(1));
    expect(res.status).toBe(200);
    const body = await readJson<{ agents: { id: string }[] }>(res);
    expect(body.agents.map(a => a.id)).toEqual([mine]);
  });

  test("listing without a token is rejected (401)", async () => {
    const res = await getJson(`${API}/agents`, null);
    expect(res.status).toBe(401);
  });
});
