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
import { buildApp, _resetAgentStoreForTests } from "./server";
import { env } from "./config/env";

const API = env.API_PREFIX;
const app = buildApp();

function url(path: string): string {
  return `http://localhost${path}`;
}

async function postJson(path: string, body: unknown): Promise<Response> {
  return app.handle(
    new Request(url(path), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
  );
}

async function patchJson(path: string, body: unknown): Promise<Response> {
  return app.handle(
    new Request(url(path), {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
  );
}

async function getJson(path: string): Promise<Response> {
  return app.handle(new Request(url(path)));
}

async function del(path: string): Promise<Response> {
  return app.handle(new Request(url(path), { method: "DELETE" }));
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
