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

import { beforeEach, describe, expect, spyOn, test } from "bun:test";
import { buildApp, start, _resetAgentStoreForTests, _getAgentForTests } from "./server";
import { WorkflowSystemMetadata } from "./agent/util/workflow-system-metadata";
import { mintTestToken } from "./api/test-token";
import { env } from "./config/env";
import { ConsoleMessageType, OperatorState } from "./types/execution";

const API = env.API_PREFIX;
const app = buildApp();

const TOKEN = mintTestToken();
const OTHER_USER_TOKEN = mintTestToken(2);
// TOKEN's claims under a signature the dashboard service did not make.
const FORGED_TOKEN = mintTestToken(1, "not-the-secret");

function url(path: string): string {
  return `http://localhost${path}`;
}

// Every agent route needs a user, so the helpers below act as TOKEN's user
// unless a test names another token, or null for none.
function bearer(token: string | null): Record<string, string> {
  return token ? { Authorization: `Bearer ${token}` } : {};
}

async function postJson(
  path: string,
  body: unknown,
  headers: Record<string, string> = bearer(TOKEN)
): Promise<Response> {
  return app.handle(
    new Request(url(path), {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify(body),
    })
  );
}

async function createAgent(body: Record<string, unknown> = {}, token: string | null = TOKEN): Promise<Response> {
  return postJson(`${API}/agents`, { modelType: "m", ...body }, bearer(token));
}

async function patchJson(path: string, body: unknown, token: string | null = TOKEN): Promise<Response> {
  return app.handle(
    new Request(url(path), {
      method: "PATCH",
      headers: { "Content-Type": "application/json", ...bearer(token) },
      body: JSON.stringify(body),
    })
  );
}

async function getJson(path: string, token: string | null = TOKEN): Promise<Response> {
  return app.handle(new Request(url(path), { headers: bearer(token) }));
}

async function del(path: string, token: string | null = TOKEN): Promise<Response> {
  return app.handle(new Request(url(path), { method: "DELETE", headers: bearer(token) }));
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
  test("creates an agent for the delegating user", async () => {
    const res = await createAgent({ modelType: "test-model", name: "Tester" });
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
  });

  test("auto-numbers agent ids monotonically", async () => {
    const a = await readJson<{ id: string }>(await createAgent());
    const b = await readJson<{ id: string }>(await createAgent());

    const aNum = Number(a.id.split("-")[1]);
    const bNum = Number(b.id.split("-")[1]);
    expect(bNum).toBe(aNum + 1);
  });

  test("rejects invalid token", async () => {
    const res = await createAgent({}, "obviously-not-a-jwt");
    expect(res.status).toBe(401);
    const body = await readJson<{ error: string }>(res);
    expect(body.error).toBe("Invalid or expired token");
  });

  test("rejects missing Authorization header", async () => {
    const res = await createAgent({}, null);
    expect(res.status).toBe(401);
    const body = await readJson<{ error: string }>(res);
    expect(body.error).toBe("Authorization header with a Bearer token is required");
  });

  test("rejects non-Bearer Authorization header", async () => {
    const res = await postJson(`${API}/agents`, { modelType: "m" }, { Authorization: `Basic ${TOKEN}` });
    expect(res.status).toBe(401);
    const body = await readJson<{ error: string }>(res);
    expect(body.error).toBe("Authorization header with a Bearer token is required");
  });

  test("rejects missing modelType", async () => {
    const res = await createAgent({ modelType: undefined, name: "no-model" });
    expect(res.status).toBe(400);
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
    await createAgent({ name: "one" });
    await createAgent({ name: "two" });

    const res = await getJson(`${API}/agents`);
    const body = await readJson<{ agents: { name: string }[] }>(res);
    expect(body.agents).toHaveLength(2);
    expect(body.agents.map(a => a.name).sort()).toEqual(["one", "two"]);
  });

  test("lists each user only their own agents", async () => {
    await createAgent({ name: "mine" });
    await createAgent({ name: "theirs" }, OTHER_USER_TOKEN);

    const names = async (token: string) =>
      (await readJson<{ agents: { name: string }[] }>(await getJson(`${API}/agents`, token))).agents.map(a => a.name);
    expect(await names(TOKEN)).toEqual(["mine"]);
    expect(await names(OTHER_USER_TOKEN)).toEqual(["theirs"]);
  });

  test("rejects a request without a token", async () => {
    const res = await getJson(`${API}/agents`, null);
    expect(res.status).toBe(401);
  });

  test("rejects a token the dashboard service did not sign, and lists it nothing", async () => {
    await createAgent({ name: "mine" });

    const res = await getJson(`${API}/agents`, FORGED_TOKEN);
    expect(res.status).toBe(401);
    expect(await readJson<unknown>(res)).toEqual({ error: "Invalid or expired token" });
  });
});

// An agent answers only to the user who created it. For anyone else every route
// that names it gives the answer for an agent that does not exist.
describe("an agent created by another user", () => {
  // Every row names its body, `undefined` for none. Given a row shorter than the test's parameter
  // list, Bun fills the missing place with a `done` callback: the request then carries a JSON
  // content type, and the test can time out waiting for a callback that nobody calls.
  const ROUTES: [method: string, path: string, body: unknown][] = [
    ["GET", "", undefined],
    ["DELETE", "", undefined],
    ["GET", "/react-steps", undefined],
    ["GET", "/operator-results", undefined],
    ["POST", "/steps-by-operators", { operatorIds: [] }],
    ["GET", "/system-info", undefined],
    ["POST", "/stop", {}],
    ["POST", "/clear", {}],
    ["GET", "/operator-types", undefined],
    ["GET", "/settings", undefined],
    ["PATCH", "/settings", { maxSteps: 3 }],
  ];

  function request(method: string, path: string, body: unknown, token: string | null): Promise<Response> {
    return app.handle(
      new Request(url(path), {
        method,
        headers: { ...(body === undefined ? {} : { "Content-Type": "application/json" }), ...bearer(token) },
        body: body === undefined ? undefined : JSON.stringify(body),
      })
    );
  }

  test.each(ROUTES)("%s /:id%s is not found, and needs a signed token", async (method, path, body) => {
    const id = (await readJson<{ id: string }>(await createAgent())).id;
    const settingsBefore = _getAgentForTests(id)!.getSettings().maxSteps;

    const asOther = await request(method, `${API}/agents/${id}${path}`, body, OTHER_USER_TOKEN);
    expect(asOther.status).toBe(404);
    expect((await readJson<{ error: string }>(asOther)).error).toBe("Agent not found");

    const withoutToken = await request(method, `${API}/agents/${id}${path}`, body, null);
    expect(withoutToken.status).toBe(401);

    // The owner's own claims, forged: the user id in a token counts only once it is signed.
    const forged = await request(method, `${API}/agents/${id}${path}`, body, FORGED_TOKEN);
    expect(forged.status).toBe(401);

    // None of the calls reached the agent: it is still there for its owner, unchanged.
    expect((await getJson(`${API}/agents/${id}`)).status).toBe(200);
    expect(_getAgentForTests(id)!.getSettings().maxSteps).toBe(settingsBefore);
  });
});

describe(`GET ${API}/agents/:id`, () => {
  test("returns the agent plus its workflow snapshot", async () => {
    const created = await readJson<{ id: string }>(await createAgent());

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
    const created = await readJson<{ id: string }>(await createAgent());

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
  test("POST /:id/stop mid-run returns STOPPING", async () => {
    const created = await readJson<{ id: string }>(await createAgent());
    (_getAgentForTests(created.id)! as any).state = "GENERATING";
    const res = await postJson(`${API}/agents/${created.id}/stop`, {});
    expect(res.status).toBe(200);
    expect(await readJson<unknown>(res)).toEqual({ status: "STOPPING" });
  });

  test("POST /:id/stop on an idle agent returns AVAILABLE", async () => {
    const created = await readJson<{ id: string }>(await createAgent());
    const res = await postJson(`${API}/agents/${created.id}/stop`, {});
    expect(res.status).toBe(200);
    expect(await readJson<unknown>(res)).toEqual({ status: "AVAILABLE" });
  });

  test("POST /:id/clear resets history", async () => {
    const created = await readJson<{ id: string }>(await createAgent());
    const res = await postJson(`${API}/agents/${created.id}/clear`, {});
    expect(res.status).toBe(200);
    expect(await readJson<unknown>(res)).toEqual({ status: "cleared" });
  });

  test("GET /:id/operator-results returns an empty map on the framework build", async () => {
    const created = await readJson<{ id: string }>(await createAgent());
    const res = await getJson(`${API}/agents/${created.id}/operator-results`);
    expect(res.status).toBe(200);
    expect(await readJson<unknown>(res)).toEqual({ results: {} });
  });
});

describe(`PATCH ${API}/agents/:id/settings`, () => {
  test("updates settings and returns the new values", async () => {
    const created = await readJson<{ id: string }>(await createAgent());

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

  test("refuses a step limit above 50, the settings dialog's maximum", async () => {
    const created = await readJson<{ id: string }>(await createAgent());
    const settingsPath = `${API}/agents/${created.id}/settings`;
    const readMaxSteps = async () => (await readJson<{ maxSteps: number }>(await getJson(settingsPath))).maxSteps;
    const before = await readMaxSteps();

    expect((await patchJson(settingsPath, { maxSteps: 51 })).status).toBe(400);
    expect(await readMaxSteps()).toBe(before);

    expect((await patchJson(settingsPath, { maxSteps: 50 })).status).toBe(200);
    expect(await readMaxSteps()).toBe(50);
  });
});

describe("agent creation edge cases", () => {
  test("rejects an empty modelType", async () => {
    // The body schema accepts any string, so the handler's own guard runs.
    const res = await postJson(`${API}/agents`, { modelType: "" }, { Authorization: `Bearer ${TOKEN}` });
    expect(res.status).toBe(400);
    expect((await readJson<{ error: string }>(res)).error).toContain("modelType");
  });

  test("applies initial settings supplied at creation time", async () => {
    const res = await createAgent({ settings: { maxSteps: 9, toolTimeoutSeconds: 12 } });
    expect(res.status).toBe(200);
    const body = await readJson<{ settings: { maxSteps: number; toolTimeoutSeconds: number } }>(res);
    expect(body.settings.maxSteps).toBe(9);
    expect(body.settings.toolTimeoutSeconds).toBe(12);
  });

  test("refuses an initial step limit above 50 without creating the agent", async () => {
    expect((await createAgent({ settings: { maxSteps: 51 } })).status).toBe(400);
    expect((await readJson<{ agents: unknown[] }>(await getJson(`${API}/agents`))).agents).toHaveLength(0);

    const res = await createAgent({ settings: { maxSteps: 50 } });
    expect(res.status).toBe(200);
    expect((await readJson<{ settings: { maxSteps: number } }>(res)).settings.maxSteps).toBe(50);
  });

  test("creates the agent even when the workflow load fails (non-fatal)", async () => {
    // retrieveWorkflow targets the (unavailable) dashboard service; the failure
    // is caught and the agent is still created.
    const res = await createAgent({ workflowId: 123 });
    expect(res.status).toBe(200);
  });

  test("masks the delegate token in agent info", async () => {
    const id = (await readJson<{ id: string }>(await createAgent())).id;
    _getAgentForTests(id)!.setDelegateConfig({
      userToken: "super-secret",
      userInfo: { uid: 1, email: "tester@example.com" },
      workflowId: 5,
      workflowName: "My Flow",
      computingUnitId: 2,
    } as any);

    const info = await readJson<{ delegate?: { userToken: string; workflowName: string } }>(
      await getJson(`${API}/agents/${id}`)
    );
    expect(info.delegate?.userToken).toBe("***");
    expect(info.delegate?.workflowName).toBe("My Flow");
  });
});

describe("agent read routes", () => {
  let id: string;
  beforeEach(async () => {
    id = (await readJson<{ id: string }>(await createAgent())).id;
  });

  test("GET /:id/react-steps returns steps and state", async () => {
    const body = await readJson<{ steps: unknown[]; state: string }>(await getJson(`${API}/agents/${id}/react-steps`));
    expect(Array.isArray(body.steps)).toBe(true);
    expect(body.state).toBe("AVAILABLE");
  });

  test("GET /:id/system-info responds", async () => {
    const res = await getJson(`${API}/agents/${id}/system-info`);
    expect(res.status).toBe(200);
  });

  test("GET /:id/operator-types returns a list", async () => {
    const res = await getJson(`${API}/agents/${id}/operator-types`);
    expect(res.status).toBe(200);
    expect(Array.isArray(await readJson(res))).toBe(true);
  });

  test("POST /:id/steps-by-operators returns steps", async () => {
    const res = await postJson(`${API}/agents/${id}/steps-by-operators`, { operatorIds: [] });
    expect(res.status).toBe(200);
    expect(Array.isArray((await readJson<{ steps: unknown[] }>(res)).steps)).toBe(true);
  });

  test("GET /:id/operator-results returns only each visible operator's result summary", async () => {
    const agent = _getAgentForTests(id)!;
    const resultSummary = {
      resultMode: { type: "PaginationMode" },
      sampleTuples: [[0, { schema: { attributes: [{ attributeName: "a", attributeType: "string" }] }, fields: [1] }]],
      totalTuplesCount: 2,
    };
    (agent as any).getWorkflowResultState = () => ({
      getAllVisible: () =>
        new Map([
          [
            "op-1",
            {
              operatorInfo: {
                state: OperatorState.COMPLETED,
                errorMessages: [],
                resultSummary,
                consoleMessages: [{ msgType: ConsoleMessageType.PRINT, title: "stack trace", message: "at ..." }],
              },
            },
          ],
          ["op-2", { operatorInfo: { state: OperatorState.FAILED, errorMessages: [] } }],
        ]),
    });

    const body = await readJson<{ results: Record<string, unknown> }>(
      await getJson(`${API}/agents/${id}/operator-results`)
    );
    // The popover reads only resultSummary, so state and console messages stay server-side.
    expect(body.results).toEqual({ "op-1": { resultSummary }, "op-2": {} });
  });
});

describe("non-router routes", () => {
  test("unknown routes fall through to the catch-all error handler", async () => {
    const res = await getJson("/no-such-route");
    expect(res.status).toBe(500);
  });
});

describe("start()", () => {
  test("boots a listening app and prints the startup banner", async () => {
    const booted = await start();
    expect(typeof booted.server?.port).toBe("number");
    await booted.stop();
  });

  test("tolerates a metadata-initialization failure", async () => {
    const spy = spyOn(WorkflowSystemMetadata, "initializeGlobal").mockImplementation(async () => {
      throw new Error("metadata unavailable");
    });
    const booted = await start();
    await booted.stop();
    expect(spy).toHaveBeenCalled();
    spy.mockRestore();
  });
});
