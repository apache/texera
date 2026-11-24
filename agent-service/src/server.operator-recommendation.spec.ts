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

import { afterEach, beforeEach, describe, expect, spyOn, test, type Mock } from "bun:test";
import { buildApp } from "./server";
import { env } from "./config/env";
import { WorkflowSystemMetadata } from "./agent/util/workflow-system-metadata";
import { OPERATOR_CATALOG_FIXTURE } from "./operator-recommendation/operator-catalog.fixture";
import type { OperatorRecommendationResponse } from "./operator-recommendation/operator-recommendation-types";

const API = env.API_PREFIX;
const ROUTE = `http://localhost${API}/operator-recommendations`;
const app = buildApp();

function mintTestToken(expiresInSeconds = 3600): string {
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const payload = Buffer.from(
    JSON.stringify({
      sub: "tester",
      userId: 1,
      role: "REGULAR",
      exp: Math.floor(Date.now() / 1000) + expiresInSeconds,
    })
  ).toString("base64url");
  return `${header}.${payload}.test-signature`;
}

const TOKEN = mintTestToken();

async function postRecommend(body: unknown, token: string | null = TOKEN): Promise<Response> {
  return app.handle(
    new Request(ROUTE, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify(body),
    })
  );
}

async function recommend(body: unknown): Promise<OperatorRecommendationResponse> {
  const res = await postRecommend(body);
  expect(res.status).toBe(200);
  return (await res.json()) as OperatorRecommendationResponse;
}

// Each test gets a fresh, unloaded catalog singleton, and the backend's
// operator-metadata endpoint is served by a fetch mock, so the route loads the
// real WorkflowSystemMetadata class from the fixture on its first request.
let savedCatalog: unknown;
let fetchSpy: Mock<typeof fetch>;

function respondWith(makeResponse: () => Response): void {
  fetchSpy.mockImplementation((async () => makeResponse()) as unknown as typeof fetch);
}

function serveCatalog(): void {
  respondWith(() => Response.json(OPERATOR_CATALOG_FIXTURE));
}

function failCatalog(): void {
  respondWith(() => new Response("backend not ready", { status: 503 }));
}

beforeEach(() => {
  savedCatalog = (WorkflowSystemMetadata as any).instance;
  (WorkflowSystemMetadata as any).instance = undefined;
  fetchSpy = spyOn(globalThis, "fetch");
  serveCatalog();
});

afterEach(() => {
  fetchSpy.mockRestore();
  (WorkflowSystemMetadata as any).instance = savedCatalog;
});

describe("POST /operator-recommendations", () => {
  test("returns validated, named suggestions from the loaded catalog", async () => {
    const data = await recommend({ operatorType: "CSVFileScan" });
    expect(data.strategy).toBe("hardcoded");
    expect(data.recommendations.map(r => r.operatorType)).toEqual(["Filter", "Projection", "KeywordSearch"]);
    expect(data.recommendations.map(r => r.userFriendlyName)).toEqual(["Filter", "Projection", "Keyword Search"]);
    for (const rec of data.recommendations) {
      expect(rec.targetPortID).toBe("input-0");
      expect(rec.score).toBeGreaterThan(0);
      expect(rec.reason.length).toBeGreaterThan(0);
    }
  });

  test("classifies from catalog metadata and targets the model port", async () => {
    const trained = await recommend({ operatorType: "SklearnTrainingLogisticRegression" });
    expect(trained.recommendations.map(r => [r.operatorType, r.targetPortID])).toEqual([
      ["SklearnPrediction", "input-0"],
      ["SklearnTesting", "input-0"],
    ]);
    expect((await recommend({ operatorType: "Scatterplot" })).recommendations).toEqual([]);
  });

  test("honors the requested source port and limit", async () => {
    expect((await recommend({ operatorType: "Split", sourcePortID: "output-1" })).recommendations.length).toBe(3);
    expect((await recommend({ operatorType: "Filter", sourcePortID: "output-1" })).recommendations).toEqual([]);
    expect((await recommend({ operatorType: "Filter", limit: 1 })).recommendations).toHaveLength(1);
  });

  test("retries loading the catalog on a later request when the backend was not ready", async () => {
    failCatalog();
    // Nothing can be checked without the catalog, so nothing is suggested.
    expect((await recommend({ operatorType: "Filter" })).recommendations).toEqual([]);

    serveCatalog();
    const after = await recommend({ operatorType: "Filter" });
    expect(after.recommendations[0].userFriendlyName).toBe("Aggregate");
    expect(WorkflowSystemMetadata.getInstance().isInitialized()).toBe(true);

    // Once loaded, the catalog is not fetched again.
    const fetchesSoFar = fetchSpy.mock.calls.length;
    await recommend({ operatorType: "Filter" });
    expect(fetchSpy.mock.calls.length).toBe(fetchesSoFar);
  });

  test("concurrent requests share one catalog load", async () => {
    let release!: () => void;
    const pending = new Promise<void>(resolve => (release = resolve));
    respondWith(() => Response.json(OPERATOR_CATALOG_FIXTURE));
    const serve = fetchSpy.getMockImplementation()!;
    fetchSpy.mockImplementation((async (...args: Parameters<typeof fetch>) => {
      await pending;
      return serve(...args);
    }) as unknown as typeof fetch);

    const first = recommend({ operatorType: "Filter" });
    const second = recommend({ operatorType: "CSVFileScan" });
    release();
    const [a, b] = await Promise.all([first, second]);

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    expect(a.recommendations[0].userFriendlyName).toBe("Aggregate");
    expect(b.recommendations[0].userFriendlyName).toBe("Filter");
  });

  test("suggests nothing for an operator the loaded catalog does not know", async () => {
    expect((await recommend({ operatorType: "TotallyMadeUpOp" })).recommendations).toEqual([]);
    expect((await recommend({ operatorType: "TotallyMadeUpOp", sourcePortID: "output-7" })).recommendations).toEqual(
      []
    );
  });

  test("rejects a request without a Bearer token as 401", async () => {
    const res = await postRecommend({ operatorType: "Filter" }, null);
    expect(res.status).toBe(401);
    expect(((await res.json()) as { error: string }).error).toBe(
      "Authorization header with a Bearer token is required"
    );
  });

  test("rejects an expired or malformed token as 401", async () => {
    for (const token of [mintTestToken(-60), "obviously-not-a-jwt"]) {
      const res = await postRecommend({ operatorType: "Filter" }, token);
      expect(res.status).toBe(401);
      expect(((await res.json()) as { error: string }).error).toBe("Invalid or expired token");
    }
  });

  test("rejects invalid bodies as 400", async () => {
    const invalid: unknown[] = [
      { limit: 3 },
      { operatorType: "" },
      { operatorType: "   " },
      { operatorType: "X".repeat(129) },
      { operatorType: "Filter", sourcePortID: "input-0" },
      { operatorType: "Filter", sourcePortID: "output-00" },
      { operatorType: "Filter", sourcePortID: "output-01" },
      { operatorType: "Filter", sourcePortID: `output-1${"0".repeat(9)}` },
    ];
    for (const body of invalid) {
      expect((await postRecommend(body)).status).toBe(400);
    }
  });

  test("rejects malformed JSON as 400", async () => {
    const res = await app.handle(
      new Request(ROUTE, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${TOKEN}` },
        body: "{ not json",
      })
    );
    expect(res.status).toBe(400);
  });
});
