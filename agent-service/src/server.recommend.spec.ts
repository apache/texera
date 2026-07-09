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

import { describe, expect, test } from "bun:test";
import { buildApp } from "./server";
import { env } from "./config/env";
import type { RecommendationResponse } from "./recommender/recommender-types";

const API = env.API_PREFIX;
const app = buildApp();

async function postRecommend(body: unknown): Promise<Response> {
  return app.handle(
    new Request(`http://localhost${API}/recommend`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
  );
}

describe("POST /recommend", () => {
  test("returns a hardcoded ranked list for a valid operator", async () => {
    const res = await postRecommend({ operatorType: "Filter" });
    expect(res.status).toBe(200);
    const data = (await res.json()) as RecommendationResponse;
    expect(data.strategy).toBe("hardcoded");
    expect(data.recommendations.length).toBeGreaterThan(0);
    for (const rec of data.recommendations) {
      expect(typeof rec.operatorType).toBe("string");
      expect(rec.operatorType.length).toBeGreaterThan(0);
      expect(rec.score).toBeGreaterThan(0);
      expect(rec.reason.length).toBeGreaterThan(0);
    }
  });

  test("honors the requested limit", async () => {
    const res = await postRecommend({ operatorType: "Filter", limit: 1 });
    expect(res.status).toBe(200);
    const data = (await res.json()) as RecommendationResponse;
    expect(data.recommendations).toHaveLength(1);
  });

  test("rejects a request with no operatorType as 400", async () => {
    const res = await postRecommend({ limit: 3 });
    expect(res.status).toBe(400);
  });

  test("rejects an empty operatorType as 400", async () => {
    const res = await postRecommend({ operatorType: "" });
    expect(res.status).toBe(400);
  });

  test("rejects malformed JSON as 400", async () => {
    const res = await app.handle(
      new Request(`http://localhost${API}/recommend`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{ not json",
      })
    );
    expect(res.status).toBe(400);
  });
});
