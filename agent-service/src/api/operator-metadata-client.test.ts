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

import { describe, expect, test, afterEach } from "bun:test";
import { fetchOperatorMetadata } from "./operator-metadata-client";

const realFetch = globalThis.fetch;
let lastUrl: string | undefined;

function mockFetch(responder: () => Response): void {
  globalThis.fetch = (async (input: any) => {
    lastUrl = String(input);
    return responder();
  }) as unknown as typeof fetch;
}

afterEach(() => {
  globalThis.fetch = realFetch;
  lastUrl = undefined;
});

describe("fetchOperatorMetadata", () => {
  test("fetches operator metadata from the Dashboard Service endpoint", async () => {
    const payload = { operators: [], groups: [] };
    mockFetch(() => new Response(JSON.stringify(payload), { status: 200 }));

    const result = await fetchOperatorMetadata();

    expect(result).toEqual(payload);
    expect(lastUrl).toBe("http://localhost:8080/api/resources/operator-metadata");
  });

  test("throws with status text when the response is not OK", async () => {
    mockFetch(() => new Response("nope", { status: 503, statusText: "Service Unavailable" }));

    await expect(fetchOperatorMetadata()).rejects.toThrow("Failed to fetch operator metadata: 503");
  });
});
