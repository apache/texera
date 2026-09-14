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

import { afterEach, describe, expect, test } from "bun:test";
import { fetchOperatorMetadata, getBackendConfig } from "./backend-api";

const realFetch = globalThis.fetch;
let lastUrl = "";

function stubFetch(handler: () => Response): void {
  globalThis.fetch = (async (url: unknown) => {
    lastUrl = String(url);
    return handler();
  }) as unknown as typeof fetch;
}

afterEach(() => {
  globalThis.fetch = realFetch;
});

describe("getBackendConfig", () => {
  test("exposes the configured endpoints and returns a defensive copy", () => {
    const a = getBackendConfig();
    const b = getBackendConfig();
    expect(a).not.toBe(b); // a fresh object each call
    expect(typeof a.apiEndpoint).toBe("string");
    expect(typeof a.compileEndpoint).toBe("string");
    expect(typeof a.executionEndpoint).toBe("string");
  });
});

describe("fetchOperatorMetadata", () => {
  test("requests the operator-metadata resource and returns the parsed body", async () => {
    const metadata = { operators: [], groups: [] };
    stubFetch(() => new Response(JSON.stringify(metadata), { status: 200 }));

    const result = await fetchOperatorMetadata();

    expect(result).toEqual(metadata);
    expect(lastUrl).toMatch(/\/api\/resources\/operator-metadata$/);
  });

  test("throws with the status on a non-ok response", async () => {
    stubFetch(() => new Response("err", { status: 503, statusText: "Service Unavailable" }));
    await expect(fetchOperatorMetadata()).rejects.toThrow(/Failed to fetch operator metadata: 503/);
  });
});
