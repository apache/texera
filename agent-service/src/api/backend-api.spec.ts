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

import { afterEach, describe, expect, mock, spyOn, test } from "bun:test";
import { fetchOperatorMetadata, getBackendConfig, type OperatorMetadata } from "./backend-api";

const metadata: OperatorMetadata = { operators: [], groups: [] };

describe("getBackendConfig", () => {
  test("exposes the four service endpoints", () => {
    const config = getBackendConfig();

    expect(Object.keys(config).sort()).toEqual([
      "apiEndpoint",
      "compileEndpoint",
      "executionEndpoint",
      "modelsEndpoint",
    ]);
  });

  test("hands out a copy, so a caller cannot repoint the service endpoints", () => {
    // The config is module-level state shared by every API client; returning the live object would
    // let one caller's edit silently redirect everyone else's requests.
    const first = getBackendConfig();
    first.apiEndpoint = "http://evil.example.com";

    expect(getBackendConfig().apiEndpoint).not.toBe("http://evil.example.com");
  });
});

describe("fetchOperatorMetadata", () => {
  afterEach(() => {
    mock.restore();
  });

  test("reads the metadata from the dashboard service", async () => {
    const fetchSpy = spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(metadata), { status: 200 })
    );

    const result = await fetchOperatorMetadata();

    expect(result).toEqual(metadata);
    const [url] = fetchSpy.mock.calls[0] as [string];
    expect(url).toBe(`${getBackendConfig().apiEndpoint}/api/resources/operator-metadata`);
  });

  test("throws with the status when the metadata cannot be fetched", async () => {
    spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("down", { status: 503, statusText: "Service Unavailable" })
    );

    await expect(fetchOperatorMetadata()).rejects.toThrow(/Failed to fetch operator metadata: 503 Service Unavailable/);
  });

  test("lets a network failure surface rather than returning empty metadata", async () => {
    // A silent empty result here would leave the agent believing the cluster has zero operators.
    spyOn(globalThis, "fetch").mockRejectedValue(new Error("network down"));

    await expect(fetchOperatorMetadata()).rejects.toThrow(/network down/);
  });
});
