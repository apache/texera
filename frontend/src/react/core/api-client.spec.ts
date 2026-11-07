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

import { describe, expect, it, vi } from "vitest";
import { ApiClient, ApiError } from "./api-client";

describe("ApiClient", () => {
  it("uses the root API and attaches the current token with cancellation", async () => {
    const fetch = vi.fn().mockResolvedValue(Response.json({ value: "✓" }));
    const api = new ApiClient({ fetch, getToken: () => "token" });
    const controller = new AbortController();
    await expect(api.post("workflow", { name: "日本語" }, { signal: controller.signal })).resolves.toEqual({ value: "✓" });
    const [url, init] = fetch.mock.calls[0];
    expect(url).toBe("/api/workflow");
    expect(new Headers(init.headers).get("Authorization")).toBe("Bearer token");
    expect(JSON.parse(init.body)).toEqual({ name: "日本語" });
    expect(init.signal).toBe(controller.signal);
  });

  it("does not attach credentials to anonymous config or external URLs", async () => {
    const fetch = vi.fn().mockResolvedValue(Response.json({}));
    const api = new ApiClient({ fetch, getToken: () => "secret" });
    await api.get("config/pre-login");
    expect(new Headers(fetch.mock.calls[0][1].headers).has("Authorization")).toBe(false);
    await expect(api.get("https://elsewhere.example/data")).rejects.toThrow("API path must be relative");
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("preserves server errors as plain data and invalidates authenticated 401s only", async () => {
    const fetch = vi.fn().mockImplementation(() => Promise.resolve(Response.json({ message: "<a>Rejected</a>" }, { status: 401 })));
    const onUnauthorized = vi.fn();
    const api = new ApiClient({ fetch, getToken: () => "token", onUnauthorized });
    await expect(api.get("workflow/list")).rejects.toMatchObject({ status: 401, message: "<a>Rejected</a>" });
    expect(onUnauthorized).toHaveBeenCalledTimes(1);
    await expect(api.post("auth/login", {})).rejects.toBeInstanceOf(ApiError);
    expect(onUnauthorized).toHaveBeenCalledTimes(1);
  });

  it("handles empty, text, binary and malformed JSON responses explicitly", async () => {
    const fetch = vi.fn()
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response("plain"))
      .mockResolvedValueOnce(new Response("bytes"))
      .mockResolvedValueOnce(new Response("not json"));
    const api = new ApiClient({ fetch });
    await expect(api.delete("workflow/1")).resolves.toBeUndefined();
    await expect(api.get("auth/google/clientid", { responseType: "text" })).resolves.toBe("plain");
    expect(await api.get("dataset/1/download", { responseType: "blob" })).toBeInstanceOf(Blob);
    await expect(api.get("workflow/list")).rejects.toThrow("Invalid JSON response");
  });

  it("preserves caller FormData headers and exposes HTTP errors for non-JSON bodies", async () => {
    const fetch = vi.fn().mockResolvedValueOnce(Response.json({})).mockResolvedValueOnce(new Response("Unavailable", { status: 503 }));
    const api = new ApiClient({ fetch });
    const form = new FormData();
    form.set("file", "data");
    await api.post("dataset/upload", form);
    expect(fetch.mock.calls[0][1].body).toBe(form);
    expect(new Headers(fetch.mock.calls[0][1].headers).has("Content-Type")).toBe(false);
    await expect(api.get("config/pre-login")).rejects.toMatchObject({ status: 503, message: "Unavailable" });
  });
});

