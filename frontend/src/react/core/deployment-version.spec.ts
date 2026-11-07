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

import { afterEach, describe, expect, it, vi } from "vitest";
import { DeploymentVersionMonitor, VERSION_POLL_INTERVAL_MS } from "./deployment-version";
afterEach(() => vi.useRealTimers());
describe("deployment version monitor", () => {
  it("requests the same cache-busted manifest and accepts only a nonempty different buildNumber", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(12345);
    const fetch = vi.fn(async () => Response.json({ buildNumber: "new-build" }));
    const monitor = new DeploymentVersionMonitor({ fetch, buildNumber: "dev" });
    expect(await monitor.checkForUpdate()).toBe(true);
    expect(fetch).toHaveBeenCalledWith("assets/version.json?t=12345", expect.objectContaining({ method: "GET" }));
    for (const value of [{ buildNumber: "dev" }, {}, { buildNumber: "" }, { buildNumber: 123 }, null, [], "bad"]) {
      fetch.mockResolvedValueOnce(Response.json(value));
      expect(await monitor.checkForUpdate()).toBe(false);
    }
    fetch.mockResolvedValueOnce(Response.json({ buildNumber: " " }));
    expect(await monitor.checkForUpdate()).toBe(true);
    fetch.mockResolvedValueOnce(Response.json({ buildNumber: "构建" }));
    expect(await monitor.checkForUpdate()).toBe(true);
  });
  it("returns false for HTTP/network/invalid JSON errors", async () => {
    const fetch = vi
      .fn()
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValueOnce(new Response("missing", { status: 404 }))
      .mockResolvedValueOnce(new Response("invalid JSON"));
    const monitor = new DeploymentVersionMonitor({ fetch, buildNumber: "dev" });
    expect(await monitor.checkForUpdate()).toBe(false);
    expect(await monitor.checkForUpdate()).toBe(false);
    expect(await monitor.checkForUpdate()).toBe(false);
  });
  it("waits five minutes, continues through failures, prompts exactly once and stops polling", async () => {
    vi.useFakeTimers();
    const fetch = vi
      .fn()
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValueOnce(Response.json({ buildNumber: "dev" }))
      .mockResolvedValue(Response.json({ buildNumber: "new" }));
    const monitor = new DeploymentVersionMonitor({ fetch, buildNumber: "dev" });
    const listener = vi.fn();
    monitor.subscribe(listener);
    const subscription = monitor.startPollingForUpdates();
    expect(VERSION_POLL_INTERVAL_MS).toBe(300000);
    await vi.advanceTimersByTimeAsync(299999);
    expect(fetch).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(600001);
    expect(fetch).toHaveBeenCalledTimes(3);
    expect(monitor.getSnapshot()).toBe(true);
    expect(listener).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(900000);
    expect(fetch).toHaveBeenCalledTimes(3);
    subscription.unsubscribe();
  });
  it("cancels superseded/unmounted requests and ignores late responses", async () => {
    vi.useFakeTimers();
    const requests: { signal?: AbortSignal; resolve: (value: Response) => void }[] = [];
    const fetch = vi.fn(
      (_url, options) => new Promise<Response>(resolve => requests.push({ signal: options?.signal, resolve }))
    );
    const monitor = new DeploymentVersionMonitor({ fetch, buildNumber: "dev" });
    const subscription = monitor.startPollingForUpdates(1000);
    await vi.advanceTimersByTimeAsync(2000);
    expect(requests[0].signal?.aborted).toBe(true);
    requests[0].resolve(Response.json({ buildNumber: "new" }));
    await Promise.resolve();
    expect(monitor.getSnapshot()).toBe(false);
    subscription.unsubscribe();
    expect(requests[1].signal?.aborted).toBe(true);
    requests[1].resolve(Response.json({ buildNumber: "new" }));
    await Promise.resolve();
    expect(monitor.getSnapshot()).toBe(false);
    expect(() => monitor.startPollingForUpdates(0)).toThrow("positive");
  });
});
