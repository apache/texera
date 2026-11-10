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
import { ApiClient } from "./api-client";
import { RuntimeConfig } from "./runtime-config";

describe("RuntimeConfig", () => {
  it("loads public config before authenticated sources and merges in server order", async () => {
    const paths: string[] = [];
    const fetch = vi.fn(async (url: string) => {
      paths.push(url);
      return Response.json(
        url.endsWith("pre-login")
          ? { localLogin: true, inviteOnly: false }
          : url.endsWith("user-system")
            ? { inviteOnly: true }
            : url.endsWith("amber")
              ? { defaultDataTransferBatchSize: 400 }
              : { warehouseEnabled: true }
      );
    });
    const config = new RuntimeConfig(new ApiClient({ fetch }));
    await config.load(true);
    expect(paths[0]).toBe("/api/config/pre-login");
    expect(paths).toHaveLength(4);
    expect(config.env).toMatchObject({
      localLogin: true,
      inviteOnly: true,
      warehouseEnabled: true,
      defaultDataTransferBatchSize: 400,
    });
  });

  it("skips protected endpoints for anonymous users and propagates public config failures", async () => {
    const fetch = vi.fn().mockResolvedValue(Response.json({ localLogin: false }));
    const config = new RuntimeConfig(new ApiClient({ fetch }));
    await config.load(false);
    expect(fetch).toHaveBeenCalledTimes(1);
    fetch.mockResolvedValue(new Response("offline", { status: 503 }));
    await expect(config.loadPreLogin()).rejects.toMatchObject({ status: 503 });
  });

  it("retains public config on protected config failure and clears protected config on logout", async () => {
    const fetch = vi
      .fn()
      .mockImplementation((url: string) =>
        Promise.resolve(
          url.endsWith("pre-login") ? Response.json({ localLogin: true }) : Response.json({ warehouseEnabled: true })
        )
      );
    const config = new RuntimeConfig(new ApiClient({ fetch }));
    await config.load(true);
    config.clearAuthenticated();
    expect(config.env).toEqual({ localLogin: true });
    fetch.mockResolvedValue(new Response(null, { status: 403 }));
    await expect(config.loadPostLogin()).rejects.toMatchObject({ status: 403 });
    expect(config.env).toEqual({ localLogin: true });
  });

  it("does not restore protected configuration after logout overtakes a request", async () => {
    let finish!: () => void;
    const gate = new Promise<void>(resolve => {
      finish = resolve;
    });
    const fetch = vi.fn(async () => {
      await gate;
      return Response.json({ warehouseEnabled: true });
    });
    const config = new RuntimeConfig(new ApiClient({ fetch }));
    const pending = config.loadPostLogin();
    config.clearAuthenticated();
    finish();
    await pending;
    expect(config.env).toEqual({});
  });

  it("keeps the newest protected configuration when an older request completes last", async () => {
    let finish!: () => void;
    const gate = new Promise<void>(resolve => {
      finish = resolve;
    });
    let requests = 0;
    const fetch = vi.fn(async () => {
      const old = requests++ < 3;
      if (old) await gate;
      return Response.json({ configurationOwner: old ? "old" : "new" });
    });
    const config = new RuntimeConfig(new ApiClient({ fetch }));
    const older = config.loadPostLogin();
    await config.loadPostLogin();
    expect(config.env.configurationOwner).toBe("new");
    finish();
    await older;
    expect(config.env.configurationOwner).toBe("new");
  });
});
