/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { describe, expect, it, vi } from "vitest";
import { ApiClient } from "../core/api-client";
import { ResourceApi, parseComputingUnit } from "./api";

function setup(
  responder: (path: string, init?: RequestInit) => Response | Promise<Response> = () => Response.json({})
) {
  const fetch = vi.fn(responder);
  const api = new ResourceApi(new ApiClient({ fetch, getToken: () => "token" }), () => "token");
  return { api, fetch };
}

describe("resource API contracts", () => {
  it("preserves warehouse status/create/delete endpoints and payloads", async () => {
    const { api, fetch } = setup();
    await api.warehouseStatus();
    await api.createWarehouse("日本語");
    await api.deleteWarehouse(7);
    expect(fetch.mock.calls.map(([url, init]) => [url, init?.method ?? "GET"])).toEqual([
      ["/api/warehouse/status", "GET"],
      ["/api/warehouse", "POST"],
      ["/api/warehouse/7", "DELETE"],
    ]);
    expect(JSON.parse(String(fetch.mock.calls[1][1]?.body))).toEqual({ name: "日本語" });
  });

  it("preserves database environment CRUD and live package API query-token forwarding", async () => {
    const { api, fetch } = setup(url =>
      Response.json(url.includes("/pves?") ? [{ pveName: "env", userPackages: ["numpy==2"] }] : {})
    );
    await api.listEnvironments();
    await api.createEnvironment("env", { numpy: ">=2" });
    await api.updateEnvironment(2, "renamed", {});
    await api.deleteEnvironment(2);
    await api.systemPackages(3);
    expect(await api.userPackages(3, "env")).toEqual(["numpy==2"]);
    expect(await api.userPackages(3, "absent")).toEqual([]);
    await api.deletePackage(3, "a b", "numpy+extra");
    await api.deleteLiveEnvironments(3);
    expect(fetch.mock.calls.slice(0, 4).map(([url, init]) => [url, init?.method ?? "GET"])).toEqual([
      ["/api/pve/db", "GET"],
      ["/api/pve/db", "POST"],
      ["/api/pve/db/2", "PUT"],
      ["/api/pve/db/2", "DELETE"],
    ]);
    expect(JSON.parse(String(fetch.mock.calls[1][1]?.body))).toEqual({ name: "env", packages: { numpy: ">=2" } });
    expect(fetch.mock.calls.some(([url]) => url === "/api/pve/system?access-token=token&cuid=3")).toBe(true);
    expect(fetch.mock.calls.some(([url]) => url === "/api/pve/3/a%20b/packages/numpy%2Bextra?access-token=token")).toBe(
      true
    );
    expect(fetch.mock.calls.at(-1)?.[0]).toBe("/api/pve/pves/3");
  });

  it("constructs authenticated live PVE sockets for HTTP/HTTPS without corrupting names or packages", () => {
    const { api } = setup();
    const url = new URL(
      api.environmentSocketUrl(3, "a b/文", "install", ["a>=2", "x+y"], { protocol: "https:", host: "texera.example" })
    );
    expect(url.protocol).toBe("wss:");
    expect(url.pathname).toBe("/wsapi/pve");
    expect(url.searchParams.get("pveName")).toBe("a b/文");
    expect(JSON.parse(url.searchParams.get("packages")!)).toEqual(["a>=2", "x+y"]);
    expect(url.searchParams.get("access-token")).toBe("token");
    const anonymous = new ResourceApi(new ApiClient(), () => null);
    const local = new URL(
      anonymous.environmentSocketUrl(3, "env", "create", [], { protocol: "http:", host: "localhost:4200" })
    );
    expect(local.protocol).toBe("ws:");
    expect(local.searchParams.has("access-token")).toBe(false);
  });

  it("shares public settings reads, invalidates only after writes, and retries rejected reads", async () => {
    let failure = true;
    const { api, fetch } = setup(url =>
      url.endsWith("/public") && failure
        ? new Response("unavailable", { status: 503 })
        : Response.json({ home_enabled: "true" })
    );
    await expect(api.publicSettings()).rejects.toThrow("unavailable");
    failure = false;
    const [left, right] = await Promise.all([api.publicSettings(), api.publicSettings()]);
    expect(left).toEqual(right);
    expect(fetch).toHaveBeenCalledTimes(2);
    await api.updateSetting("logo", "data:image/png;base64,AA");
    const update = fetch.mock.calls.at(-1)!;
    expect(update[0]).toBe("/api/config/settings/logo");
    expect(update[1]?.credentials).toBe("include");
    expect(JSON.parse(String(update[1]?.body))).toEqual({ value: "data:image/png;base64,AA" });
    await api.publicSettings();
    expect(fetch).toHaveBeenCalledTimes(4);
    await api.resetSetting("logo");
    expect(fetch.mock.calls.at(-1)?.[0]).toBe("/api/config/settings/reset/logo");
    await api.settings();
    expect(fetch.mock.calls.at(-1)?.[0]).toBe("/api/config/settings");
  });

  it("reads sender as text and sends exact mail fields", async () => {
    const { api, fetch } = setup(url =>
      url.endsWith("/email") ? new Response("sender@example.com") : new Response(null, { status: 204 })
    );
    expect(await api.senderEmail()).toBe("sender@example.com");
    await api.sendEmail("Subject", "<unsafe>", "recipient@example.com");
    expect(fetch.mock.calls.at(-1)?.[0]).toBe("/api/gmail/send");
    expect(JSON.parse(String(fetch.mock.calls.at(-1)?.[1]?.body))).toEqual({
      receiver: "recipient@example.com",
      subject: "Subject",
      content: "<unsafe>",
    });
  });

  it("keeps admin user, execution list pagination/filter and admin quota contracts", async () => {
    const { api, fetch } = setup(url => Response.json(url.includes("totalWorkflow") ? 12 : []));
    await api.adminUsers();
    await api.addUser();
    await api.updateUser({ uid: 1, name: "A", email: "a@b.com", role: "REGULAR", comment: "note" });
    await api.executionList({
      pageSize: 5,
      pageIndex: 1,
      sortField: "startTime",
      sortDirection: "desc",
      filter: ["RUNNING", "PAUSED"],
    });
    expect(await api.totalExecutions()).toBe(12);
    expect(fetch.mock.calls.slice(0, 3).map(([url]) => url)).toEqual([
      "/api/admin/user/list",
      "/api/admin/user/add/",
      "/api/admin/user/update",
    ]);
    const listing = new URL(fetch.mock.calls[3][0], "https://texera.example");
    expect(listing.pathname).toBe("/api/admin/execution/executionList/5/1/startTime/desc");
    expect(listing.searchParams.get("filter")).toBe("RUNNING,PAUSED");
    await api.quota(4);
    expect(
      fetch.mock.calls
        .slice(-4)
        .map(([url]) => url)
        .sort()
    ).toEqual([
      "/api/admin/user/access_workflows?user_id=4",
      "/api/admin/user/created_datasets?user_id=4",
      "/api/admin/user/created_workflows?user_id=4",
      "/api/admin/user/user_quota_size?user_id=4",
    ]);
    await api.quota();
    expect(fetch.mock.calls.slice(-4).every(([url]) => url.startsWith("/api/quota/") && !url.includes("user_id"))).toBe(
      true
    );
    await api.deleteExecutionCollection(3, true);
    expect(fetch.mock.calls.at(-1)?.[0]).toBe("/api/admin/user/deleteCollection/3");
    await api.deleteExecutionCollection(3);
    expect(fetch.mock.calls.at(-1)?.[0]).toBe("/api/quota/deleteCollection/3");
  });

  it("keeps computing-unit create/resource/rename/terminate contracts", async () => {
    const raw = { computingUnit: { cuid: 1, name: "unit", resource: '{"cpuLimit":"2"}' } };
    const { api, fetch } = setup(url =>
      Response.json(url === "/api/computing-unit" || url.endsWith("/admin/list") ? [raw] : raw)
    );
    expect((await api.computingUnits())[0].computingUnit.resource.cpuLimit).toBe("2");
    await api.computingUnits(true);
    await api.computingUnit(1);
    await api.createComputingUnit({ unitType: "local", name: "Local", uri: "localhost:8085" });
    expect(JSON.parse(String(fetch.mock.calls.at(-1)?.[1]?.body))).toEqual({
      name: "Local",
      uri: "localhost:8085",
      unitType: "local",
      cpuLimit: "NaN",
      memoryLimit: "NaN",
      gpuLimit: "NaN",
      jvmMemorySize: "NaN",
      shmSize: "NaN",
    });
    await api.createComputingUnit({
      unitType: "kubernetes",
      name: "Pod",
      cpuLimit: "2",
      memoryLimit: "4Gi",
      gpuLimit: "0",
      jvmMemorySize: "2G",
      shmSize: "1Gi",
      iid: 8,
    });
    expect(JSON.parse(String(fetch.mock.calls.at(-1)?.[1]?.body)).iid).toBe(8);
    await api.renameComputingUnit(1, "name / 文");
    expect(fetch.mock.calls.at(-1)?.[0]).toBe("/api/computing-unit/1/rename/name%20%2F%20%E6%96%87");
    await api.terminateComputingUnit(1);
    expect(fetch.mock.calls.at(-1)?.[0]).toBe("/api/computing-unit/1/terminate");
  });

  it("gives malformed/null computing resources the existing placeholder without mutating the response", () => {
    for (const value of [null, "null", "{"]) {
      const raw = { computingUnit: { cuid: 1, name: "unit", resource: value } };
      const parsed = parseComputingUnit(raw as never);
      expect(parsed.computingUnit.resource).toEqual({
        cpuLimit: "NaN",
        memoryLimit: "NaN",
        gpuLimit: "NaN",
        jvmMemorySize: "NaN",
        shmSize: "NaN",
        nodeAddresses: [],
      });
      expect(raw.computingUnit.resource).toBe(value);
    }
  });

  it("preserves limits/types/capacity and curated image administration contracts", async () => {
    const { api, fetch } = setup();
    await api.computingUnitLimits();
    await api.computingUnitTypes();
    await api.clusterCapacity();
    await api.images();
    await api.createImage("Image", "registry/image:tag");
    await api.refreshImage(1);
    await api.imageLog(1);
    await api.deleteImage(1);
    expect(fetch.mock.calls.map(([url]) => url)).toEqual([
      "/api/computing-unit/limits",
      "/api/computing-unit/types",
      "/api/computing-unit/admin/cluster-capacity",
      "/api/cu-image",
      "/api/cu-image",
      "/api/cu-image/1/refresh",
      "/api/cu-image/1/log",
      "/api/cu-image/1",
    ]);
    expect(JSON.parse(String(fetch.mock.calls[4][1]?.body))).toEqual({
      name: "Image",
      sourceRef: "registry/image:tag",
    });
    expect(fetch.mock.calls[7][1]?.method).toBe("DELETE");
  });

  it("rejects unsafe identifiers and propagates API failures unchanged", async () => {
    const { api, fetch } = setup(() => new Response("Denied", { status: 403 }));
    for (const id of [-1, NaN, Infinity, 1.5]) await expect(api.deleteWarehouse(id)).rejects.toThrow("identifier");
    expect(fetch).not.toHaveBeenCalled();
    await expect(api.warehouseStatus()).rejects.toThrow("Denied");
  });
});
