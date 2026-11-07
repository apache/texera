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
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { ApiClient } from "../core/api-client";
import { ResourceApi } from "./api";
import { ComputingImagesPage, ComputingUnitsPage, jvmMemoryChoices, resourcePercentage } from "./computing";
afterEach(cleanup);
const change = (label: string, value: string) => fireEvent.change(screen.getByLabelText(label), { target: { value } });
const unit = {
  computingUnit: {
    cuid: 1,
    uid: 7,
    name: "Unit",
    type: "kubernetes",
    uri: "ws://unit",
    creationTime: 0,
    resource: {
      cpuLimit: "2",
      memoryLimit: "4Gi",
      gpuLimit: "0",
      jvmMemorySize: "2G",
      shmSize: "64Mi",
      nodeAddresses: ["node"],
    },
  },
  status: "Running",
  metrics: { cpuUsage: "500m", memoryUsage: "1Gi" },
  isOwner: true,
  accessPrivilege: "WRITE",
  ownerName: "Owner",
  ownerAvatar: "",
};
const image = {
  iid: 1,
  name: "Ready",
  sourceRef: "registry/image:tag",
  sourceDigest: "sha256:abc",
  imageTag: "registry/image@sha256:abc",
  status: "READY",
  attempt: 1,
  creationTime: 0,
  updateTime: 0,
};
function setup(responder?: (url: string, init: RequestInit) => Response | Promise<Response>) {
  const fetch = vi.fn((url: string, init: RequestInit = {}) =>
    responder
      ? responder(url, init)
      : Response.json(
          url.endsWith("/types")
            ? { typeOptions: ["kubernetes", "local"] }
            : url.endsWith("/limits")
              ? { cpuLimitOptions: ["1", "2"], memoryLimitOptions: ["1Gi", "4Gi", "8Gi"], gpuLimitOptions: ["0", "1"] }
              : url.includes("cu-image")
                ? [image, { ...image, iid: 2, name: "Failed", status: "FAILED" }]
                : [unit]
        )
  );
  return { fetch, resources: new ResourceApi(new ApiClient({ fetch })) };
}

describe("computing units", () => {
  it("retries an initial failed admin poll without requiring a manual refresh", async () => {
    let attempts = 0;
    const test = setup(url =>
      url.endsWith("cluster-capacity")
        ? new Response("Not Kubernetes", { status: 404 })
        : ++attempts === 1
          ? new Response("Offline", { status: 503 })
          : Response.json([unit])
    );
    render(
      <ComputingUnitsPage
        resources={test.resources}
        admin
        refreshMs={20}
      />
    );
    await screen.findByRole("button", { name: "Computing unit details Unit" });
    expect(attempts).toBeGreaterThan(1);
  });
  it("preserves CPU/binary-memory percentages and JVM step defaults", () => {
    expect(resourcePercentage("500m", "2", "cpu")).toBe(25);
    expect(resourcePercentage("1Gi", "4Gi", "memory")).toBe(25);
    expect(resourcePercentage("bad", "NaN", "memory")).toBe(0);
    expect(resourcePercentage("8Gi", "4Gi", "memory")).toBe(100);
    expect(jvmMemoryChoices("1Gi")).toEqual({ choices: [1], selected: 1 });
    expect(jvmMemoryChoices("8Gi")).toEqual({ choices: [2, 4, 8], selected: 2 });
    expect(jvmMemoryChoices("2048Mi")).toEqual({ choices: [1, 2], selected: 2 });
  });

  it("creates a Kubernetes unit from server options, curated ready images and advanced memory controls", async () => {
    const test = setup((url, init) => (init.method === "POST" ? Response.json(unit) : setupResponse(url)));
    render(
      <ComputingUnitsPage
        resources={test.resources}
        refreshMs={60000}
      />
    );
    await screen.findByRole("button", { name: "Computing unit details Unit" });
    fireEvent.click(screen.getByRole("button", { name: "Create computing unit" }));
    await screen.findByLabelText("CPU limit");
    expect(within(screen.getByLabelText("Container image")).queryByRole("option", { name: "Failed" })).toBeNull();
    change("Computing unit name", "Pod");
    change("Memory limit", "8Gi");
    change("CPU limit", "2");
    change("Container image", "1");
    fireEvent.click(screen.getByText("Advanced settings"));
    change("JVM memory (GiB)", "4");
    change("Shared memory size", "128");
    fireEvent.click(screen.getByRole("button", { name: "Create unit" }));
    await screen.findByText("Computing unit created.");
    const body = JSON.parse(String(test.fetch.mock.calls.find(([, init]) => init.method === "POST")?.[1].body));
    expect(body).toEqual({
      name: "Pod",
      cpuLimit: "2",
      memoryLimit: "8Gi",
      gpuLimit: "0",
      jvmMemorySize: "4G",
      shmSize: "128Mi",
      uri: "",
      unitType: "kubernetes",
      iid: 1,
    });
  });

  it("validates local URI/shared memory, resets creation choices, and omits default image id", async () => {
    const test = setup((url, init) => (init.method ? Response.json(unit) : setupResponse(url)));
    render(
      <ComputingUnitsPage
        resources={test.resources}
        refreshMs={60000}
      />
    );
    await screen.findByRole("button", { name: "Create computing unit" });
    fireEvent.click(screen.getByRole("button", { name: "Create computing unit" }));
    await screen.findByLabelText("CPU limit");
    change("Computing unit type", "local");
    change("Local computing unit URI", "");
    fireEvent.click(screen.getByRole("button", { name: "Create unit" }));
    expect(screen.getByRole("alert").textContent).toContain("URI");
    change("Computing unit type", "kubernetes");
    fireEvent.click(screen.getByText("Advanced settings"));
    change("Shared memory size", "2000");
    fireEvent.click(screen.getByRole("button", { name: "Create unit" }));
    expect(screen.getByRole("alert").textContent).toContain("Shared memory");
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    fireEvent.click(screen.getByRole("button", { name: "Create computing unit" }));
    await screen.findByLabelText("CPU limit");
    fireEvent.click(screen.getByRole("button", { name: "Create unit" }));
    await screen.findByText("Computing unit created.");
    expect(
      JSON.parse(String(test.fetch.mock.calls.find(([, init]) => init.method === "POST")?.[1].body)).iid
    ).toBeUndefined();
  });

  it("renames owned units, shares through the adapter, and closes selected sockets before termination", async () => {
    const beforeTerminate = vi.fn().mockResolvedValue(undefined);
    const onShare = vi.fn();
    const test = setup((url, init) => (init.method ? new Response(null, { status: 204 }) : setupResponse(url)));
    render(
      <ComputingUnitsPage
        resources={test.resources}
        onShare={onShare}
        sharingEnabled
        beforeTerminate={beforeTerminate}
        refreshMs={60000}
      />
    );
    await screen.findByRole("button", { name: "Rename computing unit Unit" });
    fireEvent.click(screen.getByRole("button", { name: "Rename computing unit Unit" }));
    change("New computing unit name", "Renamed");
    fireEvent.click(screen.getByRole("button", { name: "Save name" }));
    await screen.findByRole("button", { name: "Computing unit details Renamed" });
    fireEvent.click(screen.getByRole("button", { name: "Share computing unit Renamed" }));
    expect(onShare).toHaveBeenCalledWith(1);
    fireEvent.click(screen.getByRole("button", { name: "Terminate computing unit Renamed" }));
    const dialog = screen.getByRole("dialog");
    expect(dialog.textContent).toContain("execution results");
    fireEvent.click(within(dialog).getByRole("button", { name: "Terminate" }));
    await waitFor(() => expect(test.fetch.mock.calls.some(([url]) => url.endsWith("/1/terminate"))).toBe(true));
    expect(beforeTerminate).toHaveBeenCalledWith(1);
    expect(screen.queryByRole("button", { name: "Computing unit details Renamed" })).toBeNull();
  });

  it("renders admin capacity, filters rows and preserves the last good snapshot during polling failure", async () => {
    let failed = false;
    const test = setup(url =>
      url.endsWith("cluster-capacity")
        ? Response.json({
            cpu: { total: 2000, allocated: 1000, available: 1000 },
            memory: { total: 1024, allocated: 512, available: 512 },
            gpu: { total: 0, allocated: 0, available: 0 },
          })
        : failed
          ? new Response("Offline", { status: 503 })
          : Response.json([unit])
    );
    const rendered = render(
      <ComputingUnitsPage
        resources={test.resources}
        admin
        refreshMs={20}
      />
    );
    await screen.findByText("Cluster capacity");
    await screen.findByRole("button", { name: "Computing unit details Unit" });
    change("Computing unit status filter", "Failed");
    expect(screen.queryByRole("button", { name: "Computing unit details Unit" })).toBeNull();
    change("Computing unit status filter", "");
    failed = true;
    await screen.findByText(/last successful snapshot/i);
    expect(screen.getByRole("button", { name: "Computing unit details Unit" })).toBeDefined();
    rendered.unmount();
    const count = test.fetch.mock.calls.length;
    await new Promise(resolve => setTimeout(resolve, 50));
    expect(test.fetch).toHaveBeenCalledTimes(count);
  });
});

function setupResponse(url: string): Response {
  return Response.json(
    url.endsWith("/types")
      ? { typeOptions: ["kubernetes", "local"] }
      : url.endsWith("/limits")
        ? { cpuLimitOptions: ["1", "2"], memoryLimitOptions: ["1Gi", "4Gi", "8Gi"], gpuLimitOptions: ["0", "1"] }
        : url.includes("cu-image")
          ? [image, { ...image, iid: 2, name: "Failed", status: "FAILED" }]
          : [unit]
  );
}

describe("computing images", () => {
  it("distinguishes a disabled image service, and registers/refreshes/logs/deletes real images", async () => {
    let rows = [image];
    const test = setup((url, init) => {
      if (url.endsWith("/log"))
        return Response.json({ iid: 1, status: "READY", attempt: 1, log: "<script>log</script>" });
      if (init.method === "DELETE") {
        rows = [];
        return new Response(null, { status: 204 });
      }
      if (init.method === "POST") return Response.json(image);
      return Response.json(rows);
    });
    const rendered = render(
      <ComputingImagesPage
        resources={test.resources}
        refreshMs={60000}
      />
    );
    await screen.findByText("registry/image:tag");
    change("Image name", "New");
    change("Image reference", "repo:new");
    fireEvent.click(screen.getByRole("button", { name: "Register image" }));
    await screen.findByText("Image registered.");
    expect(JSON.parse(String(test.fetch.mock.calls.find(([, init]) => init.method === "POST")?.[1].body))).toEqual({
      name: "New",
      sourceRef: "repo:new",
    });
    fireEvent.click(screen.getByRole("button", { name: "Validation log for Ready" }));
    await screen.findByText("<script>log</script>");
    expect(rendered.container.querySelector("script")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Close log" }));
    fireEvent.click(screen.getByRole("button", { name: "Refresh image Ready" }));
    await waitFor(() => expect(test.fetch.mock.calls.some(([url]) => url.endsWith("/1/refresh"))).toBe(true));
    fireEvent.click(screen.getByRole("button", { name: "Delete image Ready" }));
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Delete" }));
    await screen.findByText("No curated images.");
  });
  it("shows feature-disabled on 503 and recovers from other errors with retry", async () => {
    let status = 403;
    const test = setup(() => new Response("Unavailable", { status }));
    render(<ComputingImagesPage resources={test.resources} />);
    await screen.findByRole("alert");
    status = 503;
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await screen.findByText(/Curated images are disabled/);
    expect(screen.queryByRole("button", { name: "Register image" })).toBeNull();
  });
});
