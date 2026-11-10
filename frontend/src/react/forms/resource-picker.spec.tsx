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

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiClient } from "../core/api-client";
import { SchemaForm } from "./schema-form";
import { createResourceCatalog, type ResourceCatalog } from "./resource-catalog";

afterEach(cleanup);

function catalog(): ResourceCatalog {
  return {
    list: vi.fn(async () => [
      { id: 7, name: "測定", ownerEmail: "owner@example.org", accessPrivilege: "READ", isOwner: false },
    ]),
    versions: vi.fn(async () => [
      { id: 12, name: "v2" },
      { id: 11, name: "v1" },
    ]),
    files: vi.fn(async () => [
      { name: "data.csv", type: "file" as const, parentDir: "/dataset/owner@example.org/測定/v2" },
    ]),
  };
}

describe("resource picker transport", () => {
  it("adapts exact dataset/model API shapes and passes cancellation through every request", async () => {
    const fetch = vi.fn(async (url: string) =>
      Response.json(
        url.endsWith("dataset/list")
          ? [{ dataset: { did: 0, name: "d" }, ownerEmail: "o", isOwner: true, accessPrivilege: "WRITE" }]
          : url.endsWith("model/list")
            ? [{ model: { mid: 2, name: "m" }, ownerEmail: "o", isOwner: false, accessPrivilege: "READ" }]
            : url.endsWith("rootFileNodes")
              ? { fileNodes: [], size: 0 }
              : url.includes("dataset/")
                ? [{ dvid: 4, name: "v" }]
                : [{ mvid: 5, name: "v" }]
      )
    );
    const service = createResourceCatalog(new ApiClient({ fetch }));
    const signal = new AbortController().signal;
    expect(await service.list("dataset", signal)).toEqual([
      { id: 0, name: "d", ownerEmail: "o", isOwner: true, accessPrivilege: "WRITE" },
    ]);
    expect(await service.list("model", signal)).toEqual([
      { id: 2, name: "m", ownerEmail: "o", isOwner: false, accessPrivilege: "READ" },
    ]);
    expect(await service.versions("dataset", 0, signal)).toEqual([{ id: 4, name: "v" }]);
    expect(await service.versions("model", 2, signal)).toEqual([{ id: 5, name: "v" }]);
    await service.files("dataset", 0, 4, signal);
    expect(fetch.mock.calls.map(call => call[0])).toEqual([
      "/api/dataset/list",
      "/api/model/list",
      "/api/dataset/0/version/list",
      "/api/model/2/version/list",
      "/api/dataset/0/version/4/rootFileNodes",
    ]);
    expect(fetch.mock.calls.every(call => call[1].signal === signal)).toBe(true);
    await expect(service.versions("dataset", -1, signal)).rejects.toThrow("Invalid resource id");
  });

  it("rejects malformed server rows instead of emitting undefined paths", async () => {
    const service = createResourceCatalog(
      new ApiClient({ fetch: vi.fn(async () => Response.json([{ dataset: { did: 2 } }])) })
    );
    await expect(service.list("dataset")).rejects.toThrow("Invalid dataset catalog response");
  });
});

describe("operator resource pickers", () => {
  it("keeps keyboard focus in the open browser and restores it on Escape", async () => {
    render(
      <SchemaForm
        schema={{ type: "object", properties: { datasetVersionPath: { type: "string", title: "Dataset version" } } }}
        value={{}}
        context={{ resourceCatalog: catalog() }}
        onChange={vi.fn()}
      />
    );
    const browse = screen.getByRole("button", { name: "Browse Dataset version" });
    browse.focus();
    fireEvent.click(browse);
    const dialog = await screen.findByRole("dialog");
    fireEvent.keyDown(dialog, { key: "Tab", shiftKey: true });
    expect(document.activeElement).toBe(screen.getByRole("button", { name: "Cancel" }));
    fireEvent.keyDown(dialog, { key: "Escape" });
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(document.activeElement).toBe(browse);
  });

  it("picks a dataset version and retains the server's logical path convention", async () => {
    const onChange = vi.fn();
    const service = catalog();
    render(
      <SchemaForm
        schema={{ type: "object", properties: { datasetVersionPath: { type: "string", title: "Dataset version" } } }}
        value={{ datasetVersionPath: "" }}
        context={{ resourceCatalog: service }}
        onChange={onChange}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "Browse Dataset version" }));
    await screen.findByRole("option", { name: /#7 測定/ });
    fireEvent.change(screen.getByLabelText("Dataset"), { target: { value: "7" } });
    await screen.findByRole("option", { name: "v2" });
    expect((screen.getByRole("button", { name: "Select", exact: true }) as HTMLButtonElement).disabled).toBe(true);
    fireEvent.change(screen.getByLabelText("Version"), { target: { value: "12" } });
    fireEvent.click(screen.getByRole("button", { name: "Select", exact: true }));
    expect(onChange.mock.lastCall?.[0]).toEqual({ datasetVersionPath: "/dataset/owner@example.org/測定/v2" });
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("preselects legacy dataset paths and chooses files without rewriting the file-node path", async () => {
    const service = catalog();
    const onChange = vi.fn();
    render(
      <SchemaForm
        schema={{ type: "object", properties: { fileName: { type: "string", title: "File" } } }}
        value={{ fileName: "/owner@example.org/測定/v2/old.csv" }}
        context={{ resourceCatalog: service, selectingFilesFromDatasetsEnabled: true }}
        onChange={onChange}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "Browse File" }));
    fireEvent.click(await screen.findByRole("button", { name: "data.csv" }));
    fireEvent.click(screen.getByRole("button", { name: "Select", exact: true }));
    expect(onChange.mock.lastCall?.[0]).toEqual({ fileName: "/dataset/owner@example.org/測定/v2/data.csv" });
  });

  it("leaves manual file paths editable while selection is disabled by runtime config", () => {
    const onChange = vi.fn();
    render(
      <SchemaForm
        schema={{ type: "object", properties: { fileName: { type: "string", title: "File" } } }}
        value={{ fileName: "" }}
        context={{ resourceCatalog: catalog(), selectingFilesFromDatasetsEnabled: false }}
        onChange={onChange}
      />
    );
    expect(screen.queryByRole("button", { name: "Browse File" })).toBeNull();
    fireEvent.change(screen.getByLabelText("File"), { target: { value: "https://example.org/data.csv" } });
    expect(onChange.mock.lastCall?.[0]).toEqual({ fileName: "https://example.org/data.csv" });
  });

  it("uses the model browser for a resource parameter and cancel preserves its value", async () => {
    const onChange = vi.fn();
    const service = catalog();
    render(
      <SchemaForm
        schema={{
          type: "object",
          properties: {
            model: {
              type: "string",
              title: "Model",
              widget: { formlyConfig: { type: "resourcevalue", props: { resource: "model" } } },
            },
          },
        }}
        value={{ model: "/model/owner@example.org/測定/v1" }}
        context={{ resourceCatalog: service }}
        onChange={onChange}
      />
    );
    expect(screen.getByRole("button", { name: "測定 · v1" }).getAttribute("title")).toBe(
      "/model/owner@example.org/測定/v1"
    );
    fireEvent.click(screen.getByRole("button", { name: "測定 · v1" }));
    await screen.findByRole("option", { name: /#7 測定/ });
    expect(service.list).toHaveBeenCalledWith("model", expect.any(AbortSignal));
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(onChange).not.toHaveBeenCalled();
  });

  it("reports catalog errors, allows retry, and disables confirmation for an empty catalog", async () => {
    const service = catalog();
    vi.mocked(service.list).mockRejectedValueOnce(new Error("Permission denied")).mockResolvedValue([]);
    render(
      <SchemaForm
        schema={{ type: "object", properties: { datasetVersionPath: { type: "string", title: "Dataset version" } } }}
        value={{}}
        context={{ resourceCatalog: service }}
        onChange={vi.fn()}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "Browse Dataset version" }));
    expect((await screen.findByRole("alert")).textContent).toContain("Permission denied");
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await screen.findByText("No accessible datasets.");
    expect((screen.getByRole("button", { name: "Select", exact: true }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("ignores old version requests when resource selection changes and aborts them on close", async () => {
    const service = catalog();
    vi.mocked(service.list).mockResolvedValue([
      { id: 7, name: "first", ownerEmail: "o" },
      { id: 8, name: "second", ownerEmail: "o" },
    ]);
    let resolveOld!: (value: { id: number; name: string }[]) => void;
    vi.mocked(service.versions).mockImplementation(async (_kind, id) =>
      id === 7
        ? new Promise(resolve => {
            resolveOld = resolve;
          })
        : [{ id: 80, name: "current" }]
    );
    render(
      <SchemaForm
        schema={{ type: "object", properties: { datasetVersionPath: { type: "string", title: "Version" } } }}
        value={{}}
        context={{ resourceCatalog: service }}
        onChange={vi.fn()}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "Browse Version" }));
    await screen.findByRole("option", { name: /#7 first/ });
    fireEvent.change(screen.getByLabelText("Dataset"), { target: { value: "7" } });
    fireEvent.change(screen.getByLabelText("Dataset"), { target: { value: "8" } });
    await screen.findByRole("option", { name: "current" });
    await act(async () => resolveOld([{ id: 70, name: "stale" }]));
    expect(screen.queryByRole("option", { name: "stale" })).toBeNull();
    expect(vi.mocked(service.versions).mock.calls[0][2]?.aborted).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(vi.mocked(service.list).mock.calls[0][1]?.aborted).toBe(true));
  });
});
