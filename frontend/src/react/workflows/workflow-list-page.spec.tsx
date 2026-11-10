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
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { ApiClient } from "../core/api-client";
import { WorkflowListPage } from "./workflow-list-page";
afterEach(cleanup);
const workflow = {
  wid: 4,
  name: "Example workflow",
  description: "Analysis",
  content: {
    operators: [],
    links: [],
    commentBoxes: [],
    operatorPositions: {},
    settings: { dataTransferBatchSize: 400, executionMode: "PIPELINED" },
  },
  defaultView: "FORM",
  isPublished: 0,
};
const user = { uid: 7, name: "tester", email: "test@example.org", role: "REGULAR" as const, expiresAt: 9999999999 };
function setup(
  accessLevel = "WRITE",
  isOwner = true,
  empty = false,
  notebookEnabled = false,
  executionTracking = false
) {
  const fetch = vi.fn(async (path: string, _init?: RequestInit) => {
    if (path.startsWith("/api/dashboard/search"))
      return Response.json({
        results: empty
          ? []
          : [
              {
                resourceType: "workflow",
                workflow: { workflow, ownerId: 7, ownerName: "tester", accessLevel, isOwner },
              },
            ],
        more: false,
      });
    if (path === "/api/workflow/create")
      return Response.json({ workflow: { ...workflow, wid: 9 }, isOwner: true, ownerId: 7, accessLevel: "WRITE" });
    if (path === "/api/workflow/update/name") return Response.json({});
    if (path === "/api/models") return Response.json({ data: [{ id: "test-model" }] });
    if (path === "/api/executions/4") return Response.json([]);
    if (path.startsWith("/api/hub/counts"))
      return Response.json([{ entityId: 4, counts: { view: 3, clone: 2, like: 9 } }]);
    if (path.startsWith("/api/hub/isLiked") || path.startsWith("/api/hub/user-access")) return Response.json([]);
    if (path === "/api/hub/like") return Response.json(true);
    if (path.includes("/access/workflow/list/")) return Response.json([]);
    if (path.includes("/access/workflow/owner/")) return new Response("test@example.org");
    if (path.includes("/workflow/type/")) return new Response("Private");
    return Response.json({});
  });
  const onOpen = vi.fn();
  render(
    <WorkflowListPage
      api={new ApiClient({ fetch })}
      user={user}
      config={{
        formViewEnabled: true,
        defaultDataTransferBatchSize: 400,
        defaultExecutionMode: "PIPELINED",
        pythonNotebookMigrationEnabled: notebookEnabled,
        workflowExecutionsTrackingEnabled: executionTracking,
      }}
      onOpen={onOpen}
    />
  );
  return { fetch, onOpen };
}
describe("WorkflowListPage", () => {
  it("opens execution history only when enabled and keeps reader history read-only", async () => {
    setup("READ", false, false, false, true);
    fireEvent.click(await screen.findByRole("button", { name: "Executions of Example workflow" }));
    await screen.findByRole("dialog", { name: "Execution history" });
    await screen.findByText("No executions found.");
    expect(screen.queryByRole("button", { name: "Delete selected" })).toBeNull();
  });
  it("opens a saved workflow in its preferred view and creates an actual workflow", async () => {
    const { fetch, onOpen } = setup();
    fireEvent.click(await screen.findByRole("button", { name: "Open Example workflow" }));
    expect(onOpen).toHaveBeenCalledWith(4, "FORM");
    fireEvent.click(screen.getByRole("button", { name: "Create workflow" }));
    await waitFor(() => expect(onOpen).toHaveBeenCalledWith(9, "CANVAS"));
    expect(fetch.mock.calls.some(([path]) => path === "/api/workflow/create")).toBe(true);
  });
  it("sends search and sort to the backend and presents a real empty state", async () => {
    const { fetch } = setup("WRITE", true, true);
    expect(await screen.findByText("No workflows match your search.")).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Search workflows"), { target: { value: "日本語" } });
    fireEvent.change(screen.getByLabelText("Sort workflows"), { target: { value: "NameAsc" } });
    fireEvent.click(screen.getByRole("button", { name: "Search", exact: true }));
    await waitFor(() =>
      expect(fetch.mock.calls.some(([path]) => path.includes("query=") && path.includes("orderBy=NameAsc"))).toBe(true)
    );
  });
  it("permits readers to open/export/share-view while withholding editing and deletion", async () => {
    setup("READ", false);
    await screen.findByRole("button", { name: "Open Example workflow" });
    expect(screen.queryByRole("button", { name: "Edit Example workflow" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Delete Example workflow" })).toBeNull();
    expect(screen.getByRole("button", { name: "Export Example workflow" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Share Example workflow" })).toBeTruthy();
  });
  it("edits metadata and requires explicit confirmation before deletion", async () => {
    const { fetch } = setup();
    fireEvent.click(await screen.findByRole("button", { name: "Edit Example workflow" }));
    fireEvent.change(screen.getByLabelText("Workflow name"), { target: { value: "Renamed" } });
    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));
    await waitFor(() => expect(fetch.mock.calls.some(([path]) => path === "/api/workflow/update/name")).toBe(true));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    fireEvent.click(screen.getByRole("button", { name: "Delete Example workflow" }));
    expect(fetch.mock.calls.some(([path]) => path === "/api/workflow/delete")).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Confirm delete" }));
    await waitFor(() => expect(fetch.mock.calls.some(([path]) => path === "/api/workflow/delete")).toBe(true));
  });

  it("shows server activity and applies the confirmed like count", async () => {
    const { fetch } = setup();
    fireEvent.click(await screen.findByRole("button", { name: "Like Example workflow" }));
    await waitFor(() => expect(fetch.mock.calls.some(([path]) => path === "/api/hub/like")).toBe(true));
    expect(await screen.findByRole("button", { name: "Unlike Example workflow" })).toBeTruthy();
    expect(screen.getByText("9 likes")).toBeTruthy();
  });

  it("allows an owner to upload a cover image through the existing workflow endpoint", async () => {
    const { fetch } = setup();
    fireEvent.click(await screen.findByRole("button", { name: "Edit Example workflow" }));
    fireEvent.change(screen.getByLabelText("Cover image"), {
      target: { files: [new File(["image"], "cover.png", { type: "image/png" })] },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));
    await waitFor(() => expect(fetch.mock.calls.some(([path]) => path === "/api/workflow/4/cover")).toBe(true));
    const save = fetch.mock.calls.find(([path]) => path === "/api/workflow/4/cover");
    expect(JSON.parse(String(save?.[1]?.body)).image).toMatch(/^data:image\/png;base64,/);
  });

  it("offers notebook generation only when enabled and rejects unsupported files before model requests", async () => {
    setup();
    expect(screen.queryByRole("button", { name: "Generate from Python" })).toBeNull();
    await screen.findByRole("button", { name: "Open Example workflow" });
    cleanup();
    const { fetch } = setup("WRITE", true, false, true);
    fireEvent.click(screen.getByRole("button", { name: "Generate from Python" }));
    expect(await screen.findByRole("dialog", { name: "Generate a workflow from Python" })).toBeTruthy();
    await waitFor(() => expect(screen.getByLabelText("Generation model").textContent).toContain("test-model"));
    fireEvent.change(screen.getByLabelText("Notebook or Python file"), {
      target: { files: [new File(["x"], "bad.txt")] },
    });
    fireEvent.click(screen.getByRole("button", { name: "Generate workflow", exact: true }));
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain(".ipynb"));
    expect(fetch.mock.calls.some(([path]) => path === "/api/workflow/create" || path.includes("chat/"))).toBe(false);
  });
});
