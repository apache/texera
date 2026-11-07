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
import { HubSearchPage, HubWorkflowDetailPage } from "./hub-pages";
afterEach(cleanup);
const workflow = {
  wid: 4,
  name: "Public analysis",
  description: "Real published workflow",
  content: {
    operators: [],
    links: [],
    commentBoxes: [],
    operatorPositions: {},
    settings: { dataTransferBatchSize: 400, executionMode: "PIPELINED" },
  },
  defaultView: "FORM",
};
function setup() {
  const fetch = vi.fn(async (path: string) => {
    if (path.includes("/dashboard/publicSearch"))
      return Response.json({
        results: [
          {
            resourceType: "workflow",
            workflow: { workflow, ownerId: 7, ownerName: "Ada", accessLevel: "READ", isOwner: false },
          },
        ],
        more: false,
      });
    if (path.includes("/owner_name")) return new Response("Ada");
    if (path.includes("/workflow/publicised/")) return Response.json(workflow);
    if (path.includes("/workflow/clone/")) return Response.json(8);
    if (path.includes("/hub/view")) return Response.json(11);
    if (path.includes("/hub/counts"))
      return Response.json([{ entityId: 4, entityType: "workflow", counts: { like: 3, clone: 2, view: 11 } }]);
    if (path.includes("/hub/isLiked") || path.includes("/hub/user-access")) return Response.json([]);
    return Response.json({});
  });
  return { api: new ApiClient({ fetch }), fetch };
}
describe("Hub React pages", () => {
  it("applies a URL-owned query once, reports submissions, and clears external query changes", async () => {
    const { api, fetch } = setup(),
      onQuerySubmit = vi.fn();
    const view = render(
      <HubSearchPage
        api={api}
        query="日本語"
        onQuerySubmit={onQuerySubmit}
        onOpen={vi.fn()}
      />
    );
    await screen.findByRole("button", { name: "Open Public analysis" });
    expect((screen.getByLabelText("Search public resources") as HTMLInputElement).value).toBe("日本語");
    const searches = () => fetch.mock.calls.filter(([path]) => path.includes("/dashboard/publicSearch"));
    expect(searches()).toHaveLength(1);
    expect(new URL(searches()[0][0], "https://texera.test").searchParams.getAll("query")).toEqual(["日本語"]);
    fireEvent.change(screen.getByLabelText("Search public resources"), { target: { value: "next" } });
    fireEvent.submit(screen.getByLabelText("Search public resources").closest("form")!);
    expect(onQuerySubmit).toHaveBeenCalledWith("next");
    view.rerender(
      <HubSearchPage
        api={api}
        query=""
        onQuerySubmit={onQuerySubmit}
        onOpen={vi.fn()}
      />
    );
    await waitFor(() => expect((screen.getByLabelText("Search public resources") as HTMLInputElement).value).toBe(""));
    await waitFor(() =>
      expect(new URL(searches().at(-1)![0], "https://texera.test").searchParams.has("query")).toBe(false)
    );
  });
  it("renders public search from the backend, opens details and searches unicode keywords", async () => {
    const { api, fetch } = setup();
    const onOpen = vi.fn();
    render(
      <HubSearchPage
        api={api}
        kind="workflow"
        onOpen={onOpen}
      />
    );
    fireEvent.click(await screen.findByRole("button", { name: "Open Public analysis" }));
    expect(onOpen).toHaveBeenCalledWith("workflow", 4);
    fireEvent.change(screen.getByLabelText("Search public resources"), { target: { value: "日本語" } });
    fireEvent.click(screen.getByRole("button", { name: "Search", exact: true }));
    await waitFor(() => expect(fetch.mock.calls.some(([path]) => path.includes("query=%E6%97%A5"))).toBe(true));
  });
  it("allows anonymous workflow previews while withholding clone and like mutations", async () => {
    const { api } = setup();
    render(
      <HubWorkflowDetailPage
        api={api}
        wid={4}
        onClone={vi.fn()}
        onBack={vi.fn()}
      />
    );
    expect(await screen.findByRole("heading", { name: "Public analysis" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Clone workflow" }).hasAttribute("disabled")).toBe(true);
    expect(screen.getByRole("button", { name: "Like workflow" }).hasAttribute("disabled")).toBe(true);
    expect(screen.getByLabelText("Workflow preview")).toBeTruthy();
  });
  it("clones an activated user's public workflow through the real endpoint", async () => {
    const { api, fetch } = setup();
    const onClone = vi.fn();
    render(
      <HubWorkflowDetailPage
        api={api}
        wid={4}
        user={{ uid: 7, name: "Ada", email: "ada@example.org", role: "REGULAR", expiresAt: 99999999 }}
        onClone={onClone}
        onBack={vi.fn()}
      />
    );
    fireEvent.click(await screen.findByRole("button", { name: "Clone workflow" }));
    await waitFor(() => expect(onClone).toHaveBeenCalledWith(8, "FORM"));
    expect(fetch.mock.calls.some(([path]) => path === "/api/workflow/clone/4")).toBe(true);
  });

  it("refuses invalid workflow route IDs before making requests", async () => {
    const { api, fetch } = setup();
    render(
      <HubWorkflowDetailPage
        api={api}
        wid={NaN}
        onClone={vi.fn()}
        onBack={vi.fn()}
      />
    );
    expect((await screen.findByRole("alert")).textContent).toContain("valid workflow ID");
    expect(fetch).not.toHaveBeenCalled();
  });

  it("shows a failed public search instead of a fabricated empty result", async () => {
    const fetch = vi.fn(async () => new Response("Search unavailable", { status: 503 }));
    render(
      <HubSearchPage
        api={new ApiClient({ fetch })}
        kind="dataset"
        onOpen={vi.fn()}
      />
    );
    expect((await screen.findByRole("alert")).textContent).toContain("Search unavailable");
    expect(screen.queryByText("No public resources match your search.")).toBeNull();
  });
});
