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

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, useLocation, useNavigate } from "react-router-dom";
import { ApplicationRoutes } from "./application";
import { ApiClient } from "./core/api-client";
import { AuthStore } from "./core/auth";
import { RuntimeConfig } from "./core/runtime-config";
vi.mock("./workspace/workspace-page", () => ({
  WorkspacePage: ({
    workflowId,
    versionId,
    view,
    onView,
  }: {
    workflowId: number;
    versionId?: number;
    view: string;
    onView: (view: "canvas" | "form") => void;
  }) => (
    <section>
      <h1>Workflow {workflowId}</h1>
      <output aria-label="Workspace view">{view}</output>
      <output aria-label="Workspace version">{versionId}</output>
      <button onClick={() => onView(view === "form" ? "canvas" : "form")}>Switch workspace view</button>
    </section>
  ),
}));

const stores: AuthStore[] = [];
afterEach(() => {
  cleanup();
  stores.splice(0).forEach(auth => auth.dispose());
  localStorage.clear();
});
function Location() {
  const location = useLocation(),
    navigate = useNavigate();
  return (
    <>
      <output aria-label="Route">{location.pathname + location.search}</output>
      <button onClick={() => navigate(-1)}>Previous route</button>
      <button onClick={() => navigate(1)}>Next route</button>
    </>
  );
}
async function setup(path: string, role?: "REGULAR" | "ADMIN") {
  localStorage.clear();
  if (role)
    localStorage.setItem(
      "access_token",
      `header.${btoa(JSON.stringify({ userId: 7, sub: "Tester", email: "test@example.org", exp: 9999999999, role }))}.signature`
    );
  const fetch = vi.fn(async (url: string) => {
    if (url.endsWith("pre-login")) return Response.json({ localLogin: true });
    if (url.endsWith("settings/public"))
      return Response.json({ workflows_enabled: "true", your_work_enabled: "true", about_enabled: "true" });
    if (/dashboard\/(?:publicSearch|search)/.test(url)) return Response.json({ results: [], more: false });
    if (url.endsWith("warehouse/status")) return Response.json({ enabled: true, warehouses: [] });
    if (url.includes("totalWorkflow")) return Response.json(0);
    if (url.includes("cluster-capacity"))
      return Response.json({
        cpu: { total: 0, allocated: 0, available: 0 },
        memory: { total: 0, allocated: 0, available: 0 },
        gpu: { total: 0, allocated: 0, available: 0 },
      });
    const resource = url.match(/\/(dataset|model)\/(?:public\/)?7$/);
    if (resource)
      return Response.json({
        [resource[1]]: {
          [resource[1] === "dataset" ? "did" : "mid"]: 7,
          name: "Example resource",
          description: "Description",
          isPublic: true,
          isDownloadable: true,
        },
        isOwner: false,
        ownerEmail: "owner@example.test",
        accessPrivilege: "READ",
        size: 0,
      });
    if (url.endsWith("cover-url")) return Response.json({ url: null });
    if (url.includes("hub/view")) return Response.json(1);
    if (url.includes("gmail/sender/email")) return new Response("admin@example.test");
    if (/config\/(?:gui|amber|user-system)|config\/settings$/.test(url)) return Response.json({});
    return Response.json([]);
  });
  const api = new ApiClient({ fetch });
  const auth = new AuthStore(api, new RuntimeConfig(api));
  stores.push(auth);
  await auth.bootstrap();
  await act(async () => {
    render(
      <MemoryRouter initialEntries={[path]}>
        <ApplicationRoutes
          api={api}
          auth={auth}
        />
        <Location />
      </MemoryRouter>
    );
  });
  return { fetch, auth };
}
describe("React application routes", () => {
  it("loads Unicode search queries from the URL and preserves submissions through back/forward navigation", async () => {
    const initial = "日本語 α";
    const { fetch } = await setup("/search?" + new URLSearchParams({ q: initial }));
    await waitFor(() =>
      expect((screen.getByLabelText("Search public resources") as HTMLInputElement).value).toBe(initial)
    );
    const searches = () =>
      fetch.mock.calls.map(([path]) => path).filter(path => path.includes("/dashboard/publicSearch"));
    await waitFor(() =>
      expect(new URL(searches().at(-1)!, "https://texera.test").searchParams.getAll("query")).toEqual([initial])
    );
    const next = "München & 数据";
    fireEvent.change(screen.getByLabelText("Search public resources"), { target: { value: next } });
    fireEvent.submit(screen.getByLabelText("Search public resources").closest("form")!);
    await waitFor(() =>
      expect(new URLSearchParams(screen.getByLabelText("Route").textContent!.split("?")[1]).get("q")).toBe(next)
    );
    await waitFor(() =>
      expect(new URL(searches().at(-1)!, "https://texera.test").searchParams.getAll("query")).toEqual([next])
    );
    fireEvent.click(screen.getByRole("button", { name: "Previous route" }));
    await waitFor(() =>
      expect((screen.getByLabelText("Search public resources") as HTMLInputElement).value).toBe(initial)
    );
    await waitFor(() =>
      expect(new URL(searches().at(-1)!, "https://texera.test").searchParams.getAll("query")).toEqual([initial])
    );
    fireEvent.click(screen.getByRole("button", { name: "Next route" }));
    await waitFor(() =>
      expect((screen.getByLabelText("Search public resources") as HTMLInputElement).value).toBe(next)
    );
  });
  it("submits the global search input to /search?q and clears an empty query without leaving stale keywords", async () => {
    const { fetch } = await setup("/about");
    const global = await screen.findByRole("searchbox", { name: "Search Texera" });
    fireEvent.change(global, { target: { value: "研究 & science" } });
    fireEvent.submit(global.closest("form")!);
    await waitFor(() =>
      expect(screen.getByLabelText("Route").textContent).toBe("/search?" + new URLSearchParams({ q: "研究 & science" }))
    );
    await waitFor(() =>
      expect((screen.getByLabelText("Search public resources") as HTMLInputElement).value).toBe("研究 & science")
    );
    fireEvent.change(screen.getByRole("searchbox", { name: "Search Texera" }), { target: { value: "" } });
    fireEvent.submit(screen.getByRole("searchbox", { name: "Search Texera" }).closest("form")!);
    await waitFor(() => expect((screen.getByLabelText("Search public resources") as HTMLInputElement).value).toBe(""));
    const searches = () =>
      fetch.mock.calls.map(([path]) => path).filter(path => path.includes("/dashboard/publicSearch"));
    await waitFor(() =>
      expect(new URL(searches().at(-1)!, "https://texera.test").searchParams.getAll("query")).toEqual([])
    );
    expect(screen.getByLabelText("Route").textContent).toBe("/search?q=");
  });
  it.each(["/search", "/search?q="])("treats an incoming empty search at %s as a real unfiltered query", async path => {
    const { fetch } = await setup(path);
    await screen.findByRole("heading", { name: "Search Texera", exact: true });
    await waitFor(() => expect(fetch.mock.calls.some(([url]) => url.includes("/dashboard/publicSearch"))).toBe(true));
    expect((screen.getByLabelText("Search public resources") as HTMLInputElement).value).toBe("");
    for (const [url] of fetch.mock.calls.filter(([url]) => url.includes("/dashboard/publicSearch")))
      expect(new URL(url, "https://texera.test").searchParams.has("query")).toBe(false);
  });
  it("keeps private deep links through the sign-in guard", async () => {
    await setup("/user/workflow/42");
    await screen.findByLabelText("Username");
    expect(screen.getByLabelText("Route").textContent).toBe("/login?returnUrl=%2Fuser%2Fworkflow%2F42");
  });
  it("opens the real workflow list and honors server-provided navigation tabs", async () => {
    await setup("/user/workflow", "REGULAR");
    await screen.findByRole("heading", { name: "Workflows", exact: true });
    await waitFor(() => expect(screen.getByRole("link", { name: "Workflows", exact: true })).toBeTruthy());
    expect(screen.queryByRole("link", { name: "Datasets", exact: true })).toBeNull();
  });
  it("keeps non-admin users out of administration", async () => {
    await setup("/admin/settings", "REGULAR");
    await screen.findByRole("heading", { name: "Workflows", exact: true });
    expect(screen.queryByRole("heading", { name: "General settings" })).toBeNull();
  });
  it("renders the actual settings editor for administrators", async () => {
    await setup("/admin/settings", "ADMIN");
    await screen.findByRole("heading", { name: "General settings" });
    expect(screen.getByLabelText("Route").textContent).toBe("/admin/settings");
  });
  it.each([
    ["/user/dataset", "Datasets"],
    ["/user/model", "Models"],
    ["/user/dataset/create", "Create dataset"],
    ["/user/dataset/7", "Dataset: Example resource"],
    ["/user/model/7", "Model: Example resource"],
    ["/user/compute", "Computing units"],
    ["/user/warehouse", "Warehouses"],
    ["/user/python-venv", "Python environments"],
    ["/user/quota", "Your quota"],
    ["/user/feedback", "Feedback"],
  ])("opens the concrete authenticated page at %s", async (path, heading) => {
    await setup(path, "REGULAR");
    await screen.findByRole("heading", { name: heading, exact: true });
    expect(screen.getByLabelText("Route").textContent).toBe(path);
  });
  it.each([
    ["/admin/user", "Users"],
    ["/admin/gmail", "Email administration"],
    ["/admin/execution", "Executions"],
    ["/admin/compute", "All computing units"],
    ["/admin/cu-image", "Computing unit images"],
  ])("opens the concrete administrator page at %s", async (path, heading) => {
    await setup(path, "ADMIN");
    await screen.findByRole("heading", { name: heading, exact: true });
    expect(screen.getByLabelText("Route").textContent).toBe(path);
  });
  it.each(["dataset", "model"])(
    "opens public %s details for anonymous visitors without private API requests",
    async kind => {
      const { fetch } = await setup(`/hub/${kind}/result/detail/7`);
      await screen.findByRole("heading", { name: `${kind === "dataset" ? "Dataset" : "Model"}: Example resource` });
      expect(fetch.mock.calls.some(([url]) => url === `/api/${kind}/public/7`)).toBe(true);
      expect(fetch.mock.calls.some(([url]) => url === `/api/${kind}/7`)).toBe(false);
    }
  );
  it("retains the actual About copy and workflow illustration", async () => {
    await setup("/about");
    await screen.findByRole("heading", { name: "About Texera" });
    expect(screen.getByText("Natural-language data science through AI agents")).toBeDefined();
    expect(screen.getByRole("img", { name: "workflow gui screenshot" }).getAttribute("src")).toBe(
      "/assets/workflow_gui.png"
    );
  });
  it("opens the existing same-origin discussion forum", async () => {
    await setup("/user/discussion", "REGULAR");
    expect((await screen.findByTitle("Texera discussion forum")).getAttribute("src")).toBe("/forum");
  });
  it("rejects malformed resource identifiers with a visible route error", async () => {
    const { fetch } = await setup("/hub/dataset/result/detail/NaN");
    await screen.findByText("Invalid resource identifier.");
    expect(fetch.mock.calls.some(([url]) => url.includes("dataset/public/NaN"))).toBe(false);
  });
  it("preserves version query parameters while switching workspace views on one route", async () => {
    await setup("/user/workflow/42?version=3", "REGULAR");
    await screen.findByRole("heading", { name: "Workflow 42" });
    expect(screen.getByLabelText("Workspace version").textContent).toBe("3");
    fireEvent.click(screen.getByRole("button", { name: "Switch workspace view" }));
    await waitFor(() => expect(screen.getByLabelText("Route").textContent).toBe("/user/workflow/42/form?version=3"));
    expect(screen.getByLabelText("Workspace view").textContent).toBe("form");
  });
});
