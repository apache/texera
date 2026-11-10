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
import {
  AdminExecutionsPage,
  AdminUsersPage,
  FeedbackPage,
  executionElapsedSeconds,
  executionDisplayStatus,
} from "./administration";
import { QuotaPage, aggregateQuotaTrend, formatQuotaSize } from "./quota";

afterEach(cleanup);
const change = (label: string, value: string) => fireEvent.change(screen.getByLabelText(label), { target: { value } });
function setup(responder: (url: string, init: RequestInit) => Response | Promise<Response>) {
  const fetch = vi.fn((url: string, init: RequestInit = {}) => responder(url, init));
  return { fetch, resources: new ResourceApi(new ApiClient({ fetch })) };
}
const user = {
  uid: 1,
  name: "Alice",
  email: "alice@example.com",
  role: "ADMIN",
  comment: "First",
  joiningReason: "Research",
  affiliation: "UCI",
  lastLogin: 0,
  accountCreation: 1,
};
const execution = {
  workflowId: 10,
  workflowName: "Workflow",
  userName: "Alice",
  userId: 1,
  executionId: 30,
  executionName: "Run",
  executionStatus: "RUNNING",
  startTime: 1000,
  endTime: 0,
  executionTime: 0,
  access: true,
};

describe("quota analysis", () => {
  it("formats zero/invalid sizes safely and aggregates uploads into dated counts", () => {
    expect(formatQuotaSize(0)).toBe("0 B");
    expect(formatQuotaSize(1024)).toBe("1 KB");
    expect(formatQuotaSize(NaN)).toBe("0 B");
    const now = new Date("2026-11-06T00:00:00Z");
    expect(
      aggregateQuotaTrend(
        [new Date("2026-11-01T00:00:00Z").getTime(), new Date("2026-11-01T12:00:00Z").getTime(), 0, NaN],
        now
      )
    ).toEqual([["2026-11-01", 2]]);
    expect(aggregateQuotaTrend([], now)).toEqual([]);
  });

  it("renders owned/shared totals, dataset/workflow charts and grouped execution storage, then deletes after confirmation", async () => {
    const test = setup((url, init) =>
      init.method === "DELETE"
        ? new Response(null, { status: 204 })
        : Response.json(
            url.includes("created_datasets")
              ? [{ did: 1, name: "Data", size: 2048, creationTime: Date.now() }]
              : url.includes("created_workflows")
                ? [{ workflowId: 1, workflowName: "Workflow", creationTime: Date.now() }]
                : url.includes("access_workflows")
                  ? [1, 2]
                  : [
                      {
                        eid: 3,
                        workflowId: 1,
                        workflowName: "Workflow",
                        resultBytes: 1024,
                        logBytes: 1024,
                        runTimeStatsBytes: 0,
                      },
                    ]
          )
    );
    render(<QuotaPage resources={test.resources} />);
    await screen.findByRole("img", { name: "Dataset Size Distribution" });
    expect(screen.getByRole("img", { name: "Dataset Upload Overview" })).toBeDefined();
    expect(screen.getByRole("img", { name: "Workflow Upload Overview" })).toBeDefined();
    expect(screen.getByText("Accessible workflows: 2")).toBeDefined();
    expect(screen.getByText("Execution storage: 2 KB")).toBeDefined();
    fireEvent.click(screen.getByRole("button", { name: "Delete execution collection 3" }));
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Delete" }));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Delete execution collection 3" })).toBeNull());
    expect(screen.getByText("Execution storage: 0 B")).toBeDefined();
    expect(test.fetch.mock.calls.at(-1)?.[0]).toBe("/api/quota/deleteCollection/3");
  });

  it("uses admin quota scope, keeps a collection on deletion failure, and retries a failed load", async () => {
    let offline = true;
    const test = setup((url, init) =>
      offline
        ? new Response("Offline", { status: 503 })
        : init.method
          ? new Response("Locked", { status: 409 })
          : Response.json(
              url.includes("user_quota_size")
                ? [{ eid: 2, workflowId: 1, workflowName: "W", resultBytes: 0, logBytes: 0, runTimeStatsBytes: 0 }]
                : []
            )
    );
    render(
      <QuotaPage
        resources={test.resources}
        userId={7}
      />
    );
    await screen.findByRole("alert");
    offline = false;
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await screen.findByRole("button", { name: "Delete execution collection 2" });
    expect(
      test.fetch.mock.calls
        .filter(([url]) => url.includes("created_datasets"))
        .every(([url]) => url.endsWith("?user_id=7"))
    ).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Delete execution collection 2" }));
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Delete" }));
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("Locked"));
    expect(screen.getByRole("button", { name: "Delete execution collection 2" })).toBeDefined();
    expect(test.fetch.mock.calls.at(-1)?.[0]).toBe("/api/admin/user/deleteCollection/2");
  });
});

describe("AdminUsersPage", () => {
  it("filters/searches/sorts users and saves an edit while preventing self-role changes", async () => {
    const users = [user, { ...user, uid: 2, name: "Bob", role: "REGULAR", comment: "Other" }];
    const test = setup((url, init) => Response.json(init.method ? {} : url.endsWith("/counts") ? [] : users));
    render(
      <AdminUsersPage
        resources={test.resources}
        currentUid={1}
        activeTimeInMinutes={5}
      />
    );
    await screen.findByRole("button", { name: "Edit user Alice" });
    change("Search users", " bOb ");
    expect(screen.queryByRole("button", { name: "Edit user Alice" })).toBeNull();
    expect(screen.getByRole("button", { name: "Edit user Bob" })).toBeDefined();
    change("Search users", "");
    change("Role filter", "ADMIN");
    expect(screen.queryByRole("button", { name: "Edit user Bob" })).toBeNull();
    change("Role filter", "");
    fireEvent.click(screen.getByRole("button", { name: "Edit user Alice" }));
    expect((screen.getByLabelText("User role") as HTMLSelectElement).disabled).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    fireEvent.click(screen.getByRole("button", { name: "Edit user Bob" }));
    change("User name", "Robert");
    change("User comment", "Updated");
    change("User role", "RESTRICTED");
    fireEvent.click(screen.getByRole("button", { name: "Save user" }));
    await screen.findByRole("button", { name: "Edit user Robert" });
    expect(JSON.parse(String(test.fetch.mock.calls.find(([, init]) => init.method === "PUT")?.[1].body))).toEqual({
      uid: 2,
      name: "Robert",
      email: "alice@example.com",
      role: "RESTRICTED",
      comment: "Updated",
    });
  });

  it("adds server-generated users and exposes admin quota/feedback views", async () => {
    let users = [user];
    const test = setup((url, init) => {
      if (init.method === "POST") {
        users = [...users, { ...user, uid: 2, name: "New" }];
        return Response.json({});
      }
      return Response.json(
        url.endsWith("/list")
          ? users
          : url.endsWith("/counts")
            ? [{ uid: 1, count: 1 }]
            : url.includes("/feedback/user")
              ? [{ fid: 3, uid: 1, message: "Feedback text", creationTime: 0 }]
              : []
      );
    });
    render(
      <AdminUsersPage
        resources={test.resources}
        currentUid={1}
      />
    );
    await screen.findByRole("button", { name: "Edit user Alice" });
    fireEvent.click(screen.getByRole("button", { name: "Add user" }));
    await screen.findByRole("button", { name: "Edit user New" });
    expect(test.fetch.mock.calls.some(([url, init]) => url === "/api/admin/user/add/" && init.method === "POST")).toBe(
      true
    );
    fireEvent.click(screen.getByRole("button", { name: "Feedback for Alice" }));
    await screen.findByText("Feedback text");
    expect(screen.queryByLabelText("Feedback message")).toBeNull();
  });

  it("reports failed updates without replacing persisted values", async () => {
    const test = setup((url, init) =>
      init.method ? new Response("Denied", { status: 403 }) : Response.json(url.endsWith("/counts") ? [] : [user])
    );
    render(
      <AdminUsersPage
        resources={test.resources}
        currentUid={9}
      />
    );
    await screen.findByRole("button", { name: "Edit user Alice" });
    fireEvent.click(screen.getByRole("button", { name: "Edit user Alice" }));
    change("User name", "Unsaved");
    fireEvent.click(screen.getByRole("button", { name: "Save user" }));
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("Denied"));
    expect(screen.getByRole("button", { name: "Edit user Alice" })).toBeDefined();
  });
});

describe("FeedbackPage", () => {
  it("loads and submits feedback, preserving whitespace validation and plain-text rendering", async () => {
    let messages: unknown[] = [];
    const test = setup((_url, init) => {
      if (init.method) {
        messages = [{ fid: 1, uid: 2, message: "<b>Hello</b>", creationTime: 0 }];
        return new Response(null, { status: 204 });
      }
      return Response.json(messages);
    });
    const rendered = render(<FeedbackPage resources={test.resources} />);
    await screen.findByText("No feedback yet.");
    change("Feedback message", "   ");
    fireEvent.click(screen.getByRole("button", { name: "Submit feedback" }));
    expect(test.fetch).toHaveBeenCalledTimes(1);
    change("Feedback message", " <b>Hello</b> ");
    fireEvent.click(screen.getByRole("button", { name: "Submit feedback" }));
    await screen.findByText("<b>Hello</b>");
    expect(rendered.container.querySelector("b")).toBeNull();
    expect(JSON.parse(String(test.fetch.mock.calls.find(([, init]) => init.method)?.[1].body))).toEqual({
      message: "<b>Hello</b>",
    });
  });
});

describe("AdminExecutionsPage", () => {
  it("computes elapsed/recent-completion status without mutating server rows", () => {
    expect(executionElapsedSeconds(execution, 11000)).toBe(10);
    expect(executionElapsedSeconds({ ...execution, executionStatus: "COMPLETED", endTime: 6000 }, 20000)).toBe(5);
    expect(executionDisplayStatus({ ...execution, executionStatus: "COMPLETED", endTime: 19000 }, 20000)).toBe(
      "JUST COMPLETED"
    );
    expect(executionDisplayStatus({ ...execution, executionStatus: "COMPLETED", endTime: 1000 }, 20000)).toBe(
      "COMPLETED"
    );
    expect(execution.executionStatus).toBe("RUNNING");
  });

  it("sends zero-based pagination, server sorting/status filters and allowed control requests", async () => {
    const control = vi.fn().mockResolvedValue(undefined);
    const history = vi.fn();
    const test = setup(url => Response.json(url.endsWith("/totalWorkflow") ? 20 : [execution]));
    render(
      <AdminExecutionsPage
        resources={test.resources}
        control={control}
        onHistory={history}
        refreshMs={60000}
      />
    );
    await screen.findByText("Run (30)");
    fireEvent.click(screen.getByRole("button", { name: "Next page" }));
    await waitFor(() => expect(test.fetch.mock.calls.some(([url]) => url.includes("executionList/5/1/"))).toBe(true));
    fireEvent.click(screen.getByRole("button", { name: "Sort by Workflow" }));
    await waitFor(() => expect(test.fetch.mock.calls.some(([url]) => url.includes("/workflow_name/asc"))).toBe(true));
    fireEvent.click(screen.getByLabelText("Filter PAUSED"));
    await waitFor(() =>
      expect(
        test.fetch.mock.calls.some(([url]) => new URL(url, "https://test").searchParams.get("filter") === "PAUSED")
      ).toBe(true)
    );
    await screen.findByRole("button", { name: "Pause Run" });
    fireEvent.click(screen.getByRole("button", { name: "Pause Run" }));
    await waitFor(() => expect(control).toHaveBeenCalledWith(10, "WorkflowPauseRequest"));
    expect((screen.getByRole("button", { name: "Resume Run" }) as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "History for Workflow" }));
    expect(history).toHaveBeenCalledWith(10, "Workflow");
  });

  it("keeps a polling failure recoverable, cleans up polling on unmount and reports control failure", async () => {
    let fail = true;
    const test = setup(url =>
      fail ? new Response("Offline", { status: 503 }) : Response.json(url.endsWith("/totalWorkflow") ? 1 : [execution])
    );
    const control = vi.fn().mockRejectedValue(new Error("Control denied"));
    const rendered = render(
      <AdminExecutionsPage
        resources={test.resources}
        control={control}
        onHistory={() => {}}
        refreshMs={20}
      />
    );
    await screen.findByRole("alert");
    fail = false;
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await screen.findByText("Run (30)");
    fireEvent.click(screen.getByRole("button", { name: "Kill Run" }));
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("Control denied"));
    rendered.unmount();
    const calls = test.fetch.mock.calls.length;
    await new Promise(resolve => setTimeout(resolve, 50));
    expect(test.fetch).toHaveBeenCalledTimes(calls);
  });
});
