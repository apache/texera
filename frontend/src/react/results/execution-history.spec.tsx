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
import { ExecutionHistoryPage, searchExecutions, runtimeSeries } from "./execution-history";
import { ResultsRepository } from "./results-repository";
import type { WorkflowExecutionsEntry } from "./results-repository";
import type { WorkflowSocket } from "../workspace/workflow-socket";
afterEach(cleanup);
const entry = (eId: number, name: string, status = 3): WorkflowExecutionsEntry => ({
  eId,
  name,
  status,
  vId: 2,
  cuId: 9,
  whId: null,
  sId: 1,
  userName: "Üser",
  avatar: "",
  startingTime: 1000,
  completionTime: 61000,
  result: "",
  bookmarked: false,
  logLocation: "",
});
describe("execution history parity", () => {
  it("keeps supported field/name queries, quotes and invalid status/fields explicit", () => {
    const entries = [entry(1, "Alpha job"), entry(2, "Beta job", 4)];
    expect(searchExecutions(entries, "Alpha user:Üser status:completed").map(e => e.eId)).toEqual([1]);
    expect(searchExecutions(entries, '"Alpha job"').map(e => e.eId)).toEqual([1]);
    expect(searchExecutions([], "")).toEqual([]);
    expect(() => searchExecutions(entries, "owner:test")).toThrow("Cannot search");
    expect(() => searchExecutions(entries, "status:unknown")).toThrow("not available");
  });
  it("scales runtime time units against first timestamp and skips sink/malformed operators", () => {
    expect(
      runtimeSeries(
        [
          { operatorId: "Map-123456", timestamp: 1000, totalDataProcessingTime: 2e9 },
          { operatorId: "Map-123456", timestamp: 2000, totalDataProcessingTime: 3e9 },
          { operatorId: "sink-000001", timestamp: 2000, totalDataProcessingTime: 1e9 },
          { timestamp: 2000 },
        ] as unknown as Parameters<typeof runtimeSeries>[0],
        "totalDataProcessingTime"
      )
    ).toEqual([
      {
        operatorId: "Map-123456",
        name: "Map-123456",
        points: [
          { x: 0, y: 2 },
          { x: 1, y: 3 },
        ],
      },
    ]);
    expect(runtimeSeries([], "inputTupleCount")).toEqual([]);
  });
  it("retains inverted bookmark wire semantics and exact delete/rename/statistics contracts", async () => {
    const fetcher = vi.fn(async () => new Response("{}"));
    const api = new ApiClient({ fetch: fetcher, getToken: () => null });
    const repo = new ResultsRepository({} as WorkflowSocket, api);
    await repo.bookmarkExecutions(7, [1, 2], false);
    expect(JSON.parse(String(fetcher.mock.calls[0][1].body))).toEqual({ wid: 7, eIds: [1, 2], isBookmarked: false });
    await repo.deleteExecutions(7, [1]);
    expect(fetcher.mock.calls[1][0]).toBe("/api/executions/delete_executions");
    await repo.renameExecution(7, 1, "Ü");
    expect(JSON.parse(String(fetcher.mock.calls[2][1].body))).toEqual({ wid: 7, eId: 1, executionName: "Ü" });
    await repo.runtimeStatistics(7, 1, 9);
    expect(fetcher.mock.calls[3][0]).toBe("/api/executions/7/stats/1?cuid=9");
    await expect(repo.deleteExecutions(7, [-1])).rejects.toThrow("execution");
    await expect(repo.renameExecution(7, 1, " ")).rejects.toThrow("name");
  });
  it("manages selection/bookmark/delete confirmation and preserves rows on backend errors", async () => {
    const response = [entry(1, "Alpha"), entry(2, "Beta", 4)];
    const fetcher = vi.fn(async (path: string, init: RequestInit) => {
      if (init.method === "PUT" && path.includes("delete")) return new Response("Forbidden", { status: 403 });
      return new Response(JSON.stringify(response));
    });
    render(
      <ExecutionHistoryPage
        api={new ApiClient({ fetch: fetcher as typeof fetch, getToken: () => null })}
        wid={7}
      />
    );
    await screen.findByText("Alpha");
    fireEvent.click(screen.getByLabelText("Select execution 1"));
    fireEvent.click(screen.getByRole("button", { name: "Toggle selected bookmarks" }));
    await waitFor(() =>
      expect(fetcher.mock.calls.some(([path]) => path.includes("set_execution_bookmarks"))).toBe(true)
    );
    expect(
      JSON.parse(String(fetcher.mock.calls.find(([path]) => path.includes("set_execution_bookmarks"))?.[1].body))
    ).toEqual({ wid: 7, eIds: [1], isBookmarked: false });
    fireEvent.click(screen.getByRole("button", { name: "Delete selected" }));
    expect(fetcher.mock.calls.some(([path]) => path.includes("delete_executions"))).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Confirm deletion" }));
    await screen.findByText("Forbidden");
    expect(screen.getByText("Alpha")).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Search executions"), { target: { value: "status:failed" } });
    expect(screen.queryByText("Alpha")).toBeNull();
    expect(screen.getByText("Beta")).toBeTruthy();
  });
  it("offers only writable datasets and handles inaccessible dataset lists as errors", async () => {
    const fetcher = vi.fn(
      async () =>
        new Response(
          JSON.stringify([
            { accessPrivilege: "READ", dataset: { did: 1, name: "readonly" } },
            { accessPrivilege: "WRITE", dataset: { did: 2, name: "writable" } },
          ])
        )
    );
    const repo = new ResultsRepository({} as WorkflowSocket, new ApiClient({ fetch: fetcher, getToken: () => null }));
    expect(await repo.writableDatasets()).toEqual([{ id: 2, name: "writable" }]);
    expect(fetcher.mock.calls[0][0]).toBe("/api/dataset/list");
    fetcher.mockResolvedValueOnce(new Response("No access", { status: 403 }));
    await expect(repo.writableDatasets()).rejects.toThrow("No access");
  });
});
