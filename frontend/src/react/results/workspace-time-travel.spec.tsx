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
import { WorkspaceTimeTravel } from "./workspace-time-travel";
import { ApiClient } from "../core/api-client";
import type { WorkflowSession } from "../workspace/workflow-session";
import type { WorkflowRuntime } from "../workspace/workflow-runtime";
afterEach(cleanup);
describe("time travel interface", () => {
  it("loads logged execution interactions, replays selected point and returns original workflow on exit", async () => {
    const content = {
      operators: [],
      links: [],
      commentBoxes: [],
      operatorPositions: {},
      settings: { dataTransferBatchSize: 100, executionMode: "PIPELINED" },
    };
    const fetcher = vi.fn(async (path: string) =>
      Response.json(
        path === "/api/executions/7"
          ? [{ eId: 5, vId: 2, name: "Run", logLocation: "log", startingTime: 0 }]
          : path.includes("interactions")
            ? ["checkpoint α"]
            : { wid: 7, name: "Flow", content }
      )
    );
    const snapshot = { workflow: { wid: 7, name: "Flow", content } },
      session = {
        getSnapshot: () => snapshot,
        save: vi.fn(async () => snapshot.workflow),
        setAutosaveSuspended: vi.fn(),
        setExecutionLocked: vi.fn(),
        applyRemoteWorkflow: vi.fn(),
        enterPreview: vi.fn(),
        exitPreview: vi.fn(),
      } as unknown as WorkflowSession;
    const runtime = {
      getSnapshot: () => ({ state: "Completed" }),
      subscribe: () => () => {},
      run: vi.fn(async () => undefined),
      kill: vi.fn(),
    } as unknown as WorkflowRuntime;
    const onReplayChange = vi.fn();
    render(
      <WorkspaceTimeTravel
        api={new ApiClient({ fetch: fetcher as typeof fetch, getToken: () => null })}
        session={session}
        runtime={runtime}
        onReplayChange={onReplayChange}
      />
    );
    fireEvent.click(await screen.findByRole("button", { name: "Interactions for execution 5" }));
    fireEvent.click(await screen.findByRole("button", { name: "checkpoint α" }));
    await waitFor(() => expect(runtime.run).toHaveBeenCalled());
    expect(onReplayChange).toHaveBeenLastCalledWith({ eid: 5, interaction: "checkpoint α" });
    fireEvent.click(screen.getByRole("button", { name: "Return to current workflow" }));
    await waitFor(() => expect(onReplayChange).toHaveBeenLastCalledWith(undefined));
    expect(session.exitPreview).toHaveBeenCalled();
  });
});
