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
import { VersionRestoreDialog, restoreWorkflowVersion } from "./version-restore";
import * as Y from "yjs";
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import { WorkflowSession } from "../workspace/workflow-session";
import type { Workflow } from "../../core/common/type/workflow";
afterEach(cleanup);
const current = {
  wid: 7,
  name: "Current title ü",
  description: "Current description",
  readonly: false,
  content: {
    operators: [],
    links: [],
    commentBoxes: [],
    operatorPositions: {},
    settings: { dataTransferBatchSize: 400, executionMode: "PIPELINED" },
  },
};
const historic = {
  ...current,
  name: "Old title",
  description: "Old description",
  readonly: true,
  content: {
    ...current.content,
    settings: { dataTransferBatchSize: 100, executionMode: "MATERIALIZED" },
    formBinding: { fields: [], instruction: { body: "Retained instructions" } },
  },
};
function build() {
  const fetcher = vi.fn<(path: string, init?: RequestInit) => Promise<Response>>(async path =>
    Response.json(path === "/api/workflow/7" ? current : path === "/api/version/7/2" ? historic : current)
  );
  const createSharedModel = () => {
    const shared = new SharedModel(7, undefined, undefined, { connect: false });
    shared.wsProvider.synced = true;
    return shared;
  };
  return {
    fetcher,
    createSharedModel,
    api: new ApiClient({ fetch: fetcher as typeof fetch, getToken: () => "token" }),
  };
}
describe("restoring an existing workflow version", () => {
  it("rolls collaborators back to their latest room content on persist failure and refuses an unsynchronized room", async () => {
    const { api, fetcher } = build();
    const shared = new SharedModel(7, undefined, undefined, { connect: false });
    shared.wsProvider.synced = true;
    const peer = new SharedModel(7, undefined, undefined, { connect: false });
    const room = {
      ...current,
      content: { ...current.content, settings: { ...current.content.settings, dataTransferBatchSize: 999 } },
    };
    const peerSession = new WorkflowSession(room as unknown as Workflow, {
      shared: peer,
      autosaveMs: 0,
      save: async value => value,
    });
    Y.applyUpdate(shared.yDoc, Y.encodeStateAsUpdate(peer.yDoc));
    shared.yDoc.on("update", update => Y.applyUpdate(peer.yDoc, update));
    fetcher.mockImplementation(async path =>
      path.includes("persist")
        ? new Response("Forbidden", { status: 403 })
        : Response.json(path === "/api/workflow/7" ? current : historic)
    );
    await expect(restoreWorkflowVersion(api, 7, 2, { createSharedModel: () => shared })).rejects.toThrow("Forbidden");
    expect(peerSession.getSnapshot().workflow.content.settings.dataTransferBatchSize).toBe(999);
    peerSession.dispose();
    const offline = new SharedModel(7, undefined, undefined, { connect: false });
    const destroy = vi.spyOn(offline, "destroy");
    await expect(
      restoreWorkflowVersion(api, 7, 2, { createSharedModel: () => offline, syncTimeoutMs: 1 })
    ).rejects.toThrow("synchronize");
    expect(destroy).toHaveBeenCalledOnce();
  });
  it("waits for the retained live room, updates collaborators on confirmed restore and survives a later join", async () => {
    const { api, fetcher } = build();
    const live = new SharedModel(7, undefined, undefined, { connect: false });
    const liveSession = new WorkflowSession(current as unknown as Workflow, {
      shared: live,
      autosaveMs: 0,
      save: async value => value,
    });
    const joining = new SharedModel(7, undefined, undefined, { connect: false });
    const createSharedModel = vi.fn(() => joining);
    const pending = restoreWorkflowVersion(api, 7, 2, { createSharedModel });
    await vi.waitFor(() => expect(createSharedModel).toHaveBeenCalled());
    expect(fetcher.mock.calls.some(([path]) => path.includes("persist"))).toBe(false);
    Y.applyUpdate(joining.yDoc, Y.encodeStateAsUpdate(live.yDoc));
    const forward = (update: Uint8Array) => Y.applyUpdate(live.yDoc, update);
    joining.yDoc.on("update", forward);
    joining.wsProvider.synced = true;
    await pending;
    expect(liveSession.getSnapshot().workflow.content.settings).toEqual(historic.content.settings);
    expect(liveSession.getSnapshot().workflow.content.formBinding).toEqual(historic.content.formBinding);
    const later = new SharedModel(7, undefined, undefined, { connect: false });
    Y.applyUpdate(later.yDoc, Y.encodeStateAsUpdate(live.yDoc));
    const next = new WorkflowSession(historic as unknown as Workflow, {
      shared: later,
      autosaveMs: 0,
      save: async value => value,
    });
    expect(next.getSnapshot().workflow.content.settings).toEqual(historic.content.settings);
    next.dispose();
    liveSession.dispose();
  });
  it("authorizes against the actual current workflow and persists historical content to the same wid with current metadata", async () => {
    const { api, fetcher, createSharedModel } = build();
    await restoreWorkflowVersion(api, 7, 2, { createSharedModel });
    const save = fetcher.mock.calls.find(([path]) => path === "/api/workflow/persist");
    expect(JSON.parse(String(save?.[1]?.body))).toEqual({
      wid: 7,
      name: current.name,
      description: current.description,
      content: JSON.stringify(historic.content),
    });
    expect(fetcher.mock.calls.some(([path]) => path.includes("clone") || path.endsWith("/create"))).toBe(false);
  });
  it("rejects current read-only/malformed targets and invalid IDs before persisting", async () => {
    const { api, fetcher } = build();
    fetcher.mockResolvedValueOnce(Response.json({ ...current, readonly: true }));
    await expect(restoreWorkflowVersion(api, 7, 2)).rejects.toThrow("write access");
    expect(fetcher.mock.calls.some(([path]) => path.includes("persist"))).toBe(false);
    await expect(restoreWorkflowVersion(api, 0, 2)).rejects.toThrow("identifier");
    await expect(restoreWorkflowVersion(api, 7, NaN)).rejects.toThrow("identifier");
    fetcher
      .mockResolvedValueOnce(Response.json(current))
      .mockResolvedValueOnce(Response.json({ ...historic, content: { operators: null } }));
    await expect(restoreWorkflowVersion(api, 7, 2)).rejects.toThrow("workflow");
  });
  it("does not reuse preview authorization after a server permission downgrade", async () => {
    const { api, fetcher, createSharedModel } = build(),
      onRestored = vi.fn();
    render(
      <VersionRestoreDialog
        api={api}
        wid={7}
        versionId={2}
        createSharedModel={createSharedModel}
        onRestored={onRestored}
        onClose={() => {}}
      />
    );
    const confirm = await screen.findByRole("button", { name: "Restore this version" });
    await waitFor(() => expect((confirm as HTMLButtonElement).disabled).toBe(false));
    fetcher.mockImplementation(async path =>
      Response.json(path === "/api/workflow/7" ? { ...current, readonly: true } : historic)
    );
    fireEvent.click(confirm);
    await screen.findByText(/write access/);
    expect(onRestored).not.toHaveBeenCalled();
    expect(fetcher.mock.calls.some(([path]) => path.includes("persist"))).toBe(false);
  });
  it("requires confirmation and keeps the dialog open on server failure; successful restore invokes current-wid navigation", async () => {
    const { api, fetcher, createSharedModel } = build(),
      onRestored = vi.fn(),
      onClose = vi.fn();
    render(
      <VersionRestoreDialog
        api={api}
        wid={7}
        versionId={2}
        createSharedModel={createSharedModel}
        onRestored={onRestored}
        onClose={onClose}
      />
    );
    const confirm = await screen.findByRole("button", { name: "Restore this version" });
    await waitFor(() => expect((confirm as HTMLButtonElement).disabled).toBe(false));
    expect(fetcher.mock.calls.some(([path]) => path.includes("persist"))).toBe(false);
    fetcher.mockImplementation(async path =>
      path.includes("persist")
        ? new Response("Forbidden", { status: 403 })
        : Response.json(path === "/api/workflow/7" ? current : historic)
    );
    fireEvent.click(confirm);
    await screen.findByText("Forbidden");
    expect(onRestored).not.toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();
    fetcher.mockImplementation(async path => Response.json(path === "/api/version/7/2" ? historic : current));
    fireEvent.click(confirm);
    await waitFor(() => expect(onRestored).toHaveBeenCalledWith(7));
  });
});
