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
import * as Y from "yjs";
import { WorkflowSession } from "./workflow-session";
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import { ExecutionMode, type Workflow } from "../../core/common/type/workflow";
import type { OperatorPredicate } from "../../core/workspace/types/workflow-common.interface";

const operator = (id: string): OperatorPredicate => ({
  operatorID: id,
  operatorType: "Test",
  operatorVersion: "1",
  operatorProperties: { text: "日本語" },
  inputPorts: [{ portID: "input-0" }],
  outputPorts: [{ portID: "output-0" }],
  showAdvanced: false,
});
const initial = (): Workflow => ({
  wid: 42,
  name: "Workflow",
  description: "",
  readonly: false,
  creationTime: 1,
  lastModifiedTime: 1,
  isPublished: 0,
  content: {
    operators: [operator("a")],
    operatorPositions: { a: { x: 10, y: 20 } },
    links: [],
    commentBoxes: [],
    settings: { dataTransferBatchSize: 400, executionMode: ExecutionMode.PIPELINED },
  },
});
const sessions: WorkflowSession[] = [];
const models: SharedModel[] = [];
function model() {
  const result = new SharedModel(42, undefined, undefined, { connect: false });
  models.push(result);
  return result;
}
function build(
  workflow = initial(),
  options: { shared?: SharedModel; save?: (workflow: Workflow) => Promise<Workflow>; autosaveMs?: number } = {}
) {
  const save = options.save ?? vi.fn(async (workflow: Workflow) => workflow);
  const session = new WorkflowSession(workflow, {
    ...options,
    shared: options.shared ?? model(),
    save,
    autosaveMs: options.autosaveMs ?? 0,
  });
  sessions.push(session);
  return { session, save };
}
afterEach(() => {
  sessions.splice(0).forEach(session => session.dispose());
  models.splice(0).forEach(shared => shared.destroy());
  vi.useRealTimers();
});

describe("React workflow session", () => {
  it("destroys both documents and their providers on close, and preserves a live reader's access on exit", () => {
    const { session } = build({ ...initial(), readonly: true });
    const live = session.shared,
      liveProvider = vi.spyOn(live.wsProvider, "destroy");
    session.enterPreview(initial().content);
    session.exitPreview();
    expect(session.getSnapshot().workflow.readonly).toBe(true);
    expect(session.getSnapshot().canEdit).toBe(false);
    session.enterPreview(initial().content);
    const preview = session.shared,
      previewProvider = vi.spyOn(preview.wsProvider, "destroy");
    session.dispose();
    expect(live.yDoc.isDestroyed).toBe(true);
    expect(preview.yDoc.isDestroyed).toBe(true);
    expect(liveProvider).toHaveBeenCalledOnce();
    expect(previewProvider).toHaveBeenCalledOnce();
  });
  it("isolates history in a separate document and restores concurrent live edits without preview tombstones", async () => {
    vi.useFakeTimers();
    const workflow = initial();
    workflow.content.operators[0] = {
      ...workflow.content.operators[0],
      operatorProperties: { text: "live", code: "print('live')" },
    };
    const live = model(),
      { session, save } = build(workflow, { shared: live, autosaveMs: 100 });
    const peer = model();
    Y.applyUpdate(peer.yDoc, Y.encodeStateAsUpdate(live.yDoc));
    const peerSession = build(workflow, { shared: peer }).session;
    const toPeer = (update: Uint8Array) => Y.applyUpdate(peer.yDoc, update, "peer");
    const toLive = (update: Uint8Array) => Y.applyUpdate(live.yDoc, update, "peer");
    live.yDoc.on("update", toPeer);
    peer.yDoc.on("update", toLive);
    session.setProperties("a", { text: "local unsaved", code: "print('live')" });
    const originalGraph = session.graph,
      provider = live.wsProvider,
      code = originalGraph.getSharedOperatorPropertyType("a").get("code");
    const documentId = session.getSnapshot().documentId,
      undoDepth = live.undoManager.undoStack.length;
    const broadcasts = vi.fn();
    live.yDoc.on("update", broadcasts);
    const historical = {
      ...workflow.content,
      operators: [{ ...operator("a"), operatorProperties: { text: "history", code: "print('history')" } }],
      formBinding: {
        fields: [{ id: "history-field", operatorID: "a", propertyKey: "text", displayName: "Historical" }],
      },
    };
    session.enterPreview(historical);
    expect(session.shared).not.toBe(live);
    expect(session.graph).not.toBe(originalGraph);
    expect(session.shared.wsProvider.shouldConnect).toBe(false);
    expect(live.wsProvider).toBe(provider);
    expect(session.getSnapshot().documentId).not.toBe(documentId);
    expect(session.getSnapshot().previewing).toBe(true);
    expect(session.getSnapshot().canEdit).toBe(false);
    expect(session.getSnapshot().workflow.readonly).toBe(true);
    expect(session.graph.getSharedOperatorPropertyType("a").get("code")).not.toBe(code);
    expect(peerSession.graph.getOperator("a").operatorProperties.text).toBe("local unsaved");
    expect(() => session.setProperties("a", { text: "unsafe" })).toThrow(/preview/i);
    await expect(session.save()).rejects.toThrow(/paused|preview/i);
    session.shared.yDoc.transact(() => session.graph.deleteOperator("a"));
    expect(broadcasts).not.toHaveBeenCalled();
    peerSession.setProperties("a", { text: "peer latest 日本語", code: "print('peer')" });
    peerSession.addOperator(operator("new-peer-step"), { x: 200, y: 0 });
    peerSession.setFormBinding({
      fields: [{ id: "live-field", operatorID: "a", propertyKey: "text", displayName: "Live latest" }],
    });
    await vi.advanceTimersByTimeAsync(500);
    expect(save).not.toHaveBeenCalled();
    expect(session.graph.hasOperator("a")).toBe(false);
    session.exitPreview();
    expect(session.graph).toBe(originalGraph);
    expect(session.shared).toBe(live);
    expect(session.shared.wsProvider).toBe(provider);
    expect(session.getSnapshot().documentId).toBe(documentId);
    expect(session.getSnapshot().previewing).toBe(false);
    expect(session.getSnapshot().workflow.readonly).toBe(false);
    expect(session.getSnapshot().canEdit).toBe(true);
    expect(session.graph.getOperator("a").operatorProperties.text).toBe("peer latest 日本語");
    expect(session.graph.hasOperator("new-peer-step")).toBe(true);
    expect(session.graph.getSharedOperatorPropertyType("a").get("code")).toBe(code);
    expect(session.getSnapshot().workflow.content.formBinding?.fields[0].id).toBe("live-field");
    expect(live.undoManager.undoStack.length).toBe(undoDepth);
    expect(session.getSnapshot().canUndo).toBe(true);
    await vi.advanceTimersByTimeAsync(100);
    expect(save).toHaveBeenCalledOnce();
    expect(save.mock.calls[0][0].content.operators.find(op => op.operatorID === "a")?.operatorProperties.text).toBe(
      "peer latest 日本語"
    );
    live.yDoc.off("update", toPeer);
    peer.yDoc.off("update", toLive);
  });
  it("validates preview input before swapping documents and keeps repeated previews and owner locks independent", () => {
    const { session } = build();
    const live = session.shared,
      documentId = session.getSnapshot().documentId;
    expect(() => session.enterPreview({ ...initial().content, operatorPositions: {} })).toThrow(/position/i);
    expect(session.shared).toBe(live);
    expect(session.getSnapshot().documentId).toBe(documentId);
    session.setAutosaveSuspended(true, "agent");
    session.setExecutionLocked(true, "execution");
    session.enterPreview(initial().content);
    const firstPreview = session.shared,
      firstId = session.getSnapshot().documentId;
    session.enterPreview({
      ...initial().content,
      operators: [{ ...operator("a"), operatorProperties: { text: "second preview" } }],
    });
    expect(session.shared).not.toBe(firstPreview);
    expect(session.getSnapshot().documentId).not.toBe(firstId);
    session.exitPreview();
    session.exitPreview();
    expect(session.shared).toBe(live);
    expect(session.graph.getOperator("a").operatorProperties.text).toBe("日本語");
    expect(session.isAutosaveSuspended("agent")).toBe(true);
    expect(session.getSnapshot().canEdit).toBe(false);
    session.setAutosaveSuspended(false, "agent");
    session.setExecutionLocked(false, "execution");
    expect(session.getSnapshot().canEdit).toBe(true);
    session.dispose();
    expect(() => session.enterPreview(initial().content)).toThrow(/closed/i);
  });
  it("keeps live first-sync metadata and agent snapshots out of the preview document", () => {
    const shared = model();
    shared.wsProvider.shouldConnect = true;
    const { session } = build(initial(), { shared });
    session.setFormBinding({
      fields: [{ id: "pending-live", operatorID: "a", propertyKey: "text", displayName: "Pending" }],
    });
    const historical = { ...initial().content, formBinding: { fields: [] } };
    session.enterPreview(historical);
    const preview = session.shared;
    expect(session.getSnapshot().workflow.content.formBinding).toEqual({ fields: [] });
    shared.wsProvider.emit("sync", [true]);
    expect(shared.contentMetaMap.get("formBinding")?.fields[0].id).toBe("pending-live");
    expect(preview.contentMetaMap.get("formBinding")).toEqual({ fields: [] });
    session.applyRemoteWorkflow({
      ...initial().content,
      operators: [{ ...operator("a"), operatorProperties: { text: "agent live" } }],
    });
    expect(session.graph.getOperator("a").operatorProperties.text).toBe("日本語");
    session.exitPreview();
    expect(session.graph.getOperator("a").operatorProperties.text).toBe("agent live");
  });
  it("rejects malformed external operators, settings and comments before changing the shared graph", () => {
    const { session } = build();
    const original = session.getSnapshot().workflow.content;
    const malformed = [
      { ...original, settings: { ...original.settings, dataTransferBatchSize: 0 } },
      { ...original, settings: { ...original.settings, executionMode: "BAD" } },
      { ...original, operators: [{ ...original.operators[0], inputPorts: [{}] }] },
      { ...original, commentBoxes: [{ commentBoxID: "c", comments: null, commentBoxPosition: { x: 0, y: 0 } }] },
    ];
    for (const content of malformed)
      expect(() => session.applyRemoteWorkflow(content as unknown as Workflow["content"])).toThrow(/malformed/i);
    expect(session.getSnapshot().workflow.content).toEqual(original);
  });
  it("keeps independent replay and agent autosave suspensions and editing locks", async () => {
    vi.useFakeTimers();
    const { session, save } = build(initial(), { autosaveMs: 100 });
    session.setAutosaveSuspended(true, "agent");
    session.setAutosaveSuspended(true, "replay");
    session.setProperties("a", { text: "changed" });
    session.setAutosaveSuspended(false, "agent");
    expect(session.isAutosaveSuspended("replay")).toBe(true);
    await expect(session.save()).rejects.toThrow(/paused|suspended/i);
    await vi.advanceTimersByTimeAsync(200);
    expect(save).not.toHaveBeenCalled();
    session.setExecutionLocked(true, "replay");
    session.setExecutionLocked(false, "execution");
    expect(session.getSnapshot().canEdit).toBe(false);
    session.setExecutionLocked(false, "replay");
    expect(session.getSnapshot().canEdit).toBe(true);
    session.setAutosaveSuspended(false, "replay");
    await vi.advanceTimersByTimeAsync(100);
    expect(save).toHaveBeenCalledTimes(1);
  });
  it("uses runtime defaults for settings cleared by the room or missing from legacy workflow content", () => {
    const shared = model();
    shared.contentMetaMap.set("settings", null);
    const defaults = { dataTransferBatchSize: 777, executionMode: ExecutionMode.MATERIALIZED };
    const session = new WorkflowSession(initial(), {
      shared,
      save: async value => value,
      defaultSettings: defaults,
      autosaveMs: 0,
    });
    sessions.push(session);
    expect(session.getSnapshot().workflow.content.settings).toEqual(defaults);
    const legacy = initial();
    delete (legacy.content as Partial<Workflow["content"]>).settings;
    const oldSession = new WorkflowSession(legacy, {
      shared: model(),
      save: async value => value,
      defaultSettings: defaults,
      autosaveMs: 0,
    });
    sessions.push(oldSession);
    expect(oldSession.getSnapshot().workflow.content.settings).toEqual(defaults);
  });
  it("undoes a pointer gesture as one move without absorbing the preceding edit", () => {
    const { session } = build();
    session.setProperties("a", { text: "keep" });
    session.beginGesture();
    session.moveOperator("a", { x: 40, y: 40 });
    session.moveOperator("a", { x: 80, y: 80 });
    session.endGesture();
    session.undo();
    expect(session.getSnapshot().workflow.content.operatorPositions.a).toEqual({ x: 10, y: 20 });
    expect(session.graph.getOperator("a").operatorProperties.text).toBe("keep");
    session.undo();
    expect(session.graph.getOperator("a").operatorProperties.text).toBe("日本語");
  });
  it("applies validated agent snapshots without re-saving server changes or replacing shared code buffers", async () => {
    vi.useFakeTimers();
    const { session, save } = build(initial(), { autosaveMs: 100 });
    const code = session.graph.getSharedOperatorPropertyType("a").get("text");
    const content = structuredClone(session.getSnapshot().workflow.content);
    content.operators[0].operatorProperties.text = "From agent";
    content.operatorPositions.a = { x: 100, y: 100 };
    session.setAutosaveSuspended(true);
    session.setExecutionLocked(true);
    session.applyRemoteWorkflow(content);
    expect(session.graph.getSharedOperatorPropertyType("a").get("text")).toBe(code);
    expect((code as Y.Text).toString()).toBe("From agent");
    expect(session.getSnapshot().saveState).toBe("saved");
    session.setAutosaveSuspended(false);
    await vi.advanceTimersByTimeAsync(200);
    expect(save).not.toHaveBeenCalled();
    expect(() => session.applyRemoteWorkflow({ ...content, operatorPositions: {} })).toThrow(/position/i);
    expect(() =>
      session.applyRemoteWorkflow({
        ...content,
        links: [
          {
            linkID: "bad",
            source: { operatorID: "a", portID: "missing" },
            target: { operatorID: "missing", portID: "input-0" },
          },
        ],
      })
    ).toThrow(/link/i);
    expect(session.graph.getAllLinks()).toEqual([]);
    expect(session.getSnapshot().workflow.content.operatorPositions.a).toEqual({ x: 100, y: 100 });
  });
  it("suspends future autosaves during generation then resumes a remaining local edit", async () => {
    vi.useFakeTimers();
    const { session, save } = build(initial(), { autosaveMs: 100 });
    session.setProperties("a", { text: "Local" });
    session.setAutosaveSuspended(true);
    await vi.advanceTimersByTimeAsync(200);
    expect(save).not.toHaveBeenCalled();
    session.setAutosaveSuspended(false);
    await vi.advanceTimersByTimeAsync(100);
    expect(save).toHaveBeenCalledTimes(1);
  });
  it("keeps comment edits, moves and deletion shared and undoable while enforcing read-only edits", () => {
    const { session } = build();
    session.addComment({
      commentBoxID: "c",
      commentBoxPosition: { x: 1, y: 2 },
      comments: [{ content: "Hello", creatorID: 7, creatorName: "A", creationTime: "now" }],
    });
    session.moveComment("c", { x: 20, y: 30 });
    session.editComment("c", 7, "now", "日本語");
    expect(session.graph.getCommentBox("c")).toMatchObject({
      commentBoxPosition: { x: 20, y: 30 },
      comments: [{ content: "日本語" }],
    });
    expect(() => session.moveComment("c", { x: NaN, y: 0 })).toThrow(/position/i);
    session.setExecutionLocked(true);
    expect(() => session.deleteCommentBox("c")).toThrow(/execution/i);
    session.setExecutionLocked(false);
    session.deleteCommentBox("c");
    expect(session.graph.hasCommentBox("c")).toBe(false);
    session.undo();
    expect(session.graph.hasCommentBox("c")).toBe(true);
  });
  it("reuses the existing graph and persisted content without losing settings, form bindings or Unicode", () => {
    const workflow = initial();
    workflow.content = {
      ...workflow.content,
      formBinding: { fields: [{ id: "field", operatorID: "a", propertyKey: "text", displayName: "Text" }] },
    };
    const { session } = build(workflow);
    expect(session.getSnapshot().workflow).toEqual(workflow);
    expect(session.graph.getOperator("a")).toEqual(operator("a"));
    expect(session.getSnapshot()).toBe(session.getSnapshot());
  });

  it("rejects malformed positions before seeding a shared document", () => {
    const workflow = initial();
    workflow.content = { ...workflow.content, operatorPositions: {} };
    const shared = model();
    expect(() => build(workflow, { shared })).toThrow(/position/i);
    expect(shared.operatorIDMap.size).toBe(0);
  });

  it("adds, moves and removes operators with their incident links as one undoable action", () => {
    const { session } = build();
    session.addOperator(operator("b"), { x: 200, y: 20 });
    session.addLink({
      linkID: "ab",
      source: { operatorID: "a", portID: "output-0" },
      target: { operatorID: "b", portID: "input-0" },
    });
    session.moveOperator("b", { x: 300, y: 40 });
    session.deleteOperators(["b"]);
    expect(session.getSnapshot().workflow.content.links).toEqual([]);
    session.undo();
    expect(session.graph.hasOperator("b")).toBe(true);
    expect(session.getSnapshot().workflow.content.links).toHaveLength(1);
    session.redo();
    expect(session.graph.hasOperator("b")).toBe(false);
  });

  it("keeps original graph validation for invalid/duplicate links and gates read-only editing", () => {
    const { session } = build();
    expect(() =>
      session.addLink({
        linkID: "bad",
        source: { operatorID: "a", portID: "missing" },
        target: { operatorID: "a", portID: "input-0" },
      })
    ).toThrow();
    const workflow = initial();
    workflow.readonly = true;
    const readonly = build(workflow).session;
    expect(() => readonly.addOperator(operator("b"), { x: 1, y: 1 })).toThrow(/read.only/i);
    expect(() => readonly.setProperties("a", {})).toThrow(/read.only/i);
    expect(() => readonly.undo()).toThrow(/read.only/i);
  });

  it("reflects remote Yjs edits and keeps an existing room's settings instead of overwriting with stale persisted settings", () => {
    const first = model();
    first.contentMetaMap.set("settings", { dataTransferBatchSize: 999, executionMode: ExecutionMode.MATERIALIZED });
    const { session } = build(initial(), { shared: first });
    expect(session.getSnapshot().workflow.content.settings.dataTransferBatchSize).toBe(999);
    const remote = new Y.Doc();
    Y.applyUpdate(remote, Y.encodeStateAsUpdate(first.yDoc));
    remote.getMap("elementPositionMap").set("a", { x: 70, y: 80 });
    Y.applyUpdate(first.yDoc, Y.encodeStateAsUpdate(remote));
    expect(session.getSnapshot().workflow.content.operatorPositions.a).toEqual({ x: 70, y: 80 });
    remote.destroy();
  });

  it("serializes save snapshots, preserves later local metadata, and lets a failed save release the queue", async () => {
    let resolveFirst!: (workflow: Workflow) => void;
    const calls: Workflow[] = [];
    const save = vi.fn((workflow: Workflow) => {
      calls.push(workflow);
      return calls.length === 1
        ? new Promise<Workflow>(resolve => {
            resolveFirst = resolve;
          })
        : Promise.resolve(workflow);
    });
    const { session } = build(initial(), { save });
    const first = session.save();
    session.rename("New name");
    session.setProperties("a", { text: "later" });
    const second = session.save();
    await vi.waitFor(() => expect(calls).toHaveLength(1));
    expect(calls[0].content.operators[0].operatorProperties.text).toBe("日本語");
    resolveFirst({ ...calls[0], name: "stale" });
    await Promise.all([first, second]);
    expect(calls).toHaveLength(2);
    expect(session.getSnapshot().workflow.name).toBe("New name");
    expect(calls[1].content.operators[0].operatorProperties.text).toBe("later");
    save.mockRejectedValueOnce(new Error("offline"));
    await expect(session.save()).rejects.toThrow("offline");
    await expect(session.save()).resolves.toBeDefined();
  });

  it("debounces autosaves and cancels pending autosaves on disposal", async () => {
    vi.useFakeTimers();
    const { session, save } = build(initial(), { autosaveMs: 150 });
    session.setProperties("a", { text: "one" });
    session.setProperties("a", { text: "two" });
    await vi.advanceTimersByTimeAsync(149);
    expect(save).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(save).toHaveBeenCalledTimes(1);
    session.setProperties("a", { text: "three" });
    session.dispose();
    await vi.advanceTimersByTimeAsync(500);
    expect(save).toHaveBeenCalledTimes(1);
  });

  it("holds database metadata until room sync, honoring room clears and local edits made before sync", () => {
    const shared = model();
    shared.wsProvider.shouldConnect = true;
    const { session } = build(initial(), { shared });
    expect(shared.contentMetaMap.has("settings")).toBe(false);
    expect(session.getSnapshot().workflow.content.settings.dataTransferBatchSize).toBe(400);
    shared.contentMetaMap.set("settings", { dataTransferBatchSize: 777, executionMode: ExecutionMode.MATERIALIZED });
    session.setFormBinding({ fields: [], instruction: { body: "Local edit" } });
    shared.contentMetaMap.set("formBinding", null);
    shared.wsProvider.synced = true;
    expect(session.getSnapshot().workflow.content.settings.dataTransferBatchSize).toBe(777);
    expect(session.getSnapshot().workflow.content.formBinding?.instruction?.body).toBe("Local edit");
  });

  it("does not mark a metadata edit made during an older save as saved", async () => {
    let resolve!: (workflow: Workflow) => void;
    const { session } = build(initial(), {
      save: workflow =>
        new Promise(done => {
          resolve = () => done(workflow);
        }),
    });
    const saving = session.save();
    await vi.waitFor(() => expect(resolve).toBeDefined());
    session.rename("Later name");
    resolve(initial());
    await saving;
    expect(session.getSnapshot().saveState).toBe("unsaved");
  });
  it("lets a pending autosave persist the graph after execution locks further edits", async () => {
    const { session, save } = build();
    session.setProperties("a", { text: "Ready" });
    session.setExecutionLocked(true);
    await session.save();
    expect(save).toHaveBeenCalledTimes(1);
    expect(session.getSnapshot().canEdit).toBe(false);
    expect(() => session.setProperties("a", {})).toThrow(/execution/i);
  });
});
