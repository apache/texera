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
import { SharedModel } from "../../app/workspace/service/workflow-graph/model/shared-model";
import { ExecutionMode, type Workflow } from "../../app/common/type/workflow";
import type { OperatorPredicate } from "../../app/workspace/types/workflow-common.interface";

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
