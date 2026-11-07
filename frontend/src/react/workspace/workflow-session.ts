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

import {
  SharedModel,
  type ContentMetaKey,
  type ContentMetaValue,
} from "../../core/workspace/service/workflow-graph/model/shared-model";
import * as Y from "yjs";
import { WorkflowGraph } from "../../core/workspace/service/workflow-graph/model/workflow-graph";
import { updateYTypeFromObject } from "../../core/workspace/types/shared-editing.interface";
import type { FormBindingConfig, Workflow, WorkflowContent, WorkflowSettings } from "../../core/common/type/workflow";
import { ExecutionMode } from "../../core/common/type/workflow";
import type {
  CommentBox,
  Comment,
  OperatorLink,
  OperatorPredicate,
  Point,
} from "../../core/workspace/types/workflow-common.interface";

export interface WorkflowSnapshot {
  /** Identity of the active document; views must rebind their Yjs observers when it changes. */
  documentId: string;
  previewing: boolean;
  workflow: Workflow;
  saveState: "saved" | "unsaved" | "saving" | "error";
  error?: string;
  canUndo: boolean;
  canRedo: boolean;
  canEdit: boolean;
  autosaveSuspended: boolean;
}
export interface WorkflowSessionOptions {
  shared?: SharedModel;
  save: (workflow: Workflow) => Promise<Workflow>;
  autosaveMs?: number;
  defaultSettings?: WorkflowSettings;
}

/** Owns a workflow session across canvas/form views; all graph semantics remain in WorkflowGraph. */
export class WorkflowSession {
  private readonly liveGraph: WorkflowGraph;
  private readonly liveShared: SharedModel;
  private preview?: { graph: WorkflowGraph; shared: SharedModel };
  get graph(): WorkflowGraph {
    return this.preview?.graph ?? this.liveGraph;
  }
  get shared(): SharedModel {
    return this.preview?.shared ?? this.liveShared;
  }
  private metadata: Omit<Workflow, "content">;
  private readonly listeners = new Set<() => void>();
  private snapshot: WorkflowSnapshot;
  private liveSaveState: WorkflowSnapshot["saveState"] = "saved";
  private liveError?: string;
  private autosaveTimer?: ReturnType<typeof setTimeout>;
  private queue: Promise<unknown> = Promise.resolve();
  private disposed = false;
  private executionLocked = false;
  private autosaveSuspended = false;
  private readonly autosaveOwners = new Set<string>();
  private readonly editLockOwners = new Set<string>();
  private applyingRemote = false;
  private gesture = false;
  private captureTimeout = 100;
  private pending = 0;
  private revision = 0;
  private readonly pendingMeta = new Map<ContentMetaKey, { value: ContentMetaValue | null; edit: boolean }>();
  private readonly onFirstSync = (synced: boolean) => {
    if (!synced || this.disposed) return;
    this.liveShared.wsProvider.off("sync", this.onFirstSync);
    this.liveShared.yDoc.transact(() => {
      for (const [key, pending] of this.pendingMeta) {
        if (pending.edit || !this.liveShared.contentMetaMap.has(key))
          this.liveShared.contentMetaMap.set(key, pending.value);
      }
      this.pendingMeta.clear();
    });
    this.refresh(this.liveSaveState, this.liveError);
  };
  private readonly afterTransaction = () => this.changed();
  private readonly afterPreviewTransaction = () => this.refresh(this.liveSaveState, this.liveError);

  constructor(
    workflow: Workflow,
    private readonly options: WorkflowSessionOptions
  ) {
    this.validate(workflow);
    const { content, ...metadata } = structuredClone(workflow);
    this.metadata = metadata;
    this.liveShared = options.shared ?? new SharedModel(workflow.wid);
    this.liveGraph = new WorkflowGraph([], [], [], this.liveShared);
    this.shared.yDoc.transact(() => {
      // Existing room data takes precedence. Map presence is meaningful even for cleared metadata.
      for (const operator of content.operators) {
        if (!this.graph.hasOperator(operator.operatorID)) {
          this.graph.addOperator(operator);
          this.shared.elementPositionMap.set(operator.operatorID, content.operatorPositions[operator.operatorID]);
        }
      }
      for (const link of content.links) if (!this.graph.hasLinkWithID(link.linkID)) this.graph.addLink(link);
      for (const comment of content.commentBoxes)
        if (!this.graph.hasCommentBox(comment.commentBoxID)) this.graph.addCommentBox(comment);
      this.seedMeta("settings", content.settings);
      this.seedMeta("formBinding", content.formBinding ?? null);
    });
    this.shared.undoManager.clear();
    this.snapshot = {
      documentId: this.liveShared.yDoc.guid,
      previewing: false,
      workflow: this.currentWorkflow(),
      saveState: "saved",
      canUndo: false,
      canRedo: false,
      canEdit: !metadata.readonly,
      autosaveSuspended: false,
    };
    this.shared.yDoc.on("afterTransaction", this.afterTransaction);
    if (this.pendingMeta.size) this.shared.wsProvider.on("sync", this.onFirstSync);
  }

  getSnapshot = (): WorkflowSnapshot => this.snapshot;
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  /** Historical content belongs to a temporary, unconnected document, never to the live room. */
  enterPreview(content: WorkflowContent): void {
    if (this.disposed) throw new Error("Workflow session is closed.");
    const incoming = structuredClone(content);
    this.validate({ ...this.metadata, content: incoming });
    const shared = new SharedModel(undefined, this.liveShared.user, undefined, { connect: false });
    const graph = new WorkflowGraph([], [], [], shared);
    try {
      shared.yDoc.transact(() => {
        for (const operator of incoming.operators) {
          graph.addOperator(operator);
          shared.elementPositionMap.set(operator.operatorID, incoming.operatorPositions[operator.operatorID]);
        }
        for (const link of incoming.links) graph.addLink(link);
        for (const box of incoming.commentBoxes) graph.addCommentBox(box);
        shared.contentMetaMap.set("settings", incoming.settings ?? null);
        shared.contentMetaMap.set("formBinding", incoming.formBinding ?? null);
      });
      shared.undoManager.clear();
    } catch (error) {
      this.destroyShared(shared);
      throw error;
    }
    this.endGesture();
    if (this.autosaveTimer) clearTimeout(this.autosaveTimer);
    this.autosaveTimer = undefined;
    const previous = this.preview;
    this.preview = { graph, shared };
    this.autosaveSuspended = true;
    shared.yDoc.on("afterTransaction", this.afterPreviewTransaction);
    this.refresh(this.liveSaveState, this.liveError);
    if (previous) {
      previous.shared.yDoc.off("afterTransaction", this.afterPreviewTransaction);
      this.destroyShared(previous.shared);
    }
  }

  /** Reveal the current live room, including edits received while history was open. No CRDT updates are merged. */
  exitPreview(): void {
    const previous = this.preview;
    if (!previous || this.disposed) return;
    previous.shared.yDoc.off("afterTransaction", this.afterPreviewTransaction);
    this.preview = undefined;
    this.autosaveSuspended = this.autosaveOwners.size > 0;
    this.refresh(this.liveSaveState, this.liveError);
    this.destroyShared(previous.shared);
    if (this.liveSaveState === "unsaved") this.scheduleAutosave();
  }

  addOperator(operator: OperatorPredicate, position: Point): void {
    this.position(position);
    this.mutate(() => {
      this.graph.addOperator(operator);
      this.shared.elementPositionMap.set(operator.operatorID, position);
    });
  }
  moveOperator(id: string, position: Point): void {
    this.position(position);
    this.mutate(() => {
      this.graph.assertOperatorExists(id);
      this.shared.elementPositionMap.set(id, position);
    });
  }
  deleteOperators(ids: readonly string[]): void {
    this.editable();
    ids.forEach(id => this.graph.assertOperatorExists(id));
    this.mutate(() => {
      const deleted = new Set(ids);
      this.graph
        .getAllLinks()
        .filter(link => deleted.has(link.source.operatorID) || deleted.has(link.target.operatorID))
        .forEach(link => this.graph.deleteLinkWithID(link.linkID));
      ids.forEach(id => {
        this.graph.deleteOperator(id);
        this.shared.elementPositionMap.delete(id);
      });
    });
  }
  addLink(link: OperatorLink): void {
    this.mutate(() => this.graph.addLink(link));
  }
  deleteLink(id: string): void {
    this.mutate(() => this.graph.deleteLinkWithID(id));
  }
  setProperties(id: string, properties: Record<string, unknown>): void {
    this.mutate(() => this.graph.setOperatorProperty(id, properties));
  }
  setDisplayName(id: string, name: string): void {
    this.mutate(() => {
      const operator = this.graph.getSharedOperatorType(id);
      const previous = operator.get("customDisplayName");
      if (previous instanceof Y.Text) {
        previous.delete(0, previous.length);
        previous.insert(0, name);
      } else operator.set("customDisplayName", new Y.Text(name));
    });
  }
  setDisabled(id: string, disabled: boolean): void {
    this.mutate(() => this.graph.getSharedOperatorType(id).set("isDisabled", disabled));
  }
  setViewResult(id: string, view: boolean): void {
    this.mutate(() => this.graph.getSharedOperatorType(id).set("viewResult", view));
  }
  addComment(comment: CommentBox): void {
    this.position(comment.commentBoxPosition);
    this.mutate(() => this.graph.addCommentBox(comment));
  }
  moveComment(id: string, position: Point): void {
    this.position(position);
    this.mutate(() => {
      this.graph.assertCommentBoxExists(id);
      this.shared.commentBoxMap.get(id)!.set("commentBoxPosition", position);
    });
  }
  editComment(id: string, creatorId: number, creationTime: string, content: string): void {
    this.mutate(() => this.changeComment(id, creatorId, creationTime, content));
  }
  replyToComment(id: string, comment: Comment): void {
    this.mutate(() => this.graph.addCommentToCommentBox(comment, id));
  }
  deleteComment(id: string, creatorId: number, creationTime: string): void {
    this.mutate(() => this.changeComment(id, creatorId, creationTime));
  }
  deleteCommentBox(id: string): void {
    this.mutate(() => this.graph.deleteCommentBox(id));
  }
  transaction(action: () => void): void {
    this.mutate(action);
  }
  beginGesture(): void {
    this.editable();
    if (this.gesture) return;
    this.shared.undoManager.stopCapturing();
    this.captureTimeout = this.shared.undoManager.captureTimeout;
    this.shared.undoManager.captureTimeout = Number.POSITIVE_INFINITY;
    this.gesture = true;
  }
  endGesture(): void {
    if (!this.gesture) return;
    this.gesture = false;
    this.shared.undoManager.captureTimeout = this.captureTimeout;
    this.shared.undoManager.stopCapturing();
  }
  isAutosaveSuspended(owner?: string): boolean {
    return owner === undefined ? this.autosaveSuspended : this.autosaveOwners.has(owner);
  }
  setAutosaveSuspended(suspended: boolean, owner = "legacy"): void {
    if (suspended) this.autosaveOwners.add(owner);
    else this.autosaveOwners.delete(owner);
    this.autosaveSuspended = this.preview !== undefined || this.autosaveOwners.size > 0;
    if (this.autosaveTimer) clearTimeout(this.autosaveTimer);
    this.autosaveTimer = undefined;
    this.refresh(this.liveSaveState, this.liveError);
    if (!this.autosaveSuspended && this.snapshot.saveState === "unsaved") this.scheduleAutosave();
  }
  /** Agent events carry a server-persisted snapshot. Keep the current room and shared text identities. */
  applyRemoteWorkflow(content: WorkflowContent): void {
    if (this.disposed) throw new Error("Workflow session is closed.");
    const shared = this.liveShared,
      graph = this.liveGraph;
    const incoming = structuredClone(content);
    this.validate({ ...this.metadata, content: incoming });
    const operators = new Set(incoming.operators.map(operator => operator.operatorID));
    const comments = new Set(incoming.commentBoxes.map(box => box.commentBoxID));
    this.applyingRemote = true;
    try {
      shared.yDoc.transact(() => {
        graph.getAllLinks().forEach(link => graph.deleteLinkWithID(link.linkID));
        graph
          .getAllOperators()
          .filter(operator => !operators.has(operator.operatorID))
          .forEach(operator => {
            graph.deleteOperator(operator.operatorID);
            shared.elementPositionMap.delete(operator.operatorID);
          });
        incoming.operators.forEach(operator => {
          if (graph.hasOperator(operator.operatorID))
            updateYTypeFromObject(graph.getSharedOperatorType(operator.operatorID), operator);
          else graph.addOperator(operator);
          shared.elementPositionMap.set(operator.operatorID, incoming.operatorPositions[operator.operatorID]);
        });
        incoming.links.forEach(link => graph.addLink(link));
        graph
          .getAllCommentBoxes()
          .filter(box => !comments.has(box.commentBoxID))
          .forEach(box => graph.deleteCommentBox(box.commentBoxID));
        incoming.commentBoxes.forEach(box => {
          const previous = shared.commentBoxMap.get(box.commentBoxID);
          if (previous) updateYTypeFromObject(previous, box);
          else graph.addCommentBox(box);
        });
        this.pendingMeta.clear();
        shared.contentMetaMap.set("settings", incoming.settings ?? null);
        shared.contentMetaMap.set("formBinding", incoming.formBinding ?? null);
      }, "server-snapshot");
      ++this.revision;
      if (this.autosaveTimer) clearTimeout(this.autosaveTimer);
      this.autosaveTimer = undefined;
      this.refresh("saved");
    } finally {
      this.applyingRemote = false;
    }
  }
  private changeComment(id: string, creatorId: number, creationTime: string, content?: string): void {
    this.graph.assertCommentBoxExists(id);
    const comments = this.shared.commentBoxMap.get(id)!.get("comments");
    const index = comments.toArray().findIndex(item => {
      const value = item instanceof Y.Map ? item.toJSON() : item;
      return value.creatorID === creatorId && value.creationTime === creationTime;
    });
    if (index < 0) throw new Error("The comment no longer exists.");
    const original = comments.get(index);
    const value = original instanceof Y.Map ? original.toJSON() : original;
    comments.delete(index, 1);
    if (content !== undefined) comments.insert(index, [{ ...value, content }]);
  }
  setSettings(settings: WorkflowSettings): void {
    this.writeMeta("settings", settings);
  }
  setFormBinding(binding: FormBindingConfig): void {
    this.writeMeta("formBinding", binding);
  }
  rename(name: string): void {
    this.editable();
    if (!name.trim()) throw new Error("Workflow name must not be empty.");
    this.metadata = { ...this.metadata, name: name.trim() };
    this.changed();
  }
  setDescription(description: string): void {
    this.editable();
    this.metadata = { ...this.metadata, description };
    this.changed();
  }
  setExecutionLocked(locked: boolean, owner = "execution"): void {
    if (locked) this.editLockOwners.add(owner);
    else this.editLockOwners.delete(owner);
    this.executionLocked = this.editLockOwners.size > 0;
    this.refresh(this.liveSaveState, this.liveError);
  }
  undo(): void {
    this.editable();
    this.shared.undoManager.undo();
    this.refresh(this.liveSaveState, this.liveError);
  }
  redo(): void {
    this.editable();
    this.shared.undoManager.redo();
    this.refresh(this.liveSaveState, this.liveError);
  }

  save(): Promise<Workflow> {
    if (this.autosaveSuspended)
      return Promise.reject(new Error("Saving is paused while the workflow is being updated."));
    if (this.disposed) throw new Error("Workflow session is closed.");
    if (this.metadata.readonly) throw new Error("This workflow is read-only.");
    if (this.autosaveTimer) clearTimeout(this.autosaveTimer);
    this.autosaveTimer = undefined;
    const payload = structuredClone(this.currentWorkflow());
    const revision = this.revision;
    ++this.pending;
    this.refresh("saving");
    const result = this.queue
      .then(() => this.options.save(payload))
      .then(
        updated => {
          // Responses describe the snapshot sent, while name/description/content can have newer edits.
          this.metadata = {
            ...this.metadata,
            wid: updated.wid,
            creationTime: updated.creationTime,
            lastModifiedTime: updated.lastModifiedTime,
          };
          --this.pending;
          if (!this.disposed) this.refresh(this.pending ? "saving" : revision === this.revision ? "saved" : "unsaved");
          return updated;
        },
        error => {
          --this.pending;
          if (!this.disposed)
            this.refresh("error", error instanceof Error ? error.message : "Workflow could not be saved.");
          throw error;
        }
      );
    this.queue = result.catch(() => undefined);
    return result;
  }
  whenSavesDrained(): Promise<void> {
    return this.queue.then(() => undefined);
  }
  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    if (this.autosaveTimer) clearTimeout(this.autosaveTimer);
    this.liveShared.yDoc.off("afterTransaction", this.afterTransaction);
    this.liveShared.wsProvider.off("sync", this.onFirstSync);
    this.pendingMeta.clear();
    this.listeners.clear();
    if (this.preview) {
      this.preview.shared.yDoc.off("afterTransaction", this.afterPreviewTransaction);
      this.destroyShared(this.preview.shared);
      this.preview = undefined;
    }
    this.destroyShared(this.liveShared);
  }

  private destroyShared(shared: SharedModel): void {
    shared.undoManager.destroy();
    shared.destroy();
  }

  private mutate(action: () => void): void {
    this.editable();
    if (!this.gesture) this.shared.undoManager.stopCapturing();
    this.shared.yDoc.transact(action);
    if (!this.gesture) this.shared.undoManager.stopCapturing();
    this.refresh(this.liveSaveState, this.liveError);
  }
  private editable(): void {
    if (this.disposed) throw new Error("Workflow session is closed.");
    if (this.preview) throw new Error("The workflow is showing a read-only preview.");
    if (this.metadata.readonly) throw new Error("This workflow is read-only.");
    if (this.executionLocked) throw new Error("The current execution locks workflow editing.");
  }
  private changed(): void {
    if (this.disposed || this.applyingRemote) return;
    ++this.revision;
    this.refresh("unsaved");
    this.scheduleAutosave();
  }
  private scheduleAutosave(): void {
    if (!this.disposed && !this.autosaveSuspended && !this.metadata.readonly && (this.options.autosaveMs ?? 1000) > 0) {
      if (this.autosaveTimer) clearTimeout(this.autosaveTimer);
      this.autosaveTimer = setTimeout(() => {
        void this.save().catch(() => undefined);
      }, this.options.autosaveMs ?? 1000);
    }
  }
  private currentWorkflow(): Workflow {
    const settings = (this.readMeta("settings") as WorkflowSettings | null | undefined) ??
      this.options.defaultSettings ?? { dataTransferBatchSize: 400, executionMode: ExecutionMode.PIPELINED };
    const binding = this.readMeta("formBinding") as FormBindingConfig | null | undefined;
    return {
      ...this.metadata,
      ...(this.preview ? { readonly: true } : {}),
      content: {
        operators: this.graph.getAllOperators(),
        operatorPositions: this.shared.elementPositionMap.toJSON(),
        links: this.graph.getAllLinks(),
        commentBoxes: this.graph.getAllCommentBoxes(),
        settings,
        ...(binding ? { formBinding: binding } : {}),
      },
    };
  }
  private refresh(saveState: WorkflowSnapshot["saveState"], error?: string): void {
    this.liveSaveState = saveState;
    this.liveError = error;
    this.snapshot = {
      documentId: this.shared.yDoc.guid,
      previewing: this.preview !== undefined,
      workflow: this.currentWorkflow(),
      saveState,
      error,
      canUndo: !this.preview && this.liveShared.undoManager.undoStack.length > 0,
      canRedo: !this.preview && this.liveShared.undoManager.redoStack.length > 0,
      canEdit: !this.preview && !this.metadata.readonly && !this.executionLocked,
      autosaveSuspended: this.autosaveSuspended,
    };
    this.listeners.forEach(listener => listener());
  }
  private seedMeta(key: ContentMetaKey, value: ContentMetaValue | null): void {
    if (this.shared.wsProvider.shouldConnect && !this.shared.wsProvider.synced)
      this.pendingMeta.set(key, { value, edit: false });
    else if (!this.shared.contentMetaMap.has(key)) this.shared.contentMetaMap.set(key, value);
  }
  private readMeta(key: ContentMetaKey): ContentMetaValue | null | undefined {
    if (this.preview) return this.preview.shared.contentMetaMap.get(key);
    const pending = this.pendingMeta.get(key);
    if (pending?.edit) return pending.value;
    return this.shared.contentMetaMap.has(key) ? this.shared.contentMetaMap.get(key) : pending?.value;
  }
  private writeMeta(key: ContentMetaKey, value: ContentMetaValue): void {
    this.editable();
    if (this.shared.wsProvider.shouldConnect && !this.shared.wsProvider.synced) {
      this.pendingMeta.set(key, { value, edit: true });
      this.changed();
    } else this.mutate(() => this.shared.contentMetaMap.set(key, value));
  }
  private validate(workflow: Workflow): void {
    if (
      !workflow?.content ||
      !Array.isArray(workflow.content.operators) ||
      !Array.isArray(workflow.content.links) ||
      !Array.isArray(workflow.content.commentBoxes)
    )
      throw new Error("Malformed workflow content.");
    const ids = new Set<string>();
    for (const operator of workflow.content.operators) {
      if (typeof operator?.operatorID !== "string" || !operator.operatorID || ids.has(operator.operatorID))
        throw new Error("Invalid or duplicate operator identifier.");
      if (
        typeof operator.operatorType !== "string" ||
        !operator.operatorType ||
        !operator.operatorProperties ||
        typeof operator.operatorProperties !== "object" ||
        Array.isArray(operator.operatorProperties)
      )
        throw new Error("Malformed workflow operator.");
      for (const ports of [operator.inputPorts, operator.outputPorts])
        if (
          !Array.isArray(ports) ||
          ports.some(port => typeof port?.portID !== "string" || !port.portID) ||
          new Set(ports.map(port => port.portID)).size !== ports.length
        )
          throw new Error("Malformed workflow operator ports.");
      ids.add(operator.operatorID);
      this.position(workflow.content.operatorPositions?.[operator.operatorID]);
    }
    const linkIds = new Set<string>();
    const operators = new Map(workflow.content.operators.map(operator => [operator.operatorID, operator]));
    for (const link of workflow.content.links) {
      const source = operators.get(link?.source?.operatorID),
        target = operators.get(link?.target?.operatorID);
      if (
        typeof link?.linkID !== "string" ||
        !link.linkID ||
        linkIds.has(link.linkID) ||
        !source ||
        !target ||
        !source.outputPorts.some(port => port.portID === link.source.portID) ||
        !target.inputPorts.some(port => port.portID === link.target.portID)
      )
        throw new Error("Malformed workflow link.");
      linkIds.add(link.linkID);
    }
    const commentIds = new Set<string>();
    for (const box of workflow.content.commentBoxes) {
      if (
        typeof box?.commentBoxID !== "string" ||
        !box.commentBoxID ||
        commentIds.has(box.commentBoxID) ||
        !Array.isArray(box.comments) ||
        box.comments.some(
          item =>
            typeof item?.content !== "string" ||
            !Number.isFinite(item.creatorID) ||
            typeof item.creatorName !== "string" ||
            typeof item.creationTime !== "string"
        )
      )
        throw new Error("Malformed workflow comment.");
      commentIds.add(box.commentBoxID);
      this.position(box.commentBoxPosition);
    }
    const settings = workflow.content.settings;
    if (
      settings != null &&
      (!Number.isSafeInteger(settings.dataTransferBatchSize) ||
        settings.dataTransferBatchSize <= 0 ||
        !Object.values(ExecutionMode).includes(settings.executionMode))
    )
      throw new Error("Malformed workflow settings.");
  }
  private position(point: Point | undefined): void {
    if (!point || !Number.isFinite(point.x) || !Number.isFinite(point.y))
      throw new Error("Operator position is missing or invalid.");
  }
}
