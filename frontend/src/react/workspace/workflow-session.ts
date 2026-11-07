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
} from "../../app/workspace/service/workflow-graph/model/shared-model";
import * as Y from "yjs";
import { WorkflowGraph } from "../../app/workspace/service/workflow-graph/model/workflow-graph";
import type { FormBindingConfig, Workflow, WorkflowSettings } from "../../app/common/type/workflow";
import type {
  CommentBox,
  OperatorLink,
  OperatorPredicate,
  Point,
} from "../../app/workspace/types/workflow-common.interface";

export interface WorkflowSnapshot {
  workflow: Workflow;
  saveState: "saved" | "unsaved" | "saving" | "error";
  error?: string;
  canUndo: boolean;
  canRedo: boolean;
  canEdit: boolean;
}
export interface WorkflowSessionOptions {
  shared?: SharedModel;
  save: (workflow: Workflow) => Promise<Workflow>;
  autosaveMs?: number;
}

/** Owns a workflow session across canvas/form views; all graph semantics remain in WorkflowGraph. */
export class WorkflowSession {
  readonly graph: WorkflowGraph;
  readonly shared: SharedModel;
  private metadata: Omit<Workflow, "content">;
  private readonly listeners = new Set<() => void>();
  private snapshot: WorkflowSnapshot;
  private autosaveTimer?: ReturnType<typeof setTimeout>;
  private queue: Promise<unknown> = Promise.resolve();
  private disposed = false;
  private executionLocked = false;
  private pending = 0;
  private revision = 0;
  private readonly pendingMeta = new Map<ContentMetaKey, { value: ContentMetaValue | null; edit: boolean }>();
  private readonly onFirstSync = (synced: boolean) => {
    if (!synced || this.disposed) return;
    this.shared.wsProvider.off("sync", this.onFirstSync);
    this.shared.yDoc.transact(() => {
      for (const [key, pending] of this.pendingMeta) {
        if (pending.edit || !this.shared.contentMetaMap.has(key)) this.shared.contentMetaMap.set(key, pending.value);
      }
      this.pendingMeta.clear();
    });
    this.refresh(this.snapshot.saveState);
  };
  private readonly afterTransaction = () => this.changed();

  constructor(
    workflow: Workflow,
    private readonly options: WorkflowSessionOptions
  ) {
    this.validate(workflow);
    const { content, ...metadata } = structuredClone(workflow);
    this.metadata = metadata;
    this.shared = options.shared ?? new SharedModel(workflow.wid);
    this.graph = new WorkflowGraph([], [], [], this.shared);
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
      workflow: this.currentWorkflow(),
      saveState: "saved",
      canUndo: false,
      canRedo: false,
      canEdit: !metadata.readonly,
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
    this.mutate(() => this.graph.addCommentBox(comment));
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
  setExecutionLocked(locked: boolean): void {
    this.executionLocked = locked;
    this.refresh(this.snapshot.saveState);
  }
  undo(): void {
    this.editable();
    this.shared.undoManager.undo();
    this.refresh(this.snapshot.saveState);
  }
  redo(): void {
    this.editable();
    this.shared.undoManager.redo();
    this.refresh(this.snapshot.saveState);
  }

  save(): Promise<Workflow> {
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
    this.shared.yDoc.off("afterTransaction", this.afterTransaction);
    this.shared.wsProvider.off("sync", this.onFirstSync);
    this.pendingMeta.clear();
    this.listeners.clear();
    this.shared.wsProvider.destroy();
    this.shared.undoManager.destroy();
    this.shared.destroy();
  }

  private mutate(action: () => void): void {
    this.editable();
    this.shared.undoManager.stopCapturing();
    this.shared.yDoc.transact(action);
    this.shared.undoManager.stopCapturing();
    this.refresh(this.snapshot.saveState);
  }
  private editable(): void {
    if (this.disposed) throw new Error("Workflow session is closed.");
    if (this.metadata.readonly) throw new Error("This workflow is read-only.");
    if (this.executionLocked) throw new Error("The current execution locks workflow editing.");
  }
  private changed(): void {
    if (this.disposed) return;
    ++this.revision;
    this.refresh("unsaved");
    if (!this.metadata.readonly && (this.options.autosaveMs ?? 1000) > 0) {
      if (this.autosaveTimer) clearTimeout(this.autosaveTimer);
      this.autosaveTimer = setTimeout(() => {
        void this.save().catch(() => undefined);
      }, this.options.autosaveMs ?? 1000);
    }
  }
  private currentWorkflow(): Workflow {
    const settings = this.readMeta("settings") as WorkflowSettings;
    const binding = this.readMeta("formBinding") as FormBindingConfig | null | undefined;
    return {
      ...this.metadata,
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
    this.snapshot = {
      workflow: this.currentWorkflow(),
      saveState,
      error,
      canUndo: this.shared.undoManager.undoStack.length > 0,
      canRedo: this.shared.undoManager.redoStack.length > 0,
      canEdit: !this.metadata.readonly && !this.executionLocked,
    };
    this.listeners.forEach(listener => listener());
  }
  private seedMeta(key: ContentMetaKey, value: ContentMetaValue | null): void {
    if (this.shared.wsProvider.shouldConnect && !this.shared.wsProvider.synced)
      this.pendingMeta.set(key, { value, edit: false });
    else if (!this.shared.contentMetaMap.has(key)) this.shared.contentMetaMap.set(key, value);
  }
  private readMeta(key: ContentMetaKey): ContentMetaValue | null | undefined {
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
      !Array.isArray(workflow.content.commentBoxes) ||
      !workflow.content.settings
    )
      throw new Error("Malformed workflow content.");
    const ids = new Set<string>();
    for (const operator of workflow.content.operators) {
      if (!operator?.operatorID || ids.has(operator.operatorID))
        throw new Error("Invalid or duplicate operator identifier.");
      ids.add(operator.operatorID);
      this.position(workflow.content.operatorPositions?.[operator.operatorID]);
    }
  }
  private position(point: Point | undefined): void {
    if (!point || !Number.isFinite(point.x) || !Number.isFinite(point.y))
      throw new Error("Operator position is missing or invalid.");
  }
}
