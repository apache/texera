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

import { useEffect, useMemo, useState } from "react";
import type { ApiClient } from "../core/api-client";
import { WorkflowRepository, parseImportedWorkflow, type WorkflowRecord } from "./workflow-repository";
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import { WorkflowSession } from "../workspace/workflow-session";
function identifier(value: number): void {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error("Invalid workflow or version identifier.");
}
function restoreContent(workflow: WorkflowRecord): WorkflowRecord["content"] {
  const content = parseImportedWorkflow(JSON.stringify(workflow.content)).content;
  const ids = new Set<string>(),
    links = new Set<string>();
  for (const operator of content.operators) {
    if (
      !operator ||
      typeof operator.operatorID !== "string" ||
      !operator.operatorID ||
      ids.has(operator.operatorID) ||
      typeof operator.operatorType !== "string" ||
      !Array.isArray(operator.inputPorts) ||
      !Array.isArray(operator.outputPorts)
    )
      throw new Error("The selected workflow contains invalid operators.");
    const position = content.operatorPositions[operator.operatorID];
    if (!position || !Number.isFinite(position.x) || !Number.isFinite(position.y))
      throw new Error("The selected workflow contains invalid operator positions.");
    ids.add(operator.operatorID);
  }
  for (const link of content.links) {
    if (
      !link ||
      !link.linkID ||
      links.has(link.linkID) ||
      !ids.has(link.source?.operatorID) ||
      !ids.has(link.target?.operatorID)
    )
      throw new Error("The selected workflow contains invalid links.");
    links.add(link.linkID);
  }
  return structuredClone(content);
}
async function target(
  api: ApiClient,
  wid: number,
  versionId: number
): Promise<{ current: WorkflowRecord; historical: WorkflowRecord }> {
  identifier(wid);
  identifier(versionId);
  const repository = new WorkflowRepository(api);
  const [current, historical] = await Promise.all([repository.get(wid), repository.getVersion(wid, versionId)]);
  if (current.wid !== wid || historical.wid !== wid)
    throw new Error("The selected version does not belong to this workflow.");
  // GET /workflow/:wid is the server's live privilege check; a historical version is always a preview.
  if (current.readonly !== false) throw new Error("Restoring a version requires write access to the current workflow.");
  restoreContent(historical);
  return { current, historical };
}
export interface VersionRestoreOptions {
  createSharedModel?: (wid: number) => SharedModel;
  syncTimeoutMs?: number;
}
async function firstSync(shared: SharedModel, timeoutMs: number): Promise<void> {
  if (shared.wsProvider.synced) return;
  await new Promise<void>((resolve, reject) => {
    const done = (synced: boolean) => {
      if (synced) {
        clearTimeout(timeout);
        shared.wsProvider.off("sync", done);
        resolve();
      }
    };
    const timeout = setTimeout(() => {
      shared.wsProvider.off("sync", done);
      reject(
        new Error(
          "The collaborative workflow could not synchronize. Retry when the shared editing connection is available."
        )
      );
    }, timeoutMs);
    shared.wsProvider.on("sync", done);
    done(shared.wsProvider.synced);
  });
}
/** Confirmation joins the live room before mutation, so old room state cannot undo the restored database graph. */
export async function restoreWorkflowVersion(
  api: ApiClient,
  wid: number,
  versionId: number,
  options: VersionRestoreOptions = {}
): Promise<WorkflowRecord> {
  const { historical } = await target(api, wid, versionId),
    content = restoreContent(historical);
  const shared = options.createSharedModel?.(wid) ?? new SharedModel(wid);
  const repository = new WorkflowRepository(api);
  let session: WorkflowSession | undefined, before: WorkflowRecord["content"] | undefined;
  try {
    await firstSync(shared, options.syncTimeoutMs ?? 10000);
    const current = await repository.get(wid);
    if (current.wid !== wid || current.readonly !== false)
      throw new Error("Restoring a version requires write access to the current workflow.");
    session = new WorkflowSession(current, { shared, autosaveMs: 0, save: workflow => repository.persist(workflow) });
    before = structuredClone(session.getSnapshot().workflow.content);
    session.applyRemoteWorkflow(content);
    shared.undoManager.clear();
    try {
      return await repository.persist({ ...current, content });
    } catch (reason) {
      session.applyRemoteWorkflow(before);
      throw reason;
    }
  } finally {
    if (session) session.dispose();
    else {
      shared.wsProvider.destroy();
      shared.undoManager.destroy();
      shared.destroy();
    }
  }
}
export interface VersionRestoreDialogProps extends VersionRestoreOptions {
  api: ApiClient;
  wid: number;
  versionId: number;
  onRestored: (wid: number) => void;
  onClose: () => void;
}
export function VersionRestoreDialog({
  api,
  wid,
  versionId,
  onRestored,
  onClose,
  createSharedModel,
  syncTimeoutMs,
}: VersionRestoreDialogProps) {
  const [preview, setPreview] = useState<{ current: WorkflowRecord; historical: WorkflowRecord }>(),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false),
    [restored, setRestored] = useState(false);
  const key = useMemo(() => ({ wid, versionId }), [wid, versionId]);
  useEffect(() => {
    let active = true;
    setPreview(undefined);
    setError("");
    setRestored(false);
    void target(api, key.wid, key.versionId).then(
      value => {
        if (active) setPreview(value);
      },
      reason => {
        if (active) setError(reason instanceof Error ? reason.message : "Unable to load workflow version.");
      }
    );
    return () => {
      active = false;
    };
  }, [api, key]);
  async function restore() {
    setBusy(true);
    setError("");
    try {
      await restoreWorkflowVersion(api, wid, versionId, { createSharedModel, syncTimeoutMs });
      setRestored(true);
      onRestored(wid);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Unable to restore workflow version.");
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="workflow-modal-backdrop">
      <section
        role="dialog"
        aria-modal="true"
        aria-label="Restore workflow version"
        className="workflow-modal">
        <h2>Restore workflow version {versionId}</h2>
        <p>
          This replaces the current graph, settings and Form View configuration in workflow {wid}. Its name,
          description, sharing and default view stay with the current workflow.
        </p>
        {!preview && !error && <p role="status">Checking current workflow access…</p>}
        {preview && (
          <p>
            Restore {preview.historical.content.operators.length} operator(s) into{" "}
            <strong>{preview.current.name}</strong>?
          </p>
        )}
        {error && <p role="alert">{error}</p>}
        {restored && <p role="status">Workflow version restored.</p>}
        <footer>
          <button
            type="button"
            disabled={!preview || busy || restored}
            onClick={() => void restore()}>
            {busy ? "Restoring…" : "Restore this version"}
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={onClose}>
            Cancel
          </button>
        </footer>
      </section>
    </div>
  );
}
