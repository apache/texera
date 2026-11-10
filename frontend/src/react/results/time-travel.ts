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

import type { ApiClient } from "../core/api-client";
import type { ReplayExecutionInfo } from "../../core/workspace/types/workflow-websocket.interface";
import type { WorkflowSession } from "../workspace/workflow-session";
import type { WorkflowRuntime } from "../workspace/workflow-runtime";
import { WorkflowRepository } from "../workflows/workflow-repository";
import type { WorkflowExecutionsEntry } from "./results-repository";
export interface TimeTravelSnapshot {
  loading: boolean;
  busy: boolean;
  entries: readonly WorkflowExecutionsEntry[];
  interactions: Readonly<Record<number, readonly string[]>>;
  replay?: ReplayExecutionInfo;
  error?: string;
}
function identifier(id: number, label: string): void {
  if (!Number.isSafeInteger(id) || id < 0) throw new Error("Invalid " + label + " identifier.");
}
const IDLE = new Set(["Uninitialized", "Completed", "Killed", "Terminated", "Failed"]);
function message(reason: unknown) {
  return reason instanceof Error ? reason.message : "Time travel failed.";
}
/** A temporary server snapshot in the existing session; returning never saves the historical graph. */
export class TimeTravelController {
  private snapshot: TimeTravelSnapshot = { loading: false, busy: false, entries: [], interactions: {} };
  private listeners = new Set<() => void>();
  private previewActive = false;
  private disposed = false;
  private generation = 0;
  private controller?: AbortController;
  private pendingRefresh?: Promise<void>;
  private readonly stopRuntime: () => void;
  private cancelWait?: () => void;
  constructor(
    private readonly api: ApiClient,
    private readonly session: WorkflowSession,
    private readonly runtime: WorkflowRuntime
  ) {
    this.stopRuntime = runtime.subscribe(() => {
      if (this.previewActive) this.session.setExecutionLocked(true, "replay");
    });
  }
  getSnapshot = () => this.snapshot;
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  refresh(): Promise<void> {
    if (this.pendingRefresh) return this.pendingRefresh;
    const wid = this.session.getSnapshot().workflow.wid;
    if (wid === undefined) return Promise.resolve();
    identifier(wid, "workflow");
    this.update({ loading: true });
    const controller = new AbortController();
    this.controller = controller;
    const pending = this.api
      .get<WorkflowExecutionsEntry[]>("executions/" + wid, { signal: controller.signal })
      .then(entries => {
        if (!Array.isArray(entries)) throw new Error("Malformed execution history.");
        if (!this.disposed && !controller.signal.aborted)
          this.update({
            entries: entries.filter(entry => typeof entry.logLocation === "string" && entry.logLocation.length > 0),
            loading: false,
            error: undefined,
          });
      })
      .catch(reason => {
        if (!controller.signal.aborted && !this.disposed) {
          this.update({ loading: false, error: message(reason) });
          throw reason;
        }
      })
      .finally(() => {
        if (this.pendingRefresh === pending) this.pendingRefresh = undefined;
      });
    this.pendingRefresh = pending;
    return pending;
  }
  async interactions(eid: number): Promise<readonly string[]> {
    identifier(eid, "execution");
    const wid = this.session.getSnapshot().workflow.wid;
    if (wid === undefined) throw new Error("Save this workflow before reading interactions.");
    const values = await this.api.get<unknown>("executions/" + wid + "/interactions/" + eid);
    if (!Array.isArray(values) || values.some(value => typeof value !== "string" || !value.trim()))
      throw new Error("Malformed execution interaction history.");
    this.update({ interactions: { ...this.snapshot.interactions, [eid]: values } });
    return values;
  }
  async replay(entry: WorkflowExecutionsEntry, interaction: string): Promise<void> {
    identifier(entry.eId, "execution");
    identifier(entry.vId, "version");
    if (typeof interaction !== "string" || !interaction.trim()) throw new Error("Select an execution interaction.");
    if (this.disposed) throw new Error("Time travel is closed.");
    if (this.snapshot.busy) throw new Error("A time travel request is already running.");
    if (!this.previewActive && !IDLE.has(this.runtime.getSnapshot().state))
      throw new Error("Stop the current execution before replaying history.");
    const generation = ++this.generation;
    this.update({ busy: true, error: undefined });
    try {
      if (this.previewActive) await this.stopReplay();
      const workflow = this.session.getSnapshot().workflow;
      if (workflow.wid === undefined) throw new Error("Save this workflow before replaying history.");
      if (!this.previewActive) {
        this.previewActive = true;
        this.session.setExecutionLocked(true, "replay");
        if (!workflow.readonly) await this.session.save();
        if (this.disposed || generation !== this.generation) return;
        this.session.setAutosaveSuspended(true, "replay");
      }
      const version = await new WorkflowRepository(this.api).getVersion(workflow.wid, entry.vId);
      if (this.disposed || generation !== this.generation) return;
      this.session.enterPreview(version.content);
      this.session.setExecutionLocked(true, "replay");
      const replay = { eid: entry.eId, interaction };
      this.update({ replay });
      await this.runtime.run({
        replayFromExecution: replay,
        executionName: "Replay run of " + entry.eId + " to " + interaction,
      });
      if (!this.disposed && generation === this.generation) this.update({ busy: false });
    } catch (reason) {
      if (!this.disposed && generation === this.generation) {
        this.restore();
        this.update({ busy: false, replay: undefined, error: message(reason) });
      }
      throw reason;
    }
  }
  async exit(): Promise<void> {
    ++this.generation;
    this.update({ busy: true, error: undefined });
    try {
      if (this.previewActive) await this.stopReplay();
    } catch (reason) {
      this.update({ error: message(reason) });
    } finally {
      this.restore();
      this.update({ busy: false, replay: undefined });
    }
  }
  dispose(): void {
    if (this.disposed) return;
    ++this.generation;
    this.cancelWait?.();
    this.controller?.abort();
    this.stopRuntime();
    if (this.previewActive && !IDLE.has(this.runtime.getSnapshot().state)) {
      try {
        this.runtime.kill();
      } catch {
        /* The runtime may already be closing. */
      }
    }
    this.restore();
    this.disposed = true;
    this.listeners.clear();
  }
  private async stopReplay(): Promise<void> {
    if (IDLE.has(this.runtime.getSnapshot().state)) return;
    this.runtime.kill();
    if (IDLE.has(this.runtime.getSnapshot().state)) return;
    await new Promise<void>((resolve, reject) => {
      let stop: () => void = () => undefined;
      const clean = () => {
        clearTimeout(timeout);
        stop();
        this.cancelWait = undefined;
      };
      const check = () => {
        if (IDLE.has(this.runtime.getSnapshot().state)) {
          clean();
          resolve();
        }
      };
      const timeout = setTimeout(() => {
        clean();
        reject(new Error("The replay did not stop. Wait for the computing unit to finish."));
      }, 20000);
      this.cancelWait = () => {
        clean();
        reject(new Error("Time travel was closed."));
      };
      stop = this.runtime.subscribe(check);
      check();
    });
  }
  private restore(): void {
    if (!this.previewActive) return;
    this.previewActive = false;
    try {
      this.session.exitPreview();
    } finally {
      this.session.setAutosaveSuspended(false, "replay");
      this.session.setExecutionLocked(false, "replay");
    }
  }
  private update(patch: Partial<TimeTravelSnapshot>) {
    if (this.disposed) return;
    this.snapshot = { ...this.snapshot, ...patch };
    this.listeners.forEach(listener => listener());
  }
}
