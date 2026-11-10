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
import type { WorkflowContent } from "../../core/common/type/workflow";
export interface NotebookMapping {
  cell_to_operator: Record<string, string[]>;
  operator_to_cell: Record<string, string[]>;
}
export interface NotebookSnapshot {
  loading: boolean;
  exists: boolean;
  visible: boolean;
  url?: string;
  origin?: string;
  mapping?: NotebookMapping;
  error?: string;
}
function mappingRecord(value: unknown): value is Record<string, string[]> {
  return (
    !!value &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    Object.values(value).every(ids => Array.isArray(ids) && ids.every(id => typeof id === "string"))
  );
}
export function notebookHighlight(
  mapping: NotebookMapping,
  cell: string,
  workflow: Pick<WorkflowContent, "operators" | "links">
): { operators: string[]; links: string[] } {
  const ids = Object.prototype.hasOwnProperty.call(mapping.cell_to_operator, cell)
    ? mapping.cell_to_operator[cell]
    : [];
  const existing = new Set(workflow.operators.map(operator => operator.operatorID)),
    operators = ids.filter(id => existing.has(id)),
    set = new Set(operators);
  return {
    operators,
    links: workflow.links
      .filter(
        link =>
          set.has(link.source.operatorID) &&
          set.has(link.target.operatorID) &&
          link.source.operatorID !== link.target.operatorID
      )
      .map(link => link.linkID),
  };
}
function httpURL(value: unknown): URL {
  if (typeof value !== "string") throw new Error("Jupyter URL is unavailable.");
  const url = new URL(value);
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password)
    throw new Error("Invalid Jupyter URL.");
  return url;
}
export class NotebookClient {
  private snapshot: NotebookSnapshot = { loading: false, exists: false, visible: false };
  private readonly listeners = new Set<() => void>();
  private controller?: AbortController;
  private revision = 0;
  private wid?: number;
  constructor(
    private readonly api: ApiClient,
    private readonly enabled: boolean
  ) {}
  getSnapshot = () => this.snapshot;
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  async load(wid: number | undefined): Promise<void> {
    this.controller?.abort();
    const controller = new AbortController();
    this.controller = controller;
    const revision = ++this.revision;
    this.wid = wid;
    this.replace({ loading: false, exists: false, visible: false });
    if (!this.enabled || !wid) return;
    if (!Number.isSafeInteger(wid) || wid < 0) throw new Error("Invalid workflow identifier.");
    this.replace({ loading: true, exists: false, visible: false });
    const options = { signal: controller.signal };
    try {
      const result = await this.api.post<{ exists: boolean; mapping?: NotebookMapping; notebook?: unknown }>(
        "notebook-migration/fetch-notebook-and-mapping",
        { wid },
        options
      );
      if (revision !== this.revision) return;
      if (!result.exists) {
        this.replace({ loading: false, exists: false, visible: false });
        return;
      }
      if (
        !result.mapping ||
        !mappingRecord(result.mapping.cell_to_operator) ||
        !mappingRecord(result.mapping.operator_to_cell)
      )
        throw new Error("Malformed notebook mapping.");
      if (!result.notebook || typeof result.notebook !== "object") throw new Error("Malformed notebook content.");
      const filename = "notebook_" + wid + ".ipynb";
      await this.api.post(
        "notebook-migration/set-notebook",
        { notebookName: filename, notebookData: result.notebook },
        options
      );
      if (revision !== this.revision) return;
      const iframe = await this.api.get<{ success: boolean; url?: string }>(
        "notebook-migration/get-jupyter-iframe-url?notebookName=" + filename,
        options
      );
      const base = await this.api.get<{ success: boolean; url?: string }>(
        "notebook-migration/get-jupyter-url",
        options
      );
      if (revision !== this.revision) return;
      if (!iframe.success || !base.success) throw new Error("Jupyter server unavailable.");
      const url = httpURL(iframe.url),
        origin = httpURL(base.url).origin;
      if (url.origin !== origin) throw new Error("Jupyter iframe origin does not match the server.");
      this.replace({
        loading: false,
        exists: true,
        visible: true,
        url: url.toString(),
        origin,
        mapping: structuredClone(result.mapping),
      });
    } catch (reason) {
      if (revision !== this.revision || controller.signal.aborted) return;
      this.replace({
        loading: false,
        exists: false,
        visible: false,
        error: reason instanceof Error ? reason.message : "Unable to load notebook.",
      });
      throw reason;
    }
  }
  minimize(): void {
    this.replace({ ...this.snapshot, visible: false });
  }
  open(): void {
    if (this.snapshot.exists && this.snapshot.url) this.replace({ ...this.snapshot, visible: true });
  }
  async delete(): Promise<void> {
    const wid = this.wid;
    if (!this.enabled || !wid) return;
    const response = await this.api.post<{ success: boolean; message?: string }>(
      "notebook-migration/delete-notebook-and-mapping",
      { wid }
    );
    if (!response.success) throw new Error(response.message ?? "Failed to delete uploaded source code.");
    if (this.wid === wid) {
      this.controller?.abort();
      ++this.revision;
      this.replace({ loading: false, exists: false, visible: false });
    }
    try {
      await this.api.post("notebook-migration/delete-notebook", { notebookName: "notebook_" + wid + ".ipynb" });
    } catch {
      /* Database mapping is authoritative; file cleanup remains best effort. */
    }
  }
  dispose(): void {
    this.controller?.abort();
    ++this.revision;
    this.listeners.clear();
  }
  private replace(snapshot: NotebookSnapshot): void {
    this.snapshot = snapshot;
    this.listeners.forEach(listener => listener());
  }
}
