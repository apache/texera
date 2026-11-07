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

import JSZip from "jszip";
import { ApiClient } from "../core/api-client";
import type { GuiConfig } from "../core/runtime-config";
import type { Workflow as WorkflowRecord, WorkflowContent } from "../../core/common/type/workflow";
import type { DashboardWorkflow as WorkflowEntry } from "../../core/dashboard/type/dashboard-workflow.interface";
import type { DefaultView } from "../../core/dashboard/type/workflow-metadata.interface";
import type { WorkflowVersionEntry } from "../../core/dashboard/type/workflow-version-entry";
import { validateEmailFormat } from "../../core/common/util/email";

export type { WorkflowRecord, WorkflowContent, WorkflowEntry, DefaultView, WorkflowVersionEntry };
export type WorkflowSort = "NameAsc" | "NameDesc" | "CreateTimeDesc" | "EditTimeDesc" | "ExecutionTimeDesc";
export type WorkflowView = "CANVAS" | "FORM";
export interface WorkflowSearch {
  keywords?: string[];
  owners?: string[];
  ids?: string[];
  operators?: string[];
  createDateStart?: string;
  createDateEnd?: string;
  modifiedDateStart?: string;
  modifiedDateEnd?: string;
  orderBy?: WorkflowSort;
  start?: number;
  count?: number;
  includePublic?: boolean;
  publicOnly?: boolean;
}
export interface WorkflowSearchPage {
  entries: WorkflowEntry[];
  more: boolean;
  nextStart: number;
}
export interface WorkflowAccess {
  email: string;
  name: string;
  privilege: "NONE" | "READ" | "WRITE";
}
export interface WorkflowSharing {
  owner: string;
  entries: WorkflowAccess[];
  isPublished: boolean;
}
export interface WorkflowActivity {
  ownerName?: string;
  avatar?: string;
  size: number;
  views: number;
  clones: number;
  likes: number;
  isLiked: boolean;
  accessibleUserIds: number[];
}
export const DEFAULT_WORKFLOW_NAME = "Untitled workflow";

function id(value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error("A valid workflow ID is required.");
  return value;
}
function ids(values: number[]): number[] {
  if (!values.length) throw new Error("Select at least one workflow.");
  return [...new Set(values.map(id))];
}
function name(value: string): string {
  if (typeof value !== "string" || !value.trim()) throw new Error("Workflow name cannot be empty.");
  return value;
}
export function emptyWorkflow(config: Readonly<GuiConfig>): WorkflowContent {
  return {
    operators: [],
    links: [],
    commentBoxes: [],
    operatorPositions: {},
    settings: {
      dataTransferBatchSize: config.defaultDataTransferBatchSize ?? 400,
      executionMode: (config.defaultExecutionMode ?? "PIPELINED") as WorkflowContent["settings"]["executionMode"],
    },
  };
}
export function normalizeWorkflow(record: WorkflowRecord | Record<string, unknown>): WorkflowRecord {
  if (!record || typeof record !== "object") throw new Error("The server returned no workflow.");
  const content = typeof record.content === "string" ? JSON.parse(record.content) : record.content;
  return {
    ...record,
    content,
    isPublished: record.isPublished ?? Number((record as { isPublic?: boolean }).isPublic ?? false),
  } as WorkflowRecord;
}
function normalizeEntry(entry: WorkflowEntry): WorkflowEntry {
  if (!entry?.workflow) throw new Error("The server returned no workflow entry.");
  return { ...entry, workflow: normalizeWorkflow(entry.workflow) };
}

/** Exported files store the landing view beside content, never inside the saved content. */
export function parseImportedWorkflow(text: string): { content: WorkflowContent; defaultView?: WorkflowView } {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new Error("The workflow file is not valid JSON.");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("Import a workflow JSON object.");
  const { defaultView, ...content } = parsed as Record<string, unknown>;
  if (
    !Array.isArray(content.operators) ||
    !Array.isArray(content.links) ||
    !Array.isArray(content.commentBoxes) ||
    !content.operatorPositions ||
    typeof content.operatorPositions !== "object" ||
    Array.isArray(content.operatorPositions) ||
    !content.settings ||
    typeof content.settings !== "object"
  )
    throw new Error("The workflow JSON is missing operators, links, positions or settings.");
  if (defaultView !== undefined && defaultView !== "CANVAS" && defaultView !== "FORM")
    throw new Error("The workflow landing view must be CANVAS or FORM.");
  return { content: content as unknown as WorkflowContent, defaultView };
}

/** API contract shared by the workflow list and the editing session. Saves execute in request order. */
export class WorkflowRepository {
  private persistTail: Promise<unknown> = Promise.resolve();
  constructor(private readonly api: ApiClient) {}

  async list(signal?: AbortSignal): Promise<WorkflowEntry[]> {
    const entries = await this.api.get<WorkflowEntry[]>("workflow/list", { signal });
    if (!Array.isArray(entries)) throw new Error("Invalid workflow-list response.");
    return entries.map(normalizeEntry);
  }

  async search(search: WorkflowSearch = {}, signal?: AbortSignal): Promise<WorkflowSearchPage> {
    const query = new URLSearchParams();
    for (const keyword of search.keywords ?? []) if (keyword.trim()) query.append("query", keyword.trim());
    for (const [property, key] of [
      ["owners", "owner"],
      ["ids", "id"],
      ["operators", "operator"],
    ] as const) {
      for (const value of search[property] ?? []) if (value.trim()) query.append(key, value.trim());
    }
    for (const key of ["createDateStart", "createDateEnd", "modifiedDateStart", "modifiedDateEnd"] as const) {
      if (search[key]) query.set(key, search[key]);
    }
    const start = search.start ?? 0;
    const count = search.count ?? 20;
    if (!Number.isInteger(start) || start < 0 || !Number.isInteger(count) || count <= 0)
      throw new Error("Invalid search page.");
    query.set("start", String(start));
    query.set("count", String(count));
    query.set("resourceType", "workflow");
    query.set("orderBy", search.orderBy ?? "EditTimeDesc");
    query.set("includePublic", String(search.publicOnly || search.includePublic || false));
    const result = await this.api.get<{
      results: { resourceType?: string; workflow?: WorkflowEntry }[];
      more: boolean;
    }>(`dashboard/${search.publicOnly ? "publicSearch" : "search"}?${query}`, { signal });
    if (!Array.isArray(result?.results)) throw new Error("Invalid workflow-search response.");
    return {
      entries: result.results.filter(item => item?.workflow != null).map(item => normalizeEntry(item.workflow!)),
      more: Boolean(result.more),
      // Offset follows raw server rows, even when a newer server returns an unknown payload.
      nextStart: start + result.results.length,
    };
  }

  async get(wid: number, signal?: AbortSignal): Promise<WorkflowRecord> {
    return normalizeWorkflow(await this.api.get<WorkflowRecord>(`workflow/${id(wid)}`, { signal }));
  }

  async getPublic(wid: number, signal?: AbortSignal): Promise<WorkflowRecord> {
    return normalizeWorkflow(await this.api.get<WorkflowRecord>(`workflow/publicised/${id(wid)}`, { signal }));
  }

  async getActivity(entries: WorkflowEntry[], signal?: AbortSignal): Promise<Record<number, WorkflowActivity>> {
    if (!entries.length) return {};
    const ownerQuery = new URLSearchParams();
    for (const owner of new Set(entries.map(entry => entry.ownerId))) ownerQuery.append("userIds", String(owner));
    const entityQuery = new URLSearchParams();
    const sizeQuery = new URLSearchParams();
    for (const entry of entries) {
      entityQuery.append("entityType", "workflow");
      entityQuery.append("entityId", String(entry.workflow.wid));
      sizeQuery.append("wid", String(entry.workflow.wid));
    }
    type Count = { entityId: number; counts: { view?: number; clone?: number; like?: number } };
    type Liked = { entityId: number; isLiked: boolean };
    type Access = { entityId: number; userIds: number[] };
    const [owners, counts, liked, access, sizes] = await Promise.all([
      this.api.get<Record<number, { userName: string; avatar?: string }>>(`dashboard/resultsOwnersInfo?${ownerQuery}`, {
        signal,
      }),
      this.api.get<Count[]>(`hub/counts?${entityQuery}`, { signal }),
      this.api.get<Liked[]>(`hub/isLiked?${entityQuery}`, { signal }),
      this.api.get<Access[]>(`hub/user-access?${entityQuery}`, { signal }),
      this.api.get<Record<number, number>>(`workflow/size?${sizeQuery}`, { signal }),
    ]);
    const byId = <T extends { entityId: number }>(items: T[]) =>
      new Map((Array.isArray(items) ? items : []).map(item => [item.entityId, item]));
    const countMap = byId(counts),
      likedMap = byId(liked),
      accessMap = byId(access);
    return Object.fromEntries(
      entries.map(entry => {
        const wid = entry.workflow.wid!;
        const count = countMap.get(wid)?.counts;
        return [
          wid,
          {
            ownerName: owners[entry.ownerId]?.userName ?? entry.ownerName,
            avatar: owners[entry.ownerId]?.avatar,
            size: sizes[wid] ?? 0,
            views: count?.view ?? 0,
            clones: count?.clone ?? 0,
            likes: count?.like ?? 0,
            isLiked: likedMap.get(wid)?.isLiked ?? false,
            accessibleUserIds: accessMap.get(wid)?.userIds ?? [],
          },
        ];
      })
    );
  }

  async toggleLike(wid: number, currentlyLiked: boolean): Promise<{ liked: boolean; likeCount: number }> {
    const success = await this.api.post<boolean>(`hub/${currentlyLiked ? "unlike" : "like"}`, {
      entityId: id(wid),
      entityType: "workflow",
    });
    const counts = await this.api.get<{ entityId: number; counts: { like?: number } }[]>(
      `hub/counts?entityType=workflow&entityId=${wid}&actionType=like`
    );
    return {
      liked: success ? !currentlyLiked : currentlyLiked,
      likeCount: counts.find(count => count.entityId === wid)?.counts.like ?? 0,
    };
  }

  async create(
    content: WorkflowContent,
    workflowName = DEFAULT_WORKFLOW_NAME,
    defaultView?: WorkflowView
  ): Promise<WorkflowEntry> {
    const response = await this.api.post<WorkflowEntry>("workflow/create", {
      name: name(workflowName),
      content: JSON.stringify(content),
      ...(defaultView ? { defaultView } : {}),
    });
    return normalizeEntry(response);
  }

  persist(workflow: WorkflowRecord): Promise<WorkflowRecord> {
    // Snapshot before queueing: edits made while another request is in flight belong to its successor.
    const payload = {
      wid: workflow.wid,
      name: workflow.name,
      description: workflow.description,
      content: JSON.stringify(workflow.content),
    };
    const save = this.persistTail.then(async () =>
      normalizeWorkflow(await this.api.post<WorkflowRecord>("workflow/persist", payload))
    );
    this.persistTail = save.catch(() => undefined);
    return save;
  }

  async whenSavesDrained(): Promise<void> {
    await this.persistTail;
  }

  async duplicate(wids: number[]): Promise<WorkflowEntry[]> {
    return (await this.api.post<WorkflowEntry[]>("workflow/duplicate", { wids: ids(wids) })).map(normalizeEntry);
  }
  async delete(wids: number[]): Promise<void> {
    await this.api.post("workflow/delete", { wids: ids(wids) });
  }
  async rename(wid: number, next: string): Promise<void> {
    await this.api.post("workflow/update/name", { wid: id(wid), name: name(next) });
  }
  async updateDescription(wid: number, description: string): Promise<void> {
    await this.api.post("workflow/update/description", { wid: id(wid), description });
  }
  async setPublished(wid: number, published: boolean): Promise<void> {
    await this.api.put(`workflow/${published ? "public" : "private"}/${id(wid)}`, null);
  }
  async setDefaultView(wid: number, view: WorkflowView): Promise<void> {
    if (view !== "CANVAS" && view !== "FORM") throw new Error("Invalid workflow landing view.");
    await this.api.put(`workflow/set-default-view/${id(wid)}`, { view });
  }

  async importJson(text: string, fileName: string): Promise<WorkflowEntry> {
    const { content, defaultView } = parseImportedWorkflow(text);
    const extension = fileName.lastIndexOf(".");
    const derived = extension < 0 ? fileName : fileName.slice(0, extension);
    return this.create(content, derived.trim() ? derived : DEFAULT_WORKFLOW_NAME, defaultView);
  }

  async importZip(bytes: Uint8Array | ArrayBuffer): Promise<WorkflowEntry[]> {
    const zip = await JSZip.loadAsync(bytes);
    const documents = Object.values(zip.files).filter(file => !file.dir);
    if (!documents.length) throw new Error("The ZIP archive contains no workflows.");
    // Validate the entire archive before creating any workflow.
    const contents = await Promise.all(
      documents.map(async file => ({ name: file.name, text: await file.async("string") }))
    );
    for (const file of contents) parseImportedWorkflow(file.text);
    const created: WorkflowEntry[] = [];
    for (const file of contents) created.push(await this.importJson(file.text, file.name));
    return created;
  }

  async importFiles(files: Iterable<File>): Promise<WorkflowEntry[]> {
    const created: WorkflowEntry[] = [];
    for (const file of files) {
      if (file.name.toLowerCase().endsWith(".zip")) created.push(...(await this.importZip(await readFileBytes(file))));
      else created.push(await this.importJson(await readFileText(file), file.name));
    }
    return created;
  }

  async exportJson(wid: number): Promise<string> {
    const workflow = await this.get(wid);
    return JSON.stringify(
      { ...workflow.content, ...(workflow.defaultView ? { defaultView: workflow.defaultView } : {}) },
      null,
      2
    );
  }

  async exportZip(entries: { wid: number; name: string }[]): Promise<Uint8Array> {
    if (!entries.length) throw new Error("Select at least one workflow.");
    const contents = await Promise.all(
      entries.map(async entry => ({ name: entry.name, text: await this.exportJson(entry.wid) }))
    );
    const zip = new JSZip();
    for (const file of contents) {
      // Keep names inside the ZIP root and avoid silently overwriting duplicate names.
      const clean = file.name.replace(/[\\/]/g, "_") || DEFAULT_WORKFLOW_NAME;
      let filename = `${clean}.json`,
        copy = 0;
      while (zip.file(filename)) filename = `${clean}-${++copy}.json`;
      zip.file(filename, file.text);
    }
    return zip.generateAsync({ type: "uint8array" });
  }

  async getAccess(wid: number, signal?: AbortSignal): Promise<WorkflowSharing> {
    id(wid);
    const [entries, owner, type] = await Promise.all([
      this.api.get<WorkflowAccess[]>(`access/workflow/list/${wid}`, { signal }),
      this.api.get<string>(`access/workflow/owner/${wid}`, { signal, responseType: "text" }),
      this.api.get<string>(`workflow/type/${wid}`, { signal, responseType: "text" }),
    ]);
    return { entries, owner, isPublished: type === "Public" };
  }
  async grantAccess(wid: number, email: string, privilege: "READ" | "WRITE"): Promise<void> {
    const validation = validateEmailFormat(email);
    if (!validation.result) throw new Error(validation.message);
    if (!["READ", "WRITE"].includes(privilege)) throw new Error("Invalid access privilege.");
    await this.api.put(`access/workflow/grant/${id(wid)}/${encodeURIComponent(email.trim())}/${privilege}`, null);
  }
  async revokeAccess(wid: number, email: string): Promise<void> {
    await this.api.delete(`access/workflow/revoke/${id(wid)}/${encodeURIComponent(email)}`);
  }
  async notifyShare(wid: number, sender: string, receiver: string): Promise<void> {
    const subject = `Texera: ${sender} shared a workflow with you`;
    const origin = globalThis.location?.origin ?? "";
    await this.api.put("gmail/send", {
      subject,
      receiver,
      content: `${sender} shared a workflow with you, access the workflow at ${origin}/user/workflow/${id(wid)}`,
    });
  }
  getVersions(wid: number, signal?: AbortSignal): Promise<WorkflowVersionEntry[]> {
    return this.api.get(`version/${id(wid)}`, { signal });
  }
  async getVersion(wid: number, vid: number): Promise<WorkflowRecord> {
    return normalizeWorkflow(await this.api.get<WorkflowRecord>(`version/${id(wid)}/${id(vid)}`));
  }
  cloneVersion(vid: number, displayedVersionId: number): Promise<number> {
    return this.api.post(`version/clone/${id(vid)}`, { displayedVersionId });
  }
  async setCover(wid: number, image: string): Promise<void> {
    await this.api.put(`workflow/${id(wid)}/cover`, { image });
  }
  async deleteCover(wid: number): Promise<void> {
    await this.api.delete(`workflow/${id(wid)}/cover`);
  }
  async cleanupNotebook(wid: number): Promise<void> {
    try {
      await this.api.post("notebook-migration/delete-notebook", { notebookName: `notebook_${id(wid)}.ipynb` });
    } catch {
      /* A successful workflow deletion must not fail because its notebook pod is unavailable. */
    }
  }
}

export function readFileText(file: Blob): Promise<string> {
  if (typeof file.text === "function") return file.text();
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.onerror = () => reject(new Error("The workflow file could not be read."));
    reader.readAsText(file);
  });
}
export function readFileBytes(file: Blob): Promise<ArrayBuffer> {
  if (typeof file.arrayBuffer === "function") return file.arrayBuffer();
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result as ArrayBuffer);
    reader.onerror = () => reject(new Error("The workflow file could not be read."));
    reader.readAsArrayBuffer(file);
  });
}
export function saveDownload(data: BlobPart, filename: string, type = "application/json"): void {
  const url = URL.createObjectURL(new Blob([data], { type }));
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  setTimeout(() => URL.revokeObjectURL(url), 0);
}
