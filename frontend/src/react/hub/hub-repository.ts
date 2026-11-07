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

import { ApiClient } from "../core/api-client";
import {
  WorkflowRepository,
  type WorkflowRecord,
  type WorkflowSort,
  type WorkflowSearch,
} from "../workflows/workflow-repository";
import type { SearchResultItem } from "../../core/dashboard/type/search-result";

export type HubKind = "workflow" | "dataset" | "model";
export interface HubSearch extends WorkflowSearch {
  scope?: "public" | "private";
}
export interface HubItem {
  kind: HubKind;
  id: number;
  name: string;
  description: string;
  ownerId?: number;
  ownerName?: string;
  ownerEmail?: string;
  cover?: string;
  creationTime?: number;
  lastModifiedTime?: number;
  size?: number;
  accessLevel?: string;
}
export interface HubActivity {
  views: number;
  clones: number;
  likes: number;
  isLiked: boolean;
  accessibleUserIds: number[];
  ownerName?: string;
  avatar?: string;
  size?: number;
}
export type HubActivityMap = Record<string, HubActivity>;
export interface HubSearchPageResult {
  items: HubItem[];
  more: boolean;
  nextStart: number;
  hasMismatch: boolean;
}
export function activityKey(item: Pick<HubItem, "kind" | "id">): string {
  return `${item.kind}:${item.id}`;
}
function validId(value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error("A valid resource ID is required.");
  return value;
}
export function normalizeHubItem(raw: SearchResultItem): HubItem | undefined {
  if (raw?.workflow?.workflow) {
    const entry = raw.workflow;
    const workflow = entry.workflow;
    if (!workflow.wid) return undefined;
    return {
      kind: "workflow",
      id: workflow.wid,
      name: workflow.name,
      description: workflow.description ?? "",
      ownerId: entry.ownerId,
      ownerName: entry.ownerName,
      cover: entry.coverImage ?? undefined,
      creationTime: workflow.creationTime,
      lastModifiedTime: workflow.lastModifiedTime,
      accessLevel: entry.accessLevel,
    };
  }
  if (raw?.dataset?.dataset) {
    const entry = raw.dataset,
      dataset = entry.dataset;
    if (!dataset.did) return undefined;
    return {
      kind: "dataset",
      id: dataset.did,
      name: dataset.name,
      description: dataset.description ?? "",
      ownerId: dataset.ownerUid,
      ownerEmail: entry.ownerEmail,
      cover: dataset.coverImage,
      size: entry.size,
      creationTime: dataset.creationTime,
      accessLevel: entry.accessPrivilege,
    };
  }
  if (raw?.model?.model) {
    const entry = raw.model,
      model = entry.model;
    if (!model.mid) return undefined;
    return {
      kind: "model",
      id: model.mid,
      name: model.name,
      description: model.description ?? "",
      ownerId: model.ownerUid,
      ownerEmail: entry.ownerEmail,
      cover: model.coverImage,
      size: entry.size,
      creationTime: model.creationTime,
      accessLevel: entry.accessPrivilege,
    };
  }
  return undefined;
}

export class HubRepository {
  private readonly workflows: WorkflowRepository;
  constructor(private readonly api: ApiClient) {
    this.workflows = new WorkflowRepository(api);
  }

  async search(kind?: HubKind, search: HubSearch = {}, signal?: AbortSignal): Promise<HubSearchPageResult> {
    const query = new URLSearchParams();
    for (const keyword of search.keywords ?? []) if (keyword.trim()) query.append("query", keyword.trim());
    for (const [field, parameter] of [
      ["owners", "owner"],
      ["ids", "id"],
      ["operators", "operator"],
    ] as const) {
      for (const value of search[field] ?? []) if (value.trim()) query.append(parameter, value.trim());
    }
    for (const field of ["createDateStart", "createDateEnd", "modifiedDateStart", "modifiedDateEnd"] as const)
      if (search[field]) query.set(field, search[field]);
    const start = search.start ?? 0,
      count = search.count ?? 20;
    if (!Number.isInteger(start) || start < 0 || !Number.isInteger(count) || count <= 0)
      throw new Error("Invalid search page.");
    query.set("start", String(start));
    query.set("count", String(count));
    if (kind) query.set("resourceType", kind);
    query.set(
      "orderBy",
      search.orderBy ?? (kind === "dataset" || kind === "model" ? "CreateTimeDesc" : "EditTimeDesc")
    );
    query.set("includePublic", search.scope === "private" ? "false" : "true");
    const response = await this.api.get<{ results: SearchResultItem[]; more: boolean; hasMismatch?: boolean }>(
      `dashboard/${search.scope === "private" ? "search" : "publicSearch"}?${query}`,
      { signal }
    );
    if (!Array.isArray(response?.results)) throw new Error("Invalid public-search response.");
    return {
      items: response.results.map(normalizeHubItem).filter((item): item is HubItem => Boolean(item)),
      more: Boolean(response.more),
      nextStart: start + response.results.length,
      hasMismatch: Boolean(response.hasMismatch),
    };
  }

  count(kind: HubKind, signal?: AbortSignal): Promise<number> {
    return this.api.get(`hub/count?entityType=${kind}`, { signal });
  }
  owners(kind: HubKind, signal?: AbortSignal): Promise<string[]> {
    return this.api.get(`hub/owners?entityType=${kind}`, { signal });
  }
  async tops(
    kind: HubKind,
    actions: ("like" | "clone")[],
    userId?: number,
    limit = 8,
    signal?: AbortSignal
  ): Promise<Record<"like" | "clone", HubItem[]>> {
    const query = new URLSearchParams({ entityType: kind, uid: String(userId ?? -1), limit: String(limit) });
    for (const action of actions) query.append("actionTypes", action);
    const response = await this.api.get<Partial<Record<"like" | "clone", SearchResultItem[]>>>(`hub/getTops?${query}`, {
      signal,
    });
    const normalize = (items?: SearchResultItem[]) =>
      (items ?? []).map(normalizeHubItem).filter((item): item is HubItem => Boolean(item));
    return { like: normalize(response.like), clone: normalize(response.clone) };
  }

  async activity(items: HubItem[], userId?: number, signal?: AbortSignal): Promise<HubActivityMap> {
    if (!items.length) return {};
    const query = new URLSearchParams();
    for (const item of items) {
      query.append("entityType", item.kind);
      query.append("entityId", String(item.id));
    }
    const ownerIds = [...new Set(items.flatMap(item => (item.ownerId !== undefined ? [item.ownerId] : [])))];
    const ownerQuery = new URLSearchParams();
    for (const owner of ownerIds) ownerQuery.append("userIds", String(owner));
    const workflowIds = items.filter(item => item.kind === "workflow").map(item => item.id);
    const sizeQuery = new URLSearchParams();
    for (const wid of workflowIds) sizeQuery.append("wid", String(wid));
    type Count = { entityId: number; entityType: HubKind; counts: { view?: number; clone?: number; like?: number } };
    type Liked = { entityId: number; entityType: HubKind; isLiked: boolean };
    type Access = { entityId: number; entityType: HubKind; userIds: number[] };
    const [counts, liked, access, owners, sizes] = await Promise.all([
      this.api.get<Count[]>(`hub/counts?${query}`, { signal }),
      userId !== undefined ? this.api.get<Liked[]>(`hub/isLiked?${query}`, { signal }) : Promise.resolve([]),
      this.api.get<Access[]>(`hub/user-access?${query}`, { signal }),
      ownerIds.length
        ? this.api.get<Record<number, { userName: string; avatar?: string }>>(
            `dashboard/resultsOwnersInfo?${ownerQuery}`,
            { signal }
          )
        : Promise.resolve({}),
      workflowIds.length
        ? this.api.get<Record<number, number>>(`workflow/size?${sizeQuery}`, { signal })
        : Promise.resolve({}),
    ]);
    const index = <T extends { entityType: HubKind; entityId: number }>(rows: T[]) =>
      new Map((Array.isArray(rows) ? rows : []).map(row => [`${row.entityType}:${row.entityId}`, row]));
    const countMap = index(counts),
      likedMap = index(liked),
      accessMap = index(access);
    return Object.fromEntries(
      items.map(item => {
        const key = activityKey(item),
          count = countMap.get(key)?.counts;
        const owner =
          item.ownerId !== undefined
            ? (owners as Record<number, { userName: string; avatar?: string }>)[item.ownerId]
            : undefined;
        return [
          key,
          {
            views: count?.view ?? 0,
            likes: count?.like ?? 0,
            clones: count?.clone ?? 0,
            isLiked: likedMap.get(key)?.isLiked ?? false,
            accessibleUserIds: accessMap.get(key)?.userIds ?? [],
            ownerName: owner?.userName ?? item.ownerName,
            avatar: owner?.avatar,
            size: item.kind === "workflow" ? (sizes as Record<number, number>)[item.id] ?? 0 : item.size,
          },
        ];
      })
    );
  }

  async workflowDetail(wid: number, signal?: AbortSignal): Promise<{ workflow: WorkflowRecord; ownerName: string }> {
    validId(wid);
    const [workflow, ownerName] = await Promise.all([
      this.workflows.getPublic(wid, signal),
      this.api.get<string>(`workflow/owner_name?wid=${wid}`, { signal, responseType: "text" }),
    ]);
    return { workflow, ownerName };
  }
  async recordView(kind: HubKind, id: number, userId = 0): Promise<number> {
    return this.api.post("hub/view", { entityId: validId(id), userId, entityType: kind });
  }
  async cloneWorkflow(wid: number): Promise<number> {
    return this.api.post(`workflow/clone/${validId(wid)}`, null);
  }
  async toggleLike(kind: HubKind, id: number, currentlyLiked: boolean): Promise<{ liked: boolean; likeCount: number }> {
    const success = await this.api.post<boolean>(`hub/${currentlyLiked ? "unlike" : "like"}`, {
      entityId: validId(id),
      entityType: kind,
    });
    const counts = await this.api.get<{ entityId: number; entityType: HubKind; counts: { like?: number } }[]>(
      `hub/counts?entityType=${kind}&entityId=${id}&actionType=like`
    );
    return {
      liked: success ? !currentlyLiked : currentlyLiked,
      likeCount: counts.find(row => row.entityType === kind && row.entityId === id)?.counts.like ?? 0,
    };
  }
}
export type { WorkflowRecord, WorkflowSort, WorkflowSearch };
