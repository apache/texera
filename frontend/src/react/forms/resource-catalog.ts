/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import type { ApiClient } from "../core/api-client";

export type ResourceKind = "dataset" | "model";
export interface ResourceSummary {
  id: number;
  name: string;
  ownerEmail: string;
  isOwner?: boolean;
  accessPrivilege?: "READ" | "WRITE" | "NONE";
}
export interface ResourceVersion {
  id: number;
  name: string;
}
export interface ResourceFileNode {
  name: string;
  type: "file" | "directory";
  parentDir: string;
  children?: ResourceFileNode[];
  size?: number;
}
export interface ResourceCatalog {
  list(kind: ResourceKind, signal?: AbortSignal): Promise<ResourceSummary[]>;
  versions(kind: ResourceKind, id: number, signal?: AbortSignal): Promise<ResourceVersion[]>;
  files(kind: ResourceKind, id: number, versionId: number, signal?: AbortSignal): Promise<ResourceFileNode[]>;
}

function validId(value: unknown): value is number {
  return Number.isSafeInteger(value) && Number(value) >= 0;
}
function validName(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}
function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object";
}
function checkId(value: unknown): asserts value is number {
  if (!validId(value)) throw new Error("Invalid resource id");
}

/** Adapt the existing backend shapes without coupling widgets to a dashboard view. */
export function createResourceCatalog(api: Pick<ApiClient, "get">): ResourceCatalog {
  return {
    async list(kind, signal) {
      const rows = await api.get<unknown>(`${kind}/list`, { signal });
      if (!Array.isArray(rows)) throw new Error(`Invalid ${kind} catalog response`);
      return rows.map(row => {
        const resource = object(row) ? row[kind] : undefined;
        const id = object(resource) ? resource[kind === "dataset" ? "did" : "mid"] : undefined;
        if (
          !object(row) ||
          !object(resource) ||
          !validId(id) ||
          !validName(resource.name) ||
          !validName(row.ownerEmail)
        ) {
          throw new Error(`Invalid ${kind} catalog response`);
        }
        return {
          id,
          name: resource.name,
          ownerEmail: row.ownerEmail,
          isOwner: row.isOwner === true,
          accessPrivilege: row.accessPrivilege as ResourceSummary["accessPrivilege"],
        };
      });
    },
    async versions(kind, id, signal) {
      checkId(id);
      const rows = await api.get<unknown>(`${kind}/${id}/version/list`, { signal });
      if (!Array.isArray(rows)) throw new Error(`Invalid ${kind} version response`);
      return rows.map(row => {
        const versionId = object(row) ? row[kind === "dataset" ? "dvid" : "mvid"] : undefined;
        if (!object(row) || !validId(versionId) || !validName(row.name))
          throw new Error(`Invalid ${kind} version response`);
        return { id: versionId, name: row.name };
      });
    },
    async files(kind, id, versionId, signal) {
      checkId(id);
      checkId(versionId);
      const result = await api.get<{ fileNodes?: unknown }>(`${kind}/${id}/version/${versionId}/rootFileNodes`, {
        signal,
      });
      const validNodes = (value: unknown): value is ResourceFileNode[] =>
        Array.isArray(value) &&
        value.every(
          node =>
            object(node) &&
            validName(node.name) &&
            typeof node.parentDir === "string" &&
            (node.type === "file" || node.type === "directory") &&
            (node.children === undefined || validNodes(node.children))
        );
      if (!result || !validNodes(result.fileNodes)) throw new Error(`Invalid ${kind} file-tree response`);
      return result.fileNodes;
    },
  };
}

export function resourceVersionPath(kind: ResourceKind, resource: ResourceSummary, version: ResourceVersion): string {
  return `/${kind}/${resource.ownerEmail}/${resource.name}/${version.name}`;
}

export function resourcePathLabel(path: string, kind: ResourceKind): string {
  const parts = path.split("/").filter(Boolean);
  return parts.length >= 4 ? `${parts[2]} · ${parts[3]}` : path || `Select ${kind}`;
}

export function resourcePathParts(path: string): { owner?: string; name?: string; version?: string } {
  const parts = path.split("/").filter(Boolean);
  if (parts[0] === "dataset" || parts[0] === "model") parts.shift();
  return { owner: parts[0], name: parts[1], version: parts[2] };
}
