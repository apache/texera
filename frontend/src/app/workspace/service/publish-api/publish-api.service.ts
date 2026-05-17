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

import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { Observable, from, of, throwError } from "rxjs";
import { catchError, mergeMap, map } from "rxjs/operators";

/**
 * "Publish as API" — exposes a workflow's cached result snapshot as a
 * read-only HTTP endpoint hosted by agent-service. There is no live
 * execution: the call returns the same rows the Dashboard Visualizer
 * sees, which the result cache writes to localStorage as
 *   texera.results.{wid}.{operatorId}
 *
 * The set of published workflows is mirrored in localStorage at
 *   texera.published.v1   →   PublishedWorkflow[]
 * so the Publish dialog can show the existing key/endpoint without
 * re-registering. The backend store is in-process memory and resets
 * on restart — calling publish again re-uploads the snapshot.
 */

const PUBLISHED_KEY = "texera.published.v1";
const RESULTS_PREFIX = "texera.results";

export interface PublishedSnapshot {
  columns: string[];
  rows: (string | number | null)[][];
  timestamp?: string;
}

export interface PublishedWorkflow {
  workflowId: number;
  workflowName: string;
  apiKey: string;
  endpoint: string;
  createdAt: string;
  operatorCount: number;
}

interface RegisterResponse {
  workflowId: number;
  operatorCount: number;
  createdAt: string;
}

@Injectable({ providedIn: "root" })
export class PublishApiService {
  constructor(private http: HttpClient) {}

  /** Read the published-workflow list from localStorage. */
  listPublished(): PublishedWorkflow[] {
    try {
      const raw = localStorage.getItem(PUBLISHED_KEY);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? (parsed as PublishedWorkflow[]) : [];
    } catch {
      return [];
    }
  }

  getPublished(workflowId: number): PublishedWorkflow | undefined {
    return this.listPublished().find(p => p.workflowId === workflowId);
  }

  /**
   * Collects all cached operator results for `workflowId` from
   * localStorage, generates an API key (or reuses the existing one),
   * registers the snapshot with agent-service, and persists the
   * published-workflow metadata in localStorage.
   */
  publish(workflowId: number, workflowName: string): Observable<PublishedWorkflow> {
    const snapshots = this.collectSnapshots(workflowId);
    if (Object.keys(snapshots).length === 0) {
      return throwError(
        () =>
          new Error(
            "No cached results found for this workflow. Run the workflow at least once before publishing."
          )
      );
    }

    const existing = this.getPublished(workflowId);
    const apiKey = existing?.apiKey ?? this.generateApiKey();

    return this.http
      .post<RegisterResponse>("/api/published/register", {
        workflowId,
        workflowName,
        apiKey,
        results: snapshots,
      })
      .pipe(
        map(resp => {
          const endpoint = this.buildEndpoint(workflowId);
          const entry: PublishedWorkflow = {
            workflowId,
            workflowName,
            apiKey,
            endpoint,
            createdAt: resp.createdAt,
            operatorCount: resp.operatorCount,
          };
          this.upsert(entry);
          return entry;
        }),
        catchError(err => {
          const message = err?.error?.error || err?.message || "Publish failed.";
          return throwError(() => new Error(message));
        })
      );
  }

  /** Sample curl the dialog shows to the user. */
  buildCurlCommand(p: PublishedWorkflow): string {
    return [
      `curl -X POST '${p.endpoint}' \\`,
      `  -H 'X-API-Key: ${p.apiKey}' \\`,
      `  -H 'Content-Type: application/json'`,
    ].join("\n");
  }

  private collectSnapshots(workflowId: number): Record<string, PublishedSnapshot> {
    const prefix = `${RESULTS_PREFIX}.${workflowId}.`;
    const out: Record<string, PublishedSnapshot> = {};
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (!key || !key.startsWith(prefix)) continue;
      const opId = key.slice(prefix.length);
      try {
        const v = JSON.parse(localStorage.getItem(key) ?? "");
        if (v && Array.isArray(v.columns) && Array.isArray(v.rows)) {
          out[opId] = { columns: v.columns, rows: v.rows, timestamp: v.timestamp };
        }
      } catch {
        // skip malformed entry
      }
    }
    return out;
  }

  private upsert(entry: PublishedWorkflow): void {
    const list = this.listPublished().filter(p => p.workflowId !== entry.workflowId);
    list.push(entry);
    try {
      localStorage.setItem(PUBLISHED_KEY, JSON.stringify(list));
    } catch {
      // localStorage full — best effort; the backend still has the registration.
    }
  }

  private buildEndpoint(workflowId: number): string {
    // Use the current origin so the URL the user copies works from the same
    // host that proxies /api → agent-service. In dev this is the Angular
    // dev server; in prod it is whatever serves the SPA.
    const origin = typeof window !== "undefined" ? window.location.origin : "";
    return `${origin}/api/published/${workflowId}/run`;
  }

  private generateApiKey(): string {
    const bytes = new Uint8Array(24);
    crypto.getRandomValues(bytes);
    return "tex_" + Array.from(bytes, b => b.toString(16).padStart(2, "0")).join("");
  }
}
