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

/**
 * "Publish as API" — hackathon-grade workflow publishing.
 *
 * The frontend captures the cached result snapshot from localStorage
 * (the same texera.results.{wid}.{opId} entries the dashboard reads)
 * and registers it here. The /run endpoint returns the cached results
 * verbatim — there is no real execution.
 *
 * Storage is in-process memory: a registry server restart wipes
 * published workflows. The frontend retains the published metadata
 * in its own localStorage and will re-register on demand.
 */

import { Elysia, t } from "elysia";

export interface PublishedResultSnapshot {
  columns: string[];
  rows: (string | number | null)[][];
  timestamp?: string;
}

interface PublishedEntry {
  workflowId: number;
  workflowName: string;
  apiKey: string;
  results: Record<string, PublishedResultSnapshot>;
  createdAt: string;
}

const publishedStore = new Map<number, PublishedEntry>();

export function _resetPublishedStoreForTests(): void {
  publishedStore.clear();
}

const SnapshotSchema = t.Object({
  columns: t.Array(t.String()),
  rows: t.Array(t.Array(t.Union([t.String(), t.Number(), t.Null()]))),
  timestamp: t.Optional(t.String()),
});

export const publishedRouter = new Elysia({ prefix: "/published" })
  .onError(({ error, set }) => {
    const message = error instanceof Error ? error.message : String(error);
    set.status = 500;
    return { error: message || "Internal server error" };
  })

  .post(
    "/register",
    ({ body }) => {
      const { workflowId, workflowName, apiKey, results } = body;
      const entry: PublishedEntry = {
        workflowId,
        workflowName,
        apiKey,
        results,
        createdAt: new Date().toISOString(),
      };
      publishedStore.set(workflowId, entry);
      return {
        workflowId,
        operatorCount: Object.keys(results).length,
        createdAt: entry.createdAt,
      };
    },
    {
      body: t.Object({
        workflowId: t.Number(),
        workflowName: t.String(),
        apiKey: t.String({ minLength: 8 }),
        results: t.Record(t.String(), SnapshotSchema),
      }),
    }
  )

  .post("/:workflowId/run", ({ params, headers, set }) => {
    const workflowId = Number(params.workflowId);
    if (!Number.isFinite(workflowId)) {
      set.status = 400;
      return { error: "Invalid workflowId" };
    }

    const entry = publishedStore.get(workflowId);
    if (!entry) {
      set.status = 404;
      return { error: "Workflow not published" };
    }

    // Header names are lowercased by Elysia. Accept the canonical casing too
    // in case it ever surfaces unmodified.
    const provided =
      (headers["x-api-key"] as string | undefined) ?? (headers["X-API-Key"] as string | undefined);
    if (!provided) {
      set.status = 401;
      return { error: "Missing X-API-Key header" };
    }
    if (provided !== entry.apiKey) {
      set.status = 403;
      return { error: "Invalid API key" };
    }

    return {
      workflowId: entry.workflowId,
      workflowName: entry.workflowName,
      executedAt: new Date().toISOString(),
      cachedAt: entry.createdAt,
      results: entry.results,
    };
  });
