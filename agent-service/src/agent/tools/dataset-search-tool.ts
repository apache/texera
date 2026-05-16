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

import { z } from "zod";
import { tool } from "ai";
import { env } from "../../config/env";
import { createErrorResult } from "./tools-utility";

export const TOOL_NAME_SEARCH_DATASETS = "search_datasets";

const PER_SOURCE_LIMIT = 5;
const FETCH_TIMEOUT_MS = 8000;

export type DatasetSource = "dknet" | "uci" | "kaggle";

export interface DatasetSearchResult {
  name: string;
  description: string;
  source: DatasetSource;
  url: string;
  format?: string;
}

async function fetchWithTimeout(url: string, init?: RequestInit, timeoutMs = FETCH_TIMEOUT_MS): Promise<Response> {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(t);
  }
}

function asString(v: unknown): string | undefined {
  return typeof v === "string" && v.length > 0 ? v : undefined;
}

function pickArray(payload: unknown, keys: string[]): unknown[] {
  if (Array.isArray(payload)) return payload;
  if (payload && typeof payload === "object") {
    for (const k of keys) {
      const v = (payload as Record<string, unknown>)[k];
      if (Array.isArray(v)) return v;
    }
  }
  return [];
}

async function searchDknet(query: string): Promise<DatasetSearchResult[]> {
  const url = `https://dknet.org/api/search?query=${encodeURIComponent(query)}&type=dataset`;
  const resp = await fetchWithTimeout(url, { headers: { Accept: "application/json" } });
  if (!resp.ok) throw new Error(`dkNET ${resp.status}`);
  const ct = resp.headers.get("content-type") || "";
  if (!ct.includes("json")) throw new Error(`dkNET non-JSON (${ct})`);
  const body = (await resp.json()) as unknown;
  const items = pickArray(body, ["results", "items", "data", "hits"]).slice(0, PER_SOURCE_LIMIT);
  return items.map((raw): DatasetSearchResult => {
    const r = (raw ?? {}) as Record<string, unknown>;
    const name = asString(r.name) ?? asString(r.title) ?? asString(r.label) ?? "(unnamed)";
    const description = asString(r.description) ?? asString(r.summary) ?? asString(r.abstract) ?? "";
    const link =
      asString(r.url) ??
      asString(r.link) ??
      asString(r.identifier) ??
      asString((r as any).id) ??
      "https://dknet.org/";
    const format = asString(r.format) ?? asString((r as any).fileFormat);
    return { name, description, source: "dknet", url: link, format };
  });
}

async function searchUci(query: string): Promise<DatasetSearchResult[]> {
  const url = `https://archive.ics.uci.edu/api/datasets?search=${encodeURIComponent(query)}`;
  const resp = await fetchWithTimeout(url, { headers: { Accept: "application/json" } });
  if (!resp.ok) throw new Error(`UCI ${resp.status}`);
  const ct = resp.headers.get("content-type") || "";
  if (!ct.includes("json")) throw new Error(`UCI non-JSON (${ct})`);
  const body = (await resp.json()) as unknown;
  const items = pickArray(body, ["datasets", "data", "results"]).slice(0, PER_SOURCE_LIMIT);
  return items.map((raw): DatasetSearchResult => {
    const r = (raw ?? {}) as Record<string, unknown>;
    const id = (r.id ?? r.dataset_id ?? r.slug) as string | number | undefined;
    const name = asString(r.name) ?? asString(r.title) ?? `UCI dataset ${id ?? ""}`.trim();
    const description = asString(r.abstract) ?? asString(r.description) ?? asString(r.summary) ?? "";
    const slug = asString(r.slug) ?? (id !== undefined ? String(id) : "");
    const link =
      asString(r.url) ?? (slug ? `https://archive.ics.uci.edu/dataset/${slug}` : "https://archive.ics.uci.edu/datasets");
    const format = asString(r.format) ?? "csv";
    return { name, description, source: "uci", url: link, format };
  });
}

async function searchKaggle(query: string, username: string, key: string): Promise<DatasetSearchResult[]> {
  const url = `https://www.kaggle.com/api/v1/datasets/list?search=${encodeURIComponent(query)}`;
  const basic = Buffer.from(`${username}:${key}`).toString("base64");
  const resp = await fetchWithTimeout(url, {
    headers: { Authorization: `Basic ${basic}`, Accept: "application/json" },
  });
  if (!resp.ok) throw new Error(`Kaggle ${resp.status}`);
  const body = (await resp.json()) as unknown;
  const items = pickArray(body, ["datasets", "results"]).slice(0, PER_SOURCE_LIMIT);
  return items.map((raw): DatasetSearchResult => {
    const r = (raw ?? {}) as Record<string, unknown>;
    const ref = asString(r.ref);
    const name = asString(r.title) ?? ref ?? "(unnamed)";
    const description = asString(r.subtitle) ?? asString(r.description) ?? "";
    const link = asString(r.url) ?? (ref ? `https://www.kaggle.com/datasets/${ref}` : "https://www.kaggle.com/datasets");
    return { name, description, source: "kaggle", url: link, format: "csv" };
  });
}

interface PerSourceOutcome {
  source: DatasetSource;
  results: DatasetSearchResult[];
  error?: string;
}

async function runSearches(query: string, source: "dknet" | "uci" | "kaggle" | "all"): Promise<PerSourceOutcome[]> {
  const targets: DatasetSource[] = source === "all" ? ["dknet", "uci", "kaggle"] : [source];
  const tasks = targets.map(async (s): Promise<PerSourceOutcome> => {
    try {
      if (s === "dknet") return { source: s, results: await searchDknet(query) };
      if (s === "uci") return { source: s, results: await searchUci(query) };
      if (!env.KAGGLE_KEY || !env.KAGGLE_USERNAME) {
        return { source: s, results: [], error: "KAGGLE_USERNAME/KAGGLE_KEY not configured" };
      }
      return { source: s, results: await searchKaggle(query, env.KAGGLE_USERNAME, env.KAGGLE_KEY) };
    } catch (err: any) {
      return { source: s, results: [], error: err?.message ?? String(err) };
    }
  });
  return Promise.all(tasks);
}

function formatOutcomes(outcomes: PerSourceOutcome[]): string {
  const lines: string[] = [];
  const totalResults = outcomes.reduce((n, o) => n + o.results.length, 0);
  lines.push(`Found ${totalResults} dataset(s) across ${outcomes.length} source(s).`);
  for (const o of outcomes) {
    const header = `\n## ${o.source} (${o.results.length} result${o.results.length === 1 ? "" : "s"})`;
    if (o.error) {
      lines.push(`${header}  [skipped: ${o.error}]`);
      continue;
    }
    lines.push(header);
    if (o.results.length === 0) {
      lines.push("(no results)");
      continue;
    }
    o.results.forEach((r, i) => {
      const desc = r.description.length > 200 ? r.description.slice(0, 200) + "..." : r.description;
      lines.push(
        `${i + 1}. ${r.name}` +
          (r.format ? ` [${r.format}]` : "") +
          `\n   ${desc}` +
          `\n   url: ${r.url}`
      );
    });
  }
  return lines.join("\n");
}

export function createSearchDatasetsTool() {
  return tool({
    description:
      "Search public dataset catalogs (dkNET, UCI ML Repository, Kaggle) for datasets matching a query. " +
      "Use this when the user asks to find or import a dataset from an external source, e.g. " +
      "'find me a diabetes dataset' or 'is there a public dataset for X?'. " +
      "Returns up to 5 candidates per source with name, description, source, url, and format. " +
      "Present the candidates to the user and let them pick one; then add a CSVFileScan/FileScan operator " +
      "pointing at the chosen dataset's URL or downloaded path.",
    inputSchema: z.object({
      query: z.string().min(1).describe("Free-text search query, e.g. 'diabetes', 'titanic', 'protein folding'."),
      source: z
        .enum(["dknet", "uci", "kaggle", "all"])
        .optional()
        .describe("Which catalog to search. Defaults to 'all' (queries every source in parallel)."),
    }),
    execute: async (args: { query: string; source?: "dknet" | "uci" | "kaggle" | "all" }) => {
      const source = args.source ?? "all";
      try {
        const outcomes = await runSearches(args.query, source);
        return formatOutcomes(outcomes);
      } catch (err: any) {
        return createErrorResult(`search_datasets failed: ${err?.message ?? String(err)}`);
      }
    },
  });
}
