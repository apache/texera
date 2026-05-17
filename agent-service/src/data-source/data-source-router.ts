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

import { Elysia, t } from "elysia";
import { createLogger } from "../logger";
import {
  detectFormatFromContentType,
  jsonToCsv,
  parseCsvPreview,
  type FetchedData,
} from "./format-utils";
import { listSqliteTables, exportSqliteTableAsCsv } from "./sqlite-utils";

const log = createLogger("DataSource");

const FETCH_TIMEOUT_MS = 30_000;
const MAX_RESPONSE_BYTES = 100 * 1024 * 1024; // 100 MB safety cap

/**
 * In-memory cache of uploaded SQLite files. Two-step flow:
 *  1. POST /sqlite-tables uploads the bytes once and returns a fileHandle + table list
 *  2. POST /sqlite-export references the handle to export a table without re-uploading
 *
 * Entries auto-expire to prevent leaking memory if the user navigates away.
 */
interface SqliteCacheEntry {
  bytes: Uint8Array;
  filename: string;
  createdAt: number;
}
const SQLITE_CACHE_TTL_MS = 30 * 60 * 1000;
const SQLITE_MAX_BYTES = 500 * 1024 * 1024;
const sqliteCache = new Map<string, SqliteCacheEntry>();

function pruneSqliteCache(): void {
  const now = Date.now();
  for (const [handle, entry] of sqliteCache) {
    if (now - entry.createdAt > SQLITE_CACHE_TTL_MS) {
      sqliteCache.delete(handle);
    }
  }
}

function generateHandle(): string {
  return `sqlite-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function suggestFilename(url: string, contentType: string): string {
  let pathSegment = "data";
  try {
    const parsed = new URL(url);
    const segments = parsed.pathname.split("/").filter(Boolean);
    if (segments.length > 0) {
      pathSegment = segments[segments.length - 1];
    }
  } catch {}

  // Strip query/hash if any leaked through
  pathSegment = pathSegment.split("?")[0].split("#")[0];

  if (pathSegment.includes(".")) {
    return pathSegment;
  }

  const lc = contentType.toLowerCase();
  if (lc.includes("json")) return `${pathSegment}.json`;
  if (lc.includes("csv")) return `${pathSegment}.csv`;
  return `${pathSegment}.txt`;
}

async function fetchUrlWithTimeout(
  url: string,
  method: string,
  headers: Record<string, string>
): Promise<FetchedData> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const response = await fetch(url, {
      method,
      headers,
      signal: controller.signal,
      redirect: "follow",
    });
    if (!response.ok) {
      throw new Error(`Upstream returned ${response.status} ${response.statusText}`);
    }

    const contentType = response.headers.get("content-type") || "";
    const buf = await response.arrayBuffer();
    if (buf.byteLength > MAX_RESPONSE_BYTES) {
      throw new Error(`Response too large (${buf.byteLength} bytes, limit ${MAX_RESPONSE_BYTES})`);
    }
    const text = new TextDecoder("utf-8").decode(buf);
    return { contentType, text, byteLength: buf.byteLength };
  } finally {
    clearTimeout(timeoutId);
  }
}

export const dataSourceRouter = new Elysia({ prefix: "/data-source" })
  .onError(({ error, set }) => {
    log.error({ err: error }, "data-source request error");
    const message = error instanceof Error ? error.message : String(error);
    set.status = 500;
    return { error: message };
  })

  /**
   * Fetch a URL server-side (bypasses browser CORS) and return the body as text
   * along with metadata. The caller (frontend or agent tool) decides how to use it.
   */
  .post(
    "/fetch-url",
    async ({ body }) => {
      const { url, method = "GET", headers = {}, format = "auto" } = body as {
        url: string;
        method?: "GET" | "POST";
        headers?: Record<string, string>;
        format?: "json" | "csv" | "auto";
      };

      log.info({ url, method, format }, "fetching url");

      const fetched = await fetchUrlWithTimeout(url, method, headers);
      const detectedFormat =
        format === "auto" ? detectFormatFromContentType(fetched.contentType, fetched.text) : format;

      const filename = suggestFilename(url, fetched.contentType);

      let csvText: string;
      let columns: string[];
      let preview: any[];
      let rowCount: number;

      if (detectedFormat === "json") {
        const conversion = jsonToCsv(fetched.text);
        csvText = conversion.csv;
        columns = conversion.columns;
        preview = conversion.preview;
        rowCount = conversion.rowCount;
      } else {
        // Treat as CSV (or whatever text it is — pass through)
        csvText = fetched.text;
        const parsed = parseCsvPreview(fetched.text);
        columns = parsed.columns;
        preview = parsed.preview;
        rowCount = parsed.rowCount;
      }

      // Always export a normalized .csv name when we converted JSON,
      // otherwise keep the suggested filename.
      const exportFilename =
        detectedFormat === "json" ? filename.replace(/\.json$/i, ".csv") : filename;

      return {
        url,
        contentType: fetched.contentType,
        format: detectedFormat,
        byteLength: fetched.byteLength,
        filename: exportFilename,
        rows: rowCount,
        columns,
        preview,
        csv: csvText,
      };
    },
    {
      body: t.Object({
        url: t.String({ minLength: 1 }),
        method: t.Optional(t.Union([t.Literal("GET"), t.Literal("POST")])),
        headers: t.Optional(t.Record(t.String(), t.String())),
        format: t.Optional(t.Union([t.Literal("json"), t.Literal("csv"), t.Literal("auto")])),
      }),
    }
  )

  /**
   * Accept an uploaded .sqlite/.db file and return its table list. The bytes
   * are cached server-side under a fileHandle so /sqlite-export can read them
   * without forcing the browser to re-upload.
   */
  .post(
    "/sqlite-tables",
    async ({ body, set }) => {
      pruneSqliteCache();
      const form = body as { file: File };
      const file = form.file;
      if (!file) {
        set.status = 400;
        return { error: "file is required" };
      }
      if (file.size > SQLITE_MAX_BYTES) {
        set.status = 413;
        return { error: `SQLite file too large (${file.size} bytes, limit ${SQLITE_MAX_BYTES})` };
      }
      const arrayBuffer = await file.arrayBuffer();
      const bytes = new Uint8Array(arrayBuffer);

      log.info({ filename: file.name, size: bytes.byteLength }, "received sqlite upload");

      const tables = await listSqliteTables(bytes);
      const handle = generateHandle();
      sqliteCache.set(handle, { bytes, filename: file.name, createdAt: Date.now() });

      return {
        fileHandle: handle,
        filename: file.name,
        tables,
      };
    },
    {
      body: t.Object({
        file: t.File(),
      }),
    }
  )

  /**
   * Export a single table from a previously uploaded SQLite file (by fileHandle) as CSV.
   */
  .post(
    "/sqlite-export",
    async ({ body, set }) => {
      pruneSqliteCache();
      const { fileHandle, tableName, limit } = body as {
        fileHandle: string;
        tableName: string;
        limit?: number;
      };

      const entry = sqliteCache.get(fileHandle);
      if (!entry) {
        set.status = 404;
        return { error: "Unknown or expired fileHandle. Please re-upload the SQLite file." };
      }

      log.info({ fileHandle, tableName, limit }, "exporting sqlite table");

      const exported = await exportSqliteTableAsCsv(entry.bytes, tableName, limit ?? 100_000);
      return exported;
    },
    {
      body: t.Object({
        fileHandle: t.String({ minLength: 1 }),
        tableName: t.String({ minLength: 1 }),
        limit: t.Optional(t.Number()),
      }),
    }
  );
