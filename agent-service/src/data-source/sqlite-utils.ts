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

import { mkdtemp, writeFile, unlink, rmdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

export interface SqliteTableSummary {
  name: string;
  rowCount: number;
  columns: string[];
}

export interface SqliteExportResult {
  csv: string;
  rows: number;
  columns: string[];
}

const SQL_TABLE_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Open a SQLite database from raw bytes via Bun's built-in `bun:sqlite`.
 * We write the buffer to a temp file because bun:sqlite needs a path. The
 * caller is responsible for deferring cleanup until export completes too —
 * if you need both list+export, write once and reuse the path.
 */
async function withSqlite<T>(bytes: Uint8Array, fn: (db: any, tempPath: string) => Promise<T>): Promise<T> {
  // Lazy import so non-Bun runtimes don't crash at module load.
  const { Database } = (await import("bun:sqlite")) as typeof import("bun:sqlite");
  const dir = await mkdtemp(join(tmpdir(), "texera-sqlite-"));
  const path = join(dir, "input.sqlite");
  await writeFile(path, bytes);
  let db: any;
  try {
    db = new Database(path, { readonly: true });
    return await fn(db, path);
  } finally {
    try {
      if (db) db.close();
    } catch {}
    try {
      await unlink(path);
    } catch {}
    try {
      await rmdir(dir);
    } catch {}
  }
}

export async function listSqliteTables(bytes: Uint8Array): Promise<SqliteTableSummary[]> {
  return withSqlite(bytes, async db => {
    const tableRows = db
      .query("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
      .all() as { name: string }[];

    const summaries: SqliteTableSummary[] = [];
    for (const { name } of tableRows) {
      if (!SQL_TABLE_NAME.test(name)) {
        // Skip non-standard table names rather than risk an injection; users will see
        // it missing and can rename in their tool of choice.
        continue;
      }
      const countRow = db.query(`SELECT COUNT(*) AS c FROM "${name}"`).get() as { c: number };
      const columnRows = db.query(`PRAGMA table_info("${name}")`).all() as { name: string }[];
      summaries.push({
        name,
        rowCount: countRow?.c ?? 0,
        columns: columnRows.map(c => c.name),
      });
    }
    return summaries;
  });
}

export async function exportSqliteTableAsCsv(
  bytes: Uint8Array,
  tableName: string,
  limit: number = 100_000
): Promise<SqliteExportResult> {
  if (!SQL_TABLE_NAME.test(tableName)) {
    throw new Error(`Invalid table name: ${tableName}`);
  }
  return withSqlite(bytes, async db => {
    const columnRows = db.query(`PRAGMA table_info("${tableName}")`).all() as { name: string }[];
    if (columnRows.length === 0) {
      throw new Error(`Table "${tableName}" not found or has no columns.`);
    }
    const columns = columnRows.map(c => c.name);
    const rows = db.query(`SELECT * FROM "${tableName}" LIMIT ${Math.max(0, Math.floor(limit))}`).all() as Record<
      string,
      any
    >[];

    const lines: string[] = [];
    lines.push(columns.map(csvEscape).join(","));
    for (const row of rows) {
      lines.push(columns.map(c => csvEscape(row[c])).join(","));
    }
    return {
      csv: lines.join("\n"),
      rows: rows.length,
      columns,
    };
  });
}

function csvEscape(value: any): string {
  if (value === null || value === undefined) return "";
  let s: string;
  if (value instanceof Uint8Array) {
    // Encode blobs as base64 strings so the CSV stays printable.
    s = Buffer.from(value).toString("base64");
  } else if (typeof value === "object") {
    s = JSON.stringify(value);
  } else {
    s = String(value);
  }
  if (s.includes(",") || s.includes("\n") || s.includes("\r") || s.includes('"')) {
    s = `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}
