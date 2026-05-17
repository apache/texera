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

export interface FetchedData {
  contentType: string;
  text: string;
  byteLength: number;
}

export interface CsvConversion {
  csv: string;
  columns: string[];
  preview: any[];
  rowCount: number;
}

export function detectFormatFromContentType(contentType: string, body: string): "json" | "csv" {
  const lc = contentType.toLowerCase();
  if (lc.includes("json")) return "json";
  if (lc.includes("csv")) return "csv";

  // Sniff: if it parses as JSON, it's JSON.
  const trimmed = body.trimStart();
  if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
    try {
      JSON.parse(body);
      return "json";
    } catch {}
  }
  return "csv";
}

/**
 * Flatten a nested object using dot-notation. Arrays become JSON strings so they
 * fit in a single CSV cell. We don't try to expand arrays into rows.
 */
function flatten(obj: any, prefix: string = "", out: Record<string, any> = {}): Record<string, any> {
  if (obj === null || obj === undefined) {
    if (prefix) out[prefix] = "";
    return out;
  }
  if (Array.isArray(obj)) {
    out[prefix || "value"] = JSON.stringify(obj);
    return out;
  }
  if (typeof obj === "object") {
    for (const [key, value] of Object.entries(obj)) {
      const nextKey = prefix ? `${prefix}.${key}` : key;
      if (value !== null && typeof value === "object" && !Array.isArray(value)) {
        flatten(value, nextKey, out);
      } else if (Array.isArray(value)) {
        out[nextKey] = JSON.stringify(value);
      } else {
        out[nextKey] = value;
      }
    }
    return out;
  }
  out[prefix || "value"] = obj;
  return out;
}

function csvEscape(value: any): string {
  if (value === null || value === undefined) return "";
  let s = typeof value === "string" ? value : String(value);
  if (s.includes(",") || s.includes("\n") || s.includes("\r") || s.includes('"')) {
    s = `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

/**
 * Convert JSON text to CSV. Supports:
 *  - top-level array of objects: each element is a row
 *  - top-level object with an array field (e.g. {data: [...], meta: ...}): use that array
 *  - top-level scalar/object: single-row CSV
 */
export function jsonToCsv(jsonText: string): CsvConversion {
  let parsed: any;
  try {
    parsed = JSON.parse(jsonText);
  } catch (e) {
    throw new Error(`Response did not parse as JSON: ${(e as Error).message}`);
  }

  let rows: any[];
  if (Array.isArray(parsed)) {
    rows = parsed;
  } else if (parsed && typeof parsed === "object") {
    // Find the first array field that looks like records
    const arrayField = Object.values(parsed).find(v => Array.isArray(v) && v.length > 0);
    if (arrayField) {
      rows = arrayField as any[];
    } else {
      rows = [parsed];
    }
  } else {
    rows = [{ value: parsed }];
  }

  if (rows.length === 0) {
    return { csv: "", columns: [], preview: [], rowCount: 0 };
  }

  const flatRows = rows.map(row => {
    if (row === null || row === undefined) return {};
    if (typeof row !== "object") return { value: row };
    return flatten(row);
  });

  // Collect column names in first-seen order
  const colSet = new Set<string>();
  const columns: string[] = [];
  for (const row of flatRows) {
    for (const key of Object.keys(row)) {
      if (!colSet.has(key)) {
        colSet.add(key);
        columns.push(key);
      }
    }
  }

  const lines: string[] = [];
  lines.push(columns.map(csvEscape).join(","));
  for (const row of flatRows) {
    lines.push(columns.map(c => csvEscape(row[c])).join(","));
  }

  return {
    csv: lines.join("\n"),
    columns,
    preview: flatRows.slice(0, 5),
    rowCount: flatRows.length,
  };
}

/**
 * Lightweight CSV preview. Handles quoted fields with embedded commas/newlines.
 * Returns the columns (header), first 5 data rows as objects, and total row count.
 */
export function parseCsvPreview(csvText: string): {
  columns: string[];
  preview: any[];
  rowCount: number;
} {
  const records = parseCsv(csvText);
  if (records.length === 0) {
    return { columns: [], preview: [], rowCount: 0 };
  }
  const header = records[0];
  const dataRows = records.slice(1);
  const preview = dataRows.slice(0, 5).map(row => {
    const obj: Record<string, any> = {};
    header.forEach((col, i) => {
      obj[col] = row[i] ?? "";
    });
    return obj;
  });
  return {
    columns: header,
    preview,
    rowCount: dataRows.length,
  };
}

function parseCsv(text: string): string[][] {
  const records: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;
  let i = 0;
  while (i < text.length) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
        i++;
        continue;
      }
      field += ch;
      i++;
    } else {
      if (ch === '"') {
        inQuotes = true;
        i++;
      } else if (ch === ",") {
        row.push(field);
        field = "";
        i++;
      } else if (ch === "\r") {
        // swallow; \r\n handled by \n branch
        i++;
      } else if (ch === "\n") {
        row.push(field);
        records.push(row);
        row = [];
        field = "";
        i++;
      } else {
        field += ch;
        i++;
      }
    }
  }
  // Trailing field/row if no newline at end
  if (field.length > 0 || row.length > 0) {
    row.push(field);
    records.push(row);
  }
  return records;
}
