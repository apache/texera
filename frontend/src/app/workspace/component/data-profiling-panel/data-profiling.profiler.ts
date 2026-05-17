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

import {
  ColumnDtype,
  ColumnProfile,
  CorrelationCell,
  DatasetProfile,
  TopValue,
} from "./data-profiling.types";

const HISTOGRAM_BINS = 10;
const TOP_VALUES_KEEP = 5;
const CATEGORICAL_UNIQUE_THRESHOLD = 50; // <= this many uniques + non-numeric => categorical
const MIN_NUMERIC_RATIO = 0.8; // ≥ 80% of non-missing values must parse as numbers
const DATE_TEST_LIMIT = 50; // how many sample values to check for date-likeness

/**
 * Parsed CSV rows from papaparse, header-keyed.
 * Values arrive as strings from `header: true, dynamicTyping: false`.
 */
export type ParsedRow = Record<string, string | null | undefined>;

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$/;
const US_DATE_RE = /^\d{1,2}\/\d{1,2}\/\d{2,4}$/;

function isMissing(v: string | null | undefined): boolean {
  if (v === null || v === undefined) return true;
  const s = String(v).trim();
  if (s === "") return true;
  const lower = s.toLowerCase();
  return lower === "na" || lower === "n/a" || lower === "null" || lower === "nan";
}

function tryParseNumber(v: string): number | null {
  const s = v.trim();
  if (s === "") return null;
  // Strip a single trailing %, $, commas in thousands
  const cleaned = s.replace(/,/g, "");
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function looksLikeDate(v: string): boolean {
  return ISO_DATE_RE.test(v) || US_DATE_RE.test(v);
}

function inferColumnDtype(values: string[]): ColumnDtype {
  if (values.length === 0) return "text";

  // Try numeric
  let numericCount = 0;
  for (const v of values) {
    if (tryParseNumber(v) !== null) numericCount++;
  }
  if (numericCount / values.length >= MIN_NUMERIC_RATIO) {
    return "numeric";
  }

  // Try datetime on a small sample
  const sample = values.slice(0, DATE_TEST_LIMIT);
  let dateCount = 0;
  for (const v of sample) {
    if (looksLikeDate(v)) dateCount++;
  }
  if (sample.length > 0 && dateCount / sample.length >= MIN_NUMERIC_RATIO) {
    return "datetime";
  }

  // Boolean check (small set of true/false-like values)
  const lowered = new Set(values.slice(0, 200).map(v => v.trim().toLowerCase()));
  const boolTokens = new Set(["true", "false", "0", "1", "yes", "no", "y", "n", "t", "f"]);
  if (lowered.size <= 2 && [...lowered].every(v => boolTokens.has(v))) {
    return "boolean";
  }

  // Categorical vs free text
  const uniques = new Set(values);
  if (uniques.size <= CATEGORICAL_UNIQUE_THRESHOLD || uniques.size / values.length < 0.5) {
    return "categorical";
  }
  return "text";
}

function percentile(sortedAsc: number[], p: number): number {
  if (sortedAsc.length === 0) return NaN;
  if (sortedAsc.length === 1) return sortedAsc[0];
  const idx = (sortedAsc.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sortedAsc[lo];
  return sortedAsc[lo] + (sortedAsc[hi] - sortedAsc[lo]) * (idx - lo);
}

function buildHistogram(sortedAsc: number[], min: number, max: number): number[] {
  const bins = new Array<number>(HISTOGRAM_BINS).fill(0);
  if (sortedAsc.length === 0 || min === max) {
    bins[0] = sortedAsc.length;
    return bins;
  }
  const width = (max - min) / HISTOGRAM_BINS;
  for (const v of sortedAsc) {
    let idx = Math.floor((v - min) / width);
    if (idx >= HISTOGRAM_BINS) idx = HISTOGRAM_BINS - 1;
    if (idx < 0) idx = 0;
    bins[idx]++;
  }
  return bins;
}

function topValueCounts(values: string[]): TopValue[] {
  const counts = new Map<string, number>();
  for (const v of values) counts.set(v, (counts.get(v) ?? 0) + 1);
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, TOP_VALUES_KEEP)
    .map(([value, count]) => ({ value, count }));
}

function pearson(a: number[], b: number[]): number {
  const n = Math.min(a.length, b.length);
  if (n < 2) return 0;
  let sumA = 0;
  let sumB = 0;
  for (let i = 0; i < n; i++) {
    sumA += a[i];
    sumB += b[i];
  }
  const meanA = sumA / n;
  const meanB = sumB / n;
  let num = 0;
  let denA = 0;
  let denB = 0;
  for (let i = 0; i < n; i++) {
    const da = a[i] - meanA;
    const db = b[i] - meanB;
    num += da * db;
    denA += da * da;
    denB += db * db;
  }
  const den = Math.sqrt(denA * denB);
  if (den === 0) return 0;
  const r = num / den;
  // clamp tiny floating-point drift
  return Math.max(-1, Math.min(1, r));
}

function computeColumnProfile(name: string, raw: (string | null | undefined)[]): ColumnProfile {
  const count = raw.length;
  let missing = 0;
  const present: string[] = [];
  for (const v of raw) {
    if (isMissing(v)) {
      missing++;
    } else {
      present.push(String(v));
    }
  }

  const dtype = inferColumnDtype(present);
  const unique = new Set(present).size;
  const missingPercent = count > 0 ? (missing / count) * 100 : 0;

  const base: ColumnProfile = {
    name,
    dtype,
    count,
    missing,
    missingPercent,
    unique,
  };

  if (dtype === "numeric") {
    const nums: number[] = [];
    for (const v of present) {
      const n = tryParseNumber(v);
      if (n !== null) nums.push(n);
    }
    if (nums.length === 0) return base;

    const sorted = [...nums].sort((a, b) => a - b);
    const min = sorted[0];
    const max = sorted[sorted.length - 1];
    const sum = nums.reduce((s, x) => s + x, 0);
    const mean = sum / nums.length;
    const median = percentile(sorted, 0.5);
    const variance = nums.reduce((s, x) => s + (x - mean) ** 2, 0) / Math.max(1, nums.length - 1);
    const std = Math.sqrt(variance);
    // Count >3σ outliers
    let outlierCount = 0;
    if (std > 0) {
      for (const v of nums) {
        if (Math.abs(v - mean) > 3 * std) outlierCount++;
      }
    }
    const histogram = buildHistogram(sorted, min, max);

    return {
      ...base,
      mean,
      median,
      std,
      min,
      max,
      outlierCount,
      histogram,
    };
  }

  if (dtype === "categorical" || dtype === "boolean") {
    return { ...base, topValues: topValueCounts(present) };
  }

  return base;
}

function countDuplicateRows(rows: ParsedRow[], headers: string[]): number {
  if (rows.length === 0) return 0;
  const seen = new Set<string>();
  let dupes = 0;
  for (const row of rows) {
    const key = headers.map(h => String(row[h] ?? "")).join("\x1f");
    if (seen.has(key)) dupes++;
    else seen.add(key);
  }
  return dupes;
}

function pickNumericForCorrelation(columns: ColumnProfile[], cap: number = 8): string[] {
  return columns
    .filter(c => c.dtype === "numeric")
    .map(c => c.name)
    .slice(0, cap);
}

function computeCorrelations(
  rows: ParsedRow[],
  numericCols: string[]
): CorrelationCell[] {
  if (numericCols.length < 2) return [];

  // Build aligned numeric arrays, dropping rows where any selected col is missing/non-numeric
  const parsed: Record<string, number[]> = Object.fromEntries(numericCols.map(c => [c, []]));
  for (const row of rows) {
    const vals: Record<string, number> = {};
    let ok = true;
    for (const c of numericCols) {
      const raw = row[c];
      if (isMissing(raw)) {
        ok = false;
        break;
      }
      const n = tryParseNumber(String(raw));
      if (n === null) {
        ok = false;
        break;
      }
      vals[c] = n;
    }
    if (!ok) continue;
    for (const c of numericCols) parsed[c].push(vals[c]);
  }

  const out: CorrelationCell[] = [];
  for (let i = 0; i < numericCols.length; i++) {
    for (let j = i + 1; j < numericCols.length; j++) {
      const a = numericCols[i];
      const b = numericCols[j];
      const r = pearson(parsed[a], parsed[b]);
      // Round to 3dp; drop pairs that came out as exactly 0 with no data
      if (parsed[a].length > 0) {
        out.push({ a, b, r: Math.round(r * 1000) / 1000 });
      }
    }
  }
  return out;
}

/**
 * Produces a DatasetProfile from header-keyed CSV rows.
 * Strings are accepted as-is; numeric coercion happens here.
 */
export function computeProfile(
  source: string,
  headers: string[],
  rows: ParsedRow[]
): DatasetProfile {
  const columns: ColumnProfile[] = headers.map(h =>
    computeColumnProfile(
      h,
      rows.map(r => r[h])
    )
  );
  const duplicateRows = countDuplicateRows(rows, headers);
  const numericForCorr = pickNumericForCorrelation(columns);
  const correlations = computeCorrelations(rows, numericForCorr);

  return {
    source,
    rowCount: rows.length,
    duplicateRows,
    columns,
    correlations,
  };
}
