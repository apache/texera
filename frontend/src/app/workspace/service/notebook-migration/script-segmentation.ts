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

import { v4 as uuidv4 } from "uuid";

/**
 * Turns a Python script into notebook-style cells using the line ranges the LLM reported
 * for each UDF.
 *
 * A notebook arrives already split into cells, and those cells are the join key for the
 * cell<->operator mapping that drives highlighting. A script has no such boundaries, so the
 * model is asked which lines it turned into which UDF, and the cells are derived from that
 * answer here.
 *
 * The model's answer is untrusted: ranges can arrive reversed, overlapping, out of order,
 * past the end of the file, or missing entirely. Every case is reconciled rather than
 * rejected, because the ranges arrive alongside a workflow that already cost a full
 * conversion, and a degraded mapping is worth more than a discarded result.
 *
 * Guarantees, given a non-empty script:
 *   - Every line with content lands in exactly one cell, whether or not a UDF claimed it.
 *   - Cells are disjoint and ordered by position in the file.
 *   - No cell straddles a reported boundary, so a cell claimed by a UDF is claimed whole.
 *   - Lines two UDFs both claim become one shared cell that maps to both, which is what
 *     `cell_to_operator` already expresses for notebooks.
 */

// A 1-indexed, inclusive span of source lines.
interface LineRange {
  start: number;
  end: number;
}

export interface DerivedCell {
  uuid: string;
  source: string;
  // 1-indexed and inclusive, retained so callers can report or debug the split.
  startLine: number;
  endLine: number;
}

export interface ScriptSegmentation {
  cells: DerivedCell[];
  // UDF id -> the uuids of the cells it covers. A UDF whose ranges were all unusable is absent.
  udfToCellUuids: Record<string, string[]>;
}

/**
 * Splits into lines, tolerating CRLF and a trailing newline (which is a terminator, not a line).
 *
 * Exported because the caller numbers these same lines when it builds the prompt. The numbers the
 * model reports back are only meaningful if both sides agree on what counts as a line, so they must
 * share one definition rather than each keep their own.
 */
export function splitScriptLines(source: string): string[] {
  const lines = source.replace(/\r\n?/g, "\n").split("\n");
  if (lines.length > 0 && lines[lines.length - 1] === "") {
    lines.pop();
  }
  return lines;
}

// Accepts a number or a numeric string, since models drift between the two.
function parseBound(value: unknown): number | null {
  if (typeof value === "number") {
    return Number.isFinite(value) ? Math.trunc(value) : null;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? Math.trunc(parsed) : null;
  }
  return null;
}

// Reads one range in either the [start, end] or { start, end } form.
function toLineRange(raw: unknown): LineRange | null {
  let rawStart: unknown;
  let rawEnd: unknown;

  if (Array.isArray(raw)) {
    if (raw.length !== 2) return null;
    [rawStart, rawEnd] = raw;
  } else if (typeof raw === "object" && raw !== null) {
    ({ start: rawStart, end: rawEnd } = raw as { start?: unknown; end?: unknown });
  } else {
    return null;
  }

  const start = parseBound(rawStart);
  const end = parseBound(rawEnd);
  if (start === null || end === null) return null;

  // A reversed range still names the span the model meant, so read it rather than drop it.
  return start <= end ? { start, end } : { start: end, end: start };
}

// A UDF's ranges may arrive as a list of ranges, a single bare [start, end] pair, or one
// { start, end } object. Normalizes all three to a list before parsing.
function toRangeList(raw: unknown): unknown[] {
  if (Array.isArray(raw)) {
    const isBarePair = raw.length === 2 && raw.every(v => typeof v === "number" || typeof v === "string");
    return isBarePair ? [raw] : raw;
  }
  return typeof raw === "object" && raw !== null ? [raw] : [];
}

function clampToSource(range: LineRange, lineCount: number): LineRange | null {
  const start = Math.max(range.start, 1);
  const end = Math.min(range.end, lineCount);
  // Null when the range sat entirely past either end of the file.
  return start <= end ? { start, end } : null;
}

/**
 * @param source     the raw script contents
 * @param rawRanges  the model's reply, UDF id -> reported line ranges, unvalidated
 * @param newUuid    cell id factory, overridden by tests to keep output deterministic
 */
export function segmentScript(
  source: string,
  rawRanges: Record<string, unknown> | null | undefined,
  newUuid: () => string = uuidv4
): ScriptSegmentation {
  const lines = splitScriptLines(source);
  if (lines.length === 0) {
    return { cells: [], udfToCellUuids: {} };
  }

  const udfRanges = new Map<string, LineRange[]>();
  for (const [udfId, raw] of Object.entries(rawRanges ?? {})) {
    const ranges = toRangeList(raw)
      .map(toLineRange)
      .filter((range): range is LineRange => range !== null)
      .map(range => clampToSource(range, lines.length))
      .filter((range): range is LineRange => range !== null);
    if (ranges.length > 0) {
      udfRanges.set(udfId, ranges);
    }
  }

  // Cut the file at every reported boundary. Slicing on the union of boundaries is what makes
  // overlaps and gaps fall out on their own: the result is disjoint, covers every line, and no
  // segment can span a boundary, so range membership below is an exact containment test.
  const boundaries = new Set<number>([1, lines.length + 1]);
  for (const ranges of udfRanges.values()) {
    for (const { start, end } of ranges) {
      boundaries.add(start);
      boundaries.add(end + 1);
    }
  }
  const cutPoints = [...boundaries].sort((a, b) => a - b);

  const cells: DerivedCell[] = [];
  for (let i = 0; i < cutPoints.length - 1; i++) {
    const startLine = cutPoints[i];
    const endLine = cutPoints[i + 1] - 1;
    const text = lines.slice(startLine - 1, endLine).join("\n");
    // A span of only blank lines would render as an empty Jupyter cell, so it is dropped.
    // Nothing is lost: every line carrying content still lands in a cell.
    if (text.trim() === "") continue;
    cells.push({ uuid: newUuid(), source: text, startLine, endLine });
  }

  const udfToCellUuids: Record<string, string[]> = {};
  for (const [udfId, ranges] of udfRanges) {
    const uuids = cells
      .filter(cell => ranges.some(range => cell.startLine >= range.start && cell.endLine <= range.end))
      .map(cell => cell.uuid);
    if (uuids.length > 0) {
      udfToCellUuids[udfId] = uuids;
    }
  }

  return { cells, udfToCellUuids };
}
