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
 * Reusable formatting functions for operator execution results.
 *
 * Extracted from execution-tools.ts so that the same OperatorInfo can be
 * formatted on demand with different settings (e.g., for DAG serialization
 * vs. tool result vs. context optimization).
 */

import type { OperatorInfo } from "../types/execution";
import type { WorkflowState } from "../workflow/workflow-state";
import {
  OperatorResultSerializationMode,
  DEFAULT_AGENT_SETTINGS,
} from "../types/agent";
import { formatExecuteOperatorResult } from "./tools-utility";

// ============================================================================
// Public API
// ============================================================================

export interface FormatOptions {
  /** Serialization mode for result data (TABLE, TOON, JSON). Default: TABLE */
  serializationMode?: OperatorResultSerializationMode;
  /** Max characters for the serialized data section. */
  maxCharLimit?: number;
  /** If true, include per-column statistics after the table. */
  carryMetadata?: boolean;
  /** If true, omit the Input/Output shape metadata line. */
  noExecutionMetadata?: boolean;
}

/**
 * Format an OperatorInfo into a human-readable result string.
 *
 * This is the single source of truth for serializing execution results.
 * Used by: tool results (addOperator auto-execute), DAG serialization,
 * and any future consumer that needs a text representation.
 *
 * @param operatorId   - The operator ID
 * @param opInfo       - Raw OperatorInfo from the backend
 * @param workflowState - Workflow state (for upstream operator names in shape line)
 * @param options      - Formatting options
 * @returns Formatted result string
 */
export function formatOperatorResult(
  operatorId: string,
  opInfo: OperatorInfo,
  workflowState: WorkflowState,
  options: FormatOptions = {}
): string {
  // If the operator has an error, surface it instead of result data
  if (opInfo.error) {
    return `[ERROR] ${opInfo.error}`;
  }

  const serializationMode = options.serializationMode ?? OperatorResultSerializationMode.TABLE;
  const charLimit = options.maxCharLimit ?? DEFAULT_AGENT_SETTINGS.maxOperatorResultCharLimit;

  if (!opInfo.result || !Array.isArray(opInfo.result)) {
    return "(no result data)";
  }

  const jsonArray = opInfo.result as Record<string, any>[];
  const headers = jsonArray.length > 0
    ? Object.keys(jsonArray[0]).filter(k => k !== "__row_index__" && k !== "__is_visualization__")
    : [];
  const columns = headers.length;

  // For visualization results, replace large html/json cells with a placeholder
  const isViz = jsonArray.length > 0 && jsonArray[0]["__is_visualization__"] === true;
  const serializableArray = isViz
    ? jsonArray.map(row => {
        const cleaned: Record<string, any> = {};
        for (const key of Object.keys(row)) {
          if (key === "__is_visualization__") continue;
          if (key === "html-content" || key === "json-content") {
            cleaned[key] = "<skipped: visualization content>";
          } else {
            cleaned[key] = row[key];
          }
        }
        return cleaned;
      })
    : jsonArray;

  // Serialize data
  let dataString = serializeData(serializableArray, serializationMode);

  // Truncate if needed
  if (dataString.length > charLimit) {
    dataString = truncateData(dataString, charLimit);
  }

  // Build metadata lines
  const metadataLines = options.noExecutionMetadata
    ? []
    : [formatInputOutput(workflowState, operatorId, opInfo, columns), ...(opInfo.warnings ?? [])].filter(Boolean);

  // Column stats
  const columnStatsLines = (options.carryMetadata && opInfo.resultStatistics)
    ? formatColumnStatsSection(opInfo.resultStatistics, headers)
    : [];

  const briefSummary = formatExecuteOperatorResult(operatorId);
  return [briefSummary, ...metadataLines, dataString, ...columnStatsLines].filter(Boolean).join("\n");
}

// ============================================================================
// Data Serialization
// ============================================================================

function serializeData(jsonArray: Record<string, any>[], mode: OperatorResultSerializationMode): string {
  switch (mode) {
    case OperatorResultSerializationMode.TABLE:
      return jsonToTableFormat(jsonArray);
    case OperatorResultSerializationMode.TOON: {
      // Dynamic import to avoid bundling toon if not used
      try {
        const { toToonFormat } = require("@toon-format/toon");
        return toToonFormat(jsonArray);
      } catch {
        return jsonToTableFormat(jsonArray);
      }
    }
    case OperatorResultSerializationMode.JSON:
    default:
      return JSON.stringify(jsonArray);
  }
}

function truncateData(dataString: string, charLimit: number): string {
  const allLines = dataString.split("\n");
  const headerLine = allLines[0];
  const dataRows = allLines.slice(1);

  const reservedSize = headerLine.length + 1;
  const halfLimit = Math.floor((charLimit - reservedSize) / 2);

  let frontSize = 0;
  const frontRows: string[] = [];
  for (const row of dataRows) {
    const rowLen = row.length + 1;
    if (frontSize + rowLen > halfLimit && frontRows.length > 0) break;
    frontRows.push(row);
    frontSize += rowLen;
  }

  let backSize = 0;
  const backRows: string[] = [];
  for (let i = dataRows.length - 1; i >= frontRows.length; i--) {
    const rowLen = dataRows[i].length + 1;
    if (backSize + rowLen > halfLimit && backRows.length > 0) break;
    backRows.unshift(dataRows[i]);
    backSize += rowLen;
  }

  return [headerLine, ...frontRows, ...backRows].join("\n");
}

// ============================================================================
// Metadata Formatting
// ============================================================================

function formatInputOutput(
  workflowState: WorkflowState,
  operatorId: string,
  opInfo: OperatorInfo,
  outputColumns: number
): string {
  const outputRows = opInfo.totalRowCount ?? opInfo.outputTuples;
  const outputLine = `Output table shape: (${outputRows}, ${outputColumns})`;

  const inputShapes = opInfo.inputPortShapes;
  if (!inputShapes || inputShapes.length === 0) {
    return outputLine;
  }

  const inputLinks = workflowState.getAllLinks().filter(l => l.target.operatorID === operatorId);
  const portIndexToUpstream = new Map<number, string>();
  const op = workflowState.getOperator(operatorId);
  for (const link of inputLinks) {
    const portIdx = op?.inputPorts.findIndex(p => p.portID === link.target.portID) ?? -1;
    if (portIdx >= 0) {
      portIndexToUpstream.set(portIdx, link.source.operatorID);
    }
  }

  const inputPart = inputShapes
    .sort((a, b) => a.portIndex - b.portIndex)
    .map(p => {
      const name = portIndexToUpstream.get(p.portIndex) ?? `input${p.portIndex}`;
      return `${name}(${p.rows}, ${p.columns})`;
    })
    .join(", ");

  return `Input operator(table shape): ${inputPart}\n${outputLine}`;
}

// ============================================================================
// Column Stats Formatting
// ============================================================================

const MAX_STATS_COLUMNS = 50;
const EXCLUDED_STAT_KEYS = new Set(["count", "std", "p25", "median", "p75"]);
const STAT_PRECISION = 4;

function formatStatValue(v: any): string {
  if (v === null || v === undefined) return "N/A";
  if (typeof v === "number" && !Number.isInteger(v)) {
    return Number(v.toPrecision(STAT_PRECISION)).toString();
  }
  return String(v);
}

function typePriority(dataType: string): number {
  const t = dataType.toLowerCase();
  if (t === "bool" || t === "boolean") return 0;
  if (t === "str" || t === "string" || t === "object") return 1;
  if (t.startsWith("date") || t === "datetime") return 2;
  if (t === "int" || t === "integer" || t === "int64" || t === "int32") return 3;
  if (t === "float" || t === "numeric" || t === "float64" || t === "float32" || t === "number") return 4;
  return 5;
}

export function formatColumnStatsSection(resultStatistics: Record<string, string>, headers?: string[]): string[] {
  const parsed: Array<{ colName: string; dataType: string; kvPairs: string }> = [];

  const columnNames = headers ?? Object.keys(resultStatistics);
  for (const colName of columnNames) {
    const statsJson = resultStatistics[colName];
    if (!statsJson) continue;
    try {
      const p = JSON.parse(statsJson);
      const dataType: string = p.data_type ?? "unknown";
      const stats: Record<string, any> = p.statistics ?? {};

      const kvPairs = Object.entries(stats)
        .filter(([k, v]) => v !== null && v !== undefined && !EXCLUDED_STAT_KEYS.has(k))
        .map(([k, v]) => {
          if (k === "top_10" && typeof v === "object") {
            const inner = Object.entries(v)
              .map(([ik, iv]) => `"${ik}"=${formatStatValue(iv)}`)
              .join(", ");
            return `top_10={${inner}}`;
          }
          if (typeof v === "object") return null;
          return `${k}=${formatStatValue(v)}`;
        })
        .filter(Boolean)
        .join(", ");

      parsed.push({ colName, dataType, kvPairs });
    } catch {
      // skip unparseable columns
    }
  }

  if (parsed.length === 0) return [];

  parsed.sort((a, b) => typePriority(a.dataType) - typePriority(b.dataType));

  const totalColumns = parsed.length;
  const truncated = totalColumns > MAX_STATS_COLUMNS;
  const shown = truncated ? parsed.slice(0, MAX_STATS_COLUMNS) : parsed;

  const header = truncated
    ? `Column Stats (showing ${MAX_STATS_COLUMNS} of ${totalColumns} columns):`
    : `Column Stats:`;

  const lines = shown.map(({ colName, dataType, kvPairs }) =>
    kvPairs ? `- "${colName}" (${dataType}): ${kvPairs}` : `- "${colName}" (${dataType})`
  );

  return [header, ...lines];
}

// ============================================================================
// Table Format (pandas-style)
// ============================================================================

/**
 * Convert JSON result to pandas DataFrame-style table format (tab-separated).
 * Uses __row_index__ from the backend to preserve original row indices and
 * inserts "..." separator rows when there are gaps in indices.
 */
function jsonToTableFormat(jsonResult: Record<string, any>[]): string {
  if (!jsonResult || jsonResult.length === 0) return "";

  const hasRowIndex = "__row_index__" in jsonResult[0];
  const headers = Object.keys(jsonResult[0]).filter(h => h !== "__row_index__");
  if (headers.length === 0) return "";

  const headerLine = "\t" + headers.join("\t");
  const formattedRows: string[] = [];
  let prevIndex = -1;

  for (let i = 0; i < jsonResult.length; i++) {
    const row = jsonResult[i];
    const rowIndex = hasRowIndex ? (row["__row_index__"] as number) : i;

    // Detect gap in row indices — insert pandas-style "..." separator
    if (prevIndex >= 0 && rowIndex > prevIndex + 1) {
      const dots = headers.map(() => "...").join("\t");
      formattedRows.push(`...\t${dots}`);
    }
    prevIndex = rowIndex;

    const cells = headers.map(h => {
      const val = row[h];
      if (val === null) return "NaN";
      if (val === undefined) return "";
      if (typeof val === "number" || typeof val === "boolean") return String(val);
      if (typeof val === "string") {
        if (val === "NULL") return "NaN";
        return val.replace(/\t/g, "\\t").replace(/\n/g, "\\n");
      }
      return JSON.stringify(val);
    });
    formattedRows.push(`${rowIndex}\t${cells.join("\t")}`);
  }

  return [headerLine, ...formattedRows].join("\n");
}
