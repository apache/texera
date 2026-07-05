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

import type { OperatorExecutionSummary, SampleRow } from "../../types/execution";

// The single definition of "this operator failed": some fatal error carries
// message text. The engine can emit console ERRORs with empty text, which do
// not count, matching the previous `error` field's truthiness semantics.
export function getOperatorErrorText(opInfo: OperatorExecutionSummary): string {
  return opInfo.errorMessages
    .map(e => e.message)
    .filter(Boolean)
    .join("; ");
}

export function getVisibleResultHeaders(row: Record<string, any>): string[] {
  return Object.keys(row);
}

export function formatSampleRowsAsTsv(rows: SampleRow[]): string {
  if (!rows || rows.length === 0) return "";

  const headers = getVisibleResultHeaders(rows[0].tuple);
  if (headers.length === 0) return "";

  const headerLine = "\t" + headers.join("\t");
  const formattedRows: string[] = [];
  let prevIndex = -1;

  for (const { rowIndex, tuple } of rows) {
    if (prevIndex >= 0 && rowIndex > prevIndex + 1) {
      const dots = headers.map(() => "...").join("\t");
      formattedRows.push(`...\t${dots}`);
    }
    prevIndex = rowIndex;

    const cells = headers.map(h => {
      const val = tuple[h];
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

// Warnings are the console messages the engine tags with a "WARNING: " title
// prefix; derive them rather than carrying a separate field on the summary.
export function getOperatorWarnings(opInfo: OperatorExecutionSummary): string[] {
  return (opInfo.consoleMessages ?? []).filter(m => m.title.startsWith("WARNING: ")).map(m => m.title);
}

export function createToolResult(message: string): string {
  return message;
}

export function createErrorResult(error: string): string {
  return `[ERROR] ${error}`;
}

function formatLinkDescription(sourceOperatorId: string, targetOperatorId: string): string {
  return `${sourceOperatorId} --> ${targetOperatorId}`;
}

export function formatAddOperatorResult(
  operatorId: string,
  numInputPorts: number,
  numOutputPorts: number,
  createdLinks?: { source: string; target: string }[],
  deletedLinks?: { source: string; target: string }[]
): string {
  let summary = `Added operator ${operatorId}, input ports: ${numInputPorts}, output ports: ${numOutputPorts}`;
  if (deletedLinks && deletedLinks.length > 0) {
    summary += `, deleted links: [${deletedLinks.map(l => formatLinkDescription(l.source, l.target)).join(", ")}]`;
  }
  if (createdLinks && createdLinks.length > 0) {
    summary += `, created links: [${createdLinks.map(l => formatLinkDescription(l.source, l.target)).join(", ")}]`;
  }
  return summary;
}

export function formatModifyOperatorResult(
  operatorId: string,
  createdLinks?: { source: string; target: string }[],
  deletedLinks?: { source: string; target: string }[]
): string {
  let summary = `Operator ${operatorId} modified`;
  if (deletedLinks && deletedLinks.length > 0) {
    summary += `, deleted links: [${deletedLinks.map(l => formatLinkDescription(l.source, l.target)).join(", ")}]`;
  }
  if (createdLinks && createdLinks.length > 0) {
    summary += `, created links: [${createdLinks.map(l => formatLinkDescription(l.source, l.target)).join(", ")}]`;
  }
  return summary;
}

export function formatExecuteOperatorResult(operatorId: string): string {
  return `Executed operator ${operatorId}`;
}

export function formatOperatorError(operatorId: string, error: string): string {
  return `Error on operator ${operatorId}: ${error}`;
}
