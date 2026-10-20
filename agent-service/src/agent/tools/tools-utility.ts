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

import type { IndexedTuple, OperatorExecutionSummary, Tuple, WebOutputMode } from "../../types/execution";

// The single definition of "this operator failed": some fatal error carries
// message text. The engine can emit console ERRORs with empty text, which do
// not count, matching the previous `error` field's truthiness semantics.
export function getOperatorErrorText(opInfo: OperatorExecutionSummary): string {
  return opInfo.errorMessages
    .map(e => e.message)
    .filter(Boolean)
    .join("; ");
}

// The column names of a tuple, in schema order.
export function tupleColumns(tuple: Tuple): string[] {
  return tuple.schema.attributes.map(a => a.attributeName);
}

// Project a tuple's positional fields back into a column->value record.
export function tupleToRecord(tuple: Tuple): Record<string, unknown> {
  const record: Record<string, unknown> = {};
  tuple.schema.attributes.forEach((a, i) => {
    record[a.attributeName] = tuple.fields[i];
  });
  return record;
}

// Keep visualization payloads in stored results for frontend rendering, but do not
// send their potentially large HTML/JSON bodies to the LLM as tool or DAG context.
export function redactVisualizationPayloads(
  sampleTuples: ReadonlyArray<IndexedTuple>,
  resultMode: WebOutputMode
): ReadonlyArray<IndexedTuple> {
  if (resultMode.type !== "SetSnapshotMode") return sampleTuples;

  return sampleTuples.map(([rowIndex, tuple]) => {
    const fields = tuple.schema.attributes.map((attribute, index) =>
      attribute.attributeName === "html-content" || attribute.attributeName === "json-content"
        ? "<skipped: visualization content>"
        : tuple.fields[index]
    );
    return [rowIndex, { schema: tuple.schema, fields }] as const;
  });
}

// Sampled tuples arrive paired with their original row indices.
export function formatSampleTuplesAsTsv(sampleTuples: ReadonlyArray<IndexedTuple>): string {
  if (!sampleTuples || sampleTuples.length === 0) return "";

  const headers = tupleColumns(sampleTuples[0][1]);
  if (headers.length === 0) return "";

  const headerLine = "\t" + headers.join("\t");
  const formattedRows: string[] = [];
  let prevIndex = -1;

  for (const [rowIndex, tuple] of sampleTuples) {
    if (prevIndex >= 0 && rowIndex > prevIndex + 1) {
      const dots = headers.map(() => "...").join("\t");
      formattedRows.push(`...\t${dots}`);
    }
    prevIndex = rowIndex;

    const record = tupleToRecord(tuple);
    const cells = headers.map(h => {
      const val = record[h];
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

// The agent-facing rendering of an operator's materialized result, shared by the
// execute tool and the DAG context: a brief summary, the output shape, warnings,
// then the sampled tuples as TSV with visualization payloads redacted. With a
// `charLimit`, the TSV keeps its head and tail within that many characters.
export function formatOperatorOutput(operatorId: string, opInfo: OperatorExecutionSummary, charLimit?: number): string {
  const resultSummary = opInfo.resultSummary;
  if (!resultSummary) {
    return "(no result data)";
  }

  const displayTuples = redactVisualizationPayloads(resultSummary.sampleTuples, resultSummary.resultMode);
  const columns = displayTuples.length > 0 ? tupleColumns(displayTuples[0][1]).length : 0;
  const tsv = formatSampleTuplesAsTsv(displayTuples);

  // Output shape only: the agent executes every operator it adds or modifies, so an
  // upstream operator's own output shape (this operator's input) is normally
  // already in the DAG context.
  const shapeLine = `Output table shape: (${resultSummary.totalTuplesCount}, ${columns})`;

  return [
    formatExecuteOperatorResult(operatorId),
    shapeLine,
    ...getOperatorWarnings(opInfo),
    charLimit === undefined ? tsv : truncateTsv(tsv, charLimit),
  ]
    .filter(Boolean)
    .join("\n");
}

// Keep a TSV within `charLimit` characters by dropping rows from its middle: the
// header line plus as many leading and trailing rows as fit in half the budget each.
function truncateTsv(tsv: string, charLimit: number): string {
  if (tsv.length <= charLimit) {
    return tsv;
  }

  const allLines = tsv.split("\n");
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

  const keptRows = [...frontRows, ...backRows];
  return [headerLine, ...keptRows].join("\n");
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
