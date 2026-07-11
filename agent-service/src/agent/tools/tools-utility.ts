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
