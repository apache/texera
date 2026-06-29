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
import { formatExecuteOperatorResult, getOperatorWarnings, getVisibleResultHeaders } from "./tools-utility";

export function formatOperatorResult(operatorId: string, opInfo: OperatorExecutionSummary): string {
  const errorText = opInfo.errorMessages.map(e => e.message).join("; ");
  if (errorText) {
    return `[ERROR] ${errorText}`;
  }

  const sampleTuples = opInfo.resultSummary?.sampleTuples;
  if (!sampleTuples || !Array.isArray(sampleTuples)) {
    return "(no result data)";
  }

  const headers = sampleTuples.length > 0 ? getVisibleResultHeaders(sampleTuples[0].tuple) : [];
  const columns = headers.length;

  const isViz = sampleTuples.length > 0 && sampleTuples[0].tuple["__is_visualization__"] === true;
  const rows: SampleRow[] = isViz
    ? sampleTuples.map(({ rowIndex, tuple }) => {
        const cleaned: Record<string, any> = {};
        for (const key of Object.keys(tuple)) {
          if (key === "__is_visualization__") continue;
          if (key === "html-content" || key === "json-content") {
            cleaned[key] = "<skipped: visualization content>";
          } else {
            cleaned[key] = tuple[key];
          }
        }
        return { rowIndex, tuple: cleaned };
      })
    : sampleTuples;

  const dataString = jsonToTableFormat(rows);

  // Output shape only; input-port shapes are derivable by the agent from the DAG
  // links plus each upstream operator's own output shape shown in context.
  const outputRows = opInfo.resultSummary?.totalRowCount ?? 0;
  const metadataLines = [`Output table shape: (${outputRows}, ${columns})`, ...getOperatorWarnings(opInfo)].filter(
    Boolean
  );

  const briefSummary = formatExecuteOperatorResult(operatorId);
  return [briefSummary, ...metadataLines, dataString].filter(Boolean).join("\n");
}

function jsonToTableFormat(rows: SampleRow[]): string {
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
