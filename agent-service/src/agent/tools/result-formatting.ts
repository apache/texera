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

import { OperatorResultMode, type OperatorExecutionSummary, type SampleRow } from "../../types/execution";
import {
  formatExecuteOperatorResult,
  formatSampleRowsAsTsv,
  getOperatorErrorText,
  getOperatorWarnings,
  getVisibleResultHeaders,
} from "./tools-utility";

export function formatOperatorResult(operatorId: string, opInfo: OperatorExecutionSummary): string {
  const errorText = getOperatorErrorText(opInfo);
  if (errorText) {
    return `[ERROR] ${errorText}`;
  }

  const sampleTuples = opInfo.resultSummary?.sampleTuples;
  if (!sampleTuples || !Array.isArray(sampleTuples)) {
    return "(no result data)";
  }

  const isViz = opInfo.resultSummary?.resultMode === OperatorResultMode.VISUALIZATION;
  const rows: SampleRow[] = isViz
    ? sampleTuples.map(({ rowIndex, tuple }) => {
        const cleaned: Record<string, any> = {};
        for (const key of Object.keys(tuple)) {
          if (key === "html-content" || key === "json-content") {
            cleaned[key] = "<skipped: visualization content>";
          } else {
            cleaned[key] = tuple[key];
          }
        }
        return { rowIndex, tuple: cleaned };
      })
    : sampleTuples;

  const headers = rows.length > 0 ? getVisibleResultHeaders(rows[0].tuple) : [];
  const columns = headers.length;

  const dataString = formatSampleRowsAsTsv(rows);

  // Output shape only; input-port shapes are derivable by the agent from the DAG
  // links plus each upstream operator's own output shape shown in context.
  const outputRows = opInfo.resultSummary?.tuplesCount ?? 0;
  const metadataLines = [`Output table shape: (${outputRows}, ${columns})`, ...getOperatorWarnings(opInfo)].filter(
    Boolean
  );

  const briefSummary = formatExecuteOperatorResult(operatorId);
  return [briefSummary, ...metadataLines, dataString].filter(Boolean).join("\n");
}
