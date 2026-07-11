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

import type { OperatorExecutionSummary } from "../../types/execution";
import {
  formatExecuteOperatorResult,
  formatSampleRowsAsTsv,
  getOperatorErrorText,
  getOperatorWarnings,
  redactVisualizationPayloads,
  tupleColumns,
} from "./tools-utility";

export function formatOperatorResult(operatorId: string, opInfo: OperatorExecutionSummary): string {
  const errorText = getOperatorErrorText(opInfo);
  if (errorText) {
    return `[ERROR] ${errorText}`;
  }

  const resultSummary = opInfo.resultSummary;
  const sampleTuples = resultSummary?.sampleTuples;
  if (!sampleTuples || !Array.isArray(sampleTuples)) {
    return "(no result data)";
  }

  const rows = redactVisualizationPayloads(sampleTuples, resultSummary.resultMode);

  const headers = rows.length > 0 ? tupleColumns(rows[0][1]) : [];
  const columns = headers.length;

  const dataString = formatSampleRowsAsTsv(rows);

  // Output shape only; input-port shapes are derivable by the agent from the DAG
  // links plus each upstream operator's own output shape shown in context.
  const outputRows = resultSummary.totalTuplesCount;
  const metadataLines = [`Output table shape: (${outputRows}, ${columns})`, ...getOperatorWarnings(opInfo)].filter(
    Boolean
  );

  const briefSummary = formatExecuteOperatorResult(operatorId);
  return [briefSummary, ...metadataLines, dataString].filter(Boolean).join("\n");
}
