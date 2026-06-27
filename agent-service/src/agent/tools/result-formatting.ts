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

import type { OperatorInfo } from "../../types/execution";
import type { WorkflowState } from "../workflow-state";
import {
  formatExecuteOperatorResult,
  getVisibleResultHeaders,
  formatOperatorIoShape,
  formatRecordsAsTable,
} from "./tools-utility";

export function formatOperatorResult(operatorId: string, opInfo: OperatorInfo, workflowState: WorkflowState): string {
  if (opInfo.error) {
    return `[ERROR] ${opInfo.error}`;
  }

  if (!opInfo.result || !Array.isArray(opInfo.result)) {
    return "(no result data)";
  }

  const jsonArray = opInfo.result as Record<string, any>[];
  const headers = jsonArray.length > 0 ? getVisibleResultHeaders(jsonArray[0]) : [];
  const columns = headers.length;

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

  const dataString = formatRecordsAsTable(serializableArray);

  const metadataLines = [
    formatOperatorIoShape(workflowState, operatorId, opInfo, columns),
    ...(opInfo.warnings ?? []),
  ].filter(Boolean);

  const briefSummary = formatExecuteOperatorResult(operatorId);
  return [briefSummary, ...metadataLines, dataString].filter(Boolean).join("\n");
}
