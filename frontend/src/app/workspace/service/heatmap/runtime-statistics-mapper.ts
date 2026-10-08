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

import { OperatorRuntimeStatus, OperatorState } from "../../types/execute-workflow.interface";
import { WorkflowRuntimeStatistics } from "../../../dashboard/type/workflow-runtime-statistics";

/**
 * Maps a persisted per-operator status code back to an OperatorState.
 *
 * The engine persists the codes written by Utils.maptoStatusCode: 0 =
 * Uninitialized/Ready, 1 = Running, 2 = Paused, 3 = Completed. Codes 4
 * (Failed), 5 (Killed) and -1 (other) have no OperatorState member, so they
 * fall back to the neutral Uninitialized rather than a wrong state.
 */
export function operatorStateFromStatusCode(code: number): OperatorState {
  switch (code) {
    case 0:
      return OperatorState.Uninitialized;
    case 1:
      return OperatorState.Running;
    case 2:
      return OperatorState.Paused;
    case 3:
      return OperatorState.Completed;
    default:
      return OperatorState.Uninitialized;
  }
}

/**
 * Reduces a persisted runtime-statistics time series to the latest snapshot
 * per operator (by timestamp; the later row wins a tie, matching write
 * order), mapped to the wire shape WorkflowStatusService ingests.
 *
 * The engine sums the ports away before persisting, so the port maps are left
 * absent rather than empty — an empty map would zero the port labels.
 */
export function toOperatorRuntimeStatusMap(rows: WorkflowRuntimeStatistics[]): Record<string, OperatorRuntimeStatus> {
  const latestByOperator: Record<string, WorkflowRuntimeStatistics> = {};
  for (const row of rows) {
    const seen = latestByOperator[row.operatorId];
    if (seen === undefined || row.timestamp >= seen.timestamp) {
      latestByOperator[row.operatorId] = row;
    }
  }

  const result: Record<string, OperatorRuntimeStatus> = {};
  for (const [operatorId, row] of Object.entries(latestByOperator)) {
    result[operatorId] = {
      operatorState: operatorStateFromStatusCode(row.status),
      aggregatedInputRowCount: row.inputTupleCount,
      aggregatedInputSize: row.inputTupleSize,
      aggregatedOutputRowCount: row.outputTupleCount,
      aggregatedOutputSize: row.outputTupleSize,
      numWorkers: row.numberOfWorkers,
      aggregatedDataProcessingTime: row.totalDataProcessingTime,
      aggregatedControlProcessingTime: row.totalControlProcessingTime,
      aggregatedIdleTime: row.totalIdleTime,
    };
  }
  return result;
}
