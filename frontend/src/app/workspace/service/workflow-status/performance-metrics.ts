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

import { OperatorStatistics } from "../../types/execute-workflow.interface";

/**
 * Derived per-operator performance metrics.
 *
 * This is the ground-truth model captured by {@link WorkflowStatusService}. It is
 * a flat projection of the raw {@link OperatorStatistics} the backend streams over
 * the websocket, with missing optional fields defaulted. Keyed by operator id at
 * the map level (mirroring {@link OperatorStatistics}), so the id is not repeated here.
 */
export interface OperatorPerformanceMetrics
  extends Readonly<{
    dataProcessingTimeNs: number;
    controlProcessingTimeNs: number;
    idleTimeNs: number;
    inputRows: number;
    outputRows: number;
    inputSize: number;
    outputSize: number;
    numWorkers: number;
  }> {}

/**
 * Project a single raw {@link OperatorStatistics} into the flat performance model.
 *
 * Several fields are optional on {@link OperatorStatistics} because the frontend
 * builds partial objects (e.g. WorkflowStatusService.resetStatus, which omits the
 * timing/size/worker fields). A missing metric defaults to 0; a missing worker
 * count defaults to 1, since an operator always runs on at least one worker.
 * Data and control processing time are kept separate so consumers can choose how
 * to combine them.
 */
export function extractPerformanceMetrics(stats: OperatorStatistics): OperatorPerformanceMetrics {
  return {
    dataProcessingTimeNs: stats.aggregatedDataProcessingTime ?? 0,
    controlProcessingTimeNs: stats.aggregatedControlProcessingTime ?? 0,
    idleTimeNs: stats.aggregatedIdleTime ?? 0,
    inputRows: stats.aggregatedInputRowCount,
    outputRows: stats.aggregatedOutputRowCount,
    inputSize: stats.aggregatedInputSize ?? 0,
    outputSize: stats.aggregatedOutputSize ?? 0,
    numWorkers: stats.numWorkers ?? 1,
  };
}
