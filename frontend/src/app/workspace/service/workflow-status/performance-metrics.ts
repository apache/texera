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
 * a flat, defensively-defaulted projection of the raw {@link OperatorStatistics}
 * the backend streams over the websocket — every field is a finite, non-negative
 * number, so downstream consumers never have to re-validate.
 */
export interface OperatorPerformanceMetrics
  extends Readonly<{
    operatorId: string;
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
 * Coerce an untrusted numeric field (it arrives over the websocket) into a
 * finite, non-negative number. Anything missing, non-numeric, NaN, infinite, or
 * negative collapses to 0 so no NaN/Infinity can leak into downstream consumers.
 */
function toFiniteNonNegative(value: number | undefined): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : 0;
}

/**
 * Project a single raw {@link OperatorStatistics} into the flat performance model,
 * defaulting every optional/missing field to 0. Data and control processing time
 * are kept as separate fields so consumers can choose how to combine them.
 */
export function toPerformanceMetrics(operatorId: string, stats: OperatorStatistics): OperatorPerformanceMetrics {
  return {
    operatorId,
    dataProcessingTimeNs: toFiniteNonNegative(stats.aggregatedDataProcessingTime),
    controlProcessingTimeNs: toFiniteNonNegative(stats.aggregatedControlProcessingTime),
    idleTimeNs: toFiniteNonNegative(stats.aggregatedIdleTime),
    inputRows: toFiniteNonNegative(stats.aggregatedInputRowCount),
    outputRows: toFiniteNonNegative(stats.aggregatedOutputRowCount),
    inputSize: toFiniteNonNegative(stats.aggregatedInputSize),
    outputSize: toFiniteNonNegative(stats.aggregatedOutputSize),
    numWorkers: toFiniteNonNegative(stats.numWorkers),
  };
}
