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

import { OperatorPerformanceMetrics, extractPerformanceMetrics } from "./performance-metrics";
import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";

/**
 * A complete statistics object, mirroring what the backend sends once every
 * field is typed. All five timing/size fields are present.
 */
const fullStats: OperatorStatistics = {
  operatorState: OperatorState.Running,
  aggregatedInputRowCount: 1_000_000,
  aggregatedInputSize: 84_000_000,
  inputPortMetrics: { "0": 1_000_000 },
  aggregatedOutputRowCount: 12_000,
  aggregatedOutputSize: 1_010_000,
  outputPortMetrics: { "0": 12_000 },
  numWorkers: 4,
  aggregatedDataProcessingTime: 8_500_000_000,
  aggregatedControlProcessingTime: 120_000_000,
  aggregatedIdleTime: 300_000_000,
};

/**
 * The partial shape produced by WorkflowStatusService.resetStatus(): only the
 * required fields, none of the five optional timing/size fields, no numWorkers.
 * The mapper must survive this without emitting NaN/undefined.
 */
const partialStats: OperatorStatistics = {
  operatorState: OperatorState.Uninitialized,
  aggregatedInputRowCount: 0,
  inputPortMetrics: {},
  aggregatedOutputRowCount: 0,
  outputPortMetrics: {},
};

describe("extractPerformanceMetrics", () => {
  it("maps every field from a full statistics object", () => {
    const m: OperatorPerformanceMetrics = extractPerformanceMetrics(fullStats);
    expect(m).toEqual({
      dataProcessingTimeNs: 8_500_000_000,
      controlProcessingTimeNs: 120_000_000,
      idleTimeNs: 300_000_000,
      inputRows: 1_000_000,
      outputRows: 12_000,
      inputSize: 84_000_000,
      outputSize: 1_010_000,
      numWorkers: 4,
    });
  });

  it("keeps data and control processing time as separate fields", () => {
    const m = extractPerformanceMetrics(fullStats);
    expect(m.dataProcessingTimeNs).toBe(fullStats.aggregatedDataProcessingTime);
    expect(m.controlProcessingTimeNs).toBe(fullStats.aggregatedControlProcessingTime);
  });

  it("defaults missing metric fields to 0 and numWorkers to 1 (no NaN, no undefined)", () => {
    const m = extractPerformanceMetrics(partialStats);
    expect(m).toEqual({
      dataProcessingTimeNs: 0,
      controlProcessingTimeNs: 0,
      idleTimeNs: 0,
      inputRows: 0,
      outputRows: 0,
      inputSize: 0,
      outputSize: 0,
      // an operator always runs on at least one worker
      numWorkers: 1,
    });
    // explicit guard: nothing leaked through as NaN
    for (const value of Object.values(m)) {
      if (typeof value === "number") {
        expect(Number.isNaN(value)).toBe(false);
      }
    }
  });

  it("defaults a missing worker count to 1 and passes a real count through", () => {
    expect(extractPerformanceMetrics({ ...partialStats, numWorkers: 8 }).numWorkers).toBe(8);
    expect(extractPerformanceMetrics({ ...partialStats, numWorkers: undefined }).numWorkers).toBe(1);
  });
});
