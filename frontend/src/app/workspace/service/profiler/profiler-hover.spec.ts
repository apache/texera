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

import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";
import { formatHoverHeadline, formatViewLabel } from "./profiler-hover";

function stat(partial: Partial<OperatorStatistics> = {}): OperatorStatistics {
  return {
    operatorState: OperatorState.Completed,
    aggregatedInputRowCount: 0,
    inputPortMetrics: {},
    aggregatedOutputRowCount: 0,
    outputPortMetrics: {},
    ...partial,
  };
}

describe("formatHoverHeadline", () => {
  describe("runtime view", () => {
    it("returns ms with one decimal for sub-100 values", () => {
      // 50,000,000 ns = 50 ms
      const text = formatHoverHeadline("runtime", stat({ aggregatedDataProcessingTime: 50_000_000 }));
      expect(text).toBe("50.0 ms");
    });

    it("returns ms without decimals for >=100 ms values", () => {
      // 2,710,800,000 ns = 2,710.8 ms
      const text = formatHoverHeadline("runtime", stat({ aggregatedDataProcessingTime: 2_710_800_000 }));
      expect(text).toBe("2,711 ms");
    });

    it("returns undefined when runtime is missing", () => {
      expect(formatHoverHeadline("runtime", stat({}))).toBeUndefined();
    });

    it("returns undefined when runtime is zero", () => {
      expect(formatHoverHeadline("runtime", stat({ aggregatedDataProcessingTime: 0 }))).toBeUndefined();
    });
  });

  describe("throughput view", () => {
    it("returns rows/s rounded to whole rows", () => {
      // 1,000 rows over 0.5s -> 2,000 rows/s
      const text = formatHoverHeadline(
        "throughput",
        stat({ aggregatedOutputRowCount: 1_000, aggregatedDataProcessingTime: 500_000_000 })
      );
      expect(text).toBe("2,000 rows/s");
    });

    it("returns undefined when output rows are zero", () => {
      expect(
        formatHoverHeadline(
          "throughput",
          stat({ aggregatedOutputRowCount: 0, aggregatedDataProcessingTime: 1_000_000 })
        )
      ).toBeUndefined();
    });

    it("returns undefined when runtime is missing", () => {
      expect(formatHoverHeadline("throughput", stat({ aggregatedOutputRowCount: 1_000 }))).toBeUndefined();
    });
  });

  describe("io-imbalance view", () => {
    it("returns dropped percentage and absolute counts", () => {
      const text = formatHoverHeadline(
        "io-imbalance",
        stat({ aggregatedInputRowCount: 1_000, aggregatedOutputRowCount: 50 })
      );
      // 95% dropped
      expect(text).toBe("95% dropped (50 of 1,000)");
    });

    it("returns 0% dropped for pass-through operators", () => {
      const text = formatHoverHeadline(
        "io-imbalance",
        stat({ aggregatedInputRowCount: 1_000, aggregatedOutputRowCount: 1_000 })
      );
      expect(text).toBe("0% dropped (1,000 of 1,000)");
    });

    it("returns undefined when input is zero (source operators)", () => {
      expect(
        formatHoverHeadline("io-imbalance", stat({ aggregatedInputRowCount: 0, aggregatedOutputRowCount: 1_000 }))
      ).toBeUndefined();
    });
  });
});

describe("formatViewLabel", () => {
  it("maps each ProfilerView to a human-readable label", () => {
    expect(formatViewLabel("runtime")).toBe("Runtime");
    expect(formatViewLabel("throughput")).toBe("Throughput");
    expect(formatViewLabel("io-imbalance")).toBe("I/O imbalance");
  });
});
