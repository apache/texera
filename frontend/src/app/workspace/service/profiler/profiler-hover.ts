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

/**
 * Pure formatting helpers for the per-operator hover card.
 *
 * Kept dependency-free (no Angular, no JointJS) so the headline text can be
 * unit-tested with synthetic stats without standing up a paper or DOM.
 */

import { OperatorStatistics } from "../../types/execute-workflow.interface";
import { ProfilerView } from "./profiler.service";

/**
 * Returns the headline metric to surface on hover for the current profiler view.
 * The text mirrors what the side panel shows in its "key field" position, just
 * shorter so it fits a single tooltip line.
 *
 * Returns `undefined` when no meaningful value is available (e.g. an operator
 * that hasn't started or has zero of the relevant metric); callers should
 * suppress the headline row in that case rather than render an empty string.
 */
export function formatHoverHeadline(view: ProfilerView, stats: OperatorStatistics): string | undefined {
  switch (view) {
    case "runtime": {
      const t = stats.aggregatedDataProcessingTime;
      if (!t || t <= 0) return undefined;
      const ms = t / 1_000_000;
      return `${formatNumber(ms, ms >= 100 ? 0 : 1)} ms`;
    }
    case "throughput": {
      const out = stats.aggregatedOutputRowCount ?? 0;
      const t = stats.aggregatedDataProcessingTime;
      if (!t || t <= 0 || out <= 0) return undefined;
      const rowsPerSec = out / (t / 1_000_000_000);
      return `${formatNumber(rowsPerSec, 0)} rows/s`;
    }
    case "io-imbalance": {
      const inp = stats.aggregatedInputRowCount ?? 0;
      const out = stats.aggregatedOutputRowCount ?? 0;
      if (inp <= 0) return undefined;
      const dropped = 1 - out / inp;
      return `${(dropped * 100).toFixed(0)}% dropped (${out.toLocaleString()} of ${inp.toLocaleString()})`;
    }
  }
}

/**
 * Returns a short human-readable label for the current profiler view —
 * shown next to the headline metric so the tooltip is self-describing.
 */
export function formatViewLabel(view: ProfilerView): string {
  switch (view) {
    case "runtime":
      return "Runtime";
    case "throughput":
      return "Throughput";
    case "io-imbalance":
      return "I/O imbalance";
  }
}

function formatNumber(n: number, fractionDigits: number): string {
  return n.toLocaleString(undefined, {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  });
}
