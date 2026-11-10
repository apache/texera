/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import type * as joint from "jointjs";
import type { JointUIService } from "../../core/workspace/service/joint-ui/joint-ui";
import type { OperatorPredicate } from "../../core/workspace/types/workflow-common.interface";
import {
  ExecutionState,
  OperatorState,
  type OperatorRuntimeStatus,
} from "../../core/workspace/types/execute-workflow.interface";
import { HeatmapView, normalizeScores, rawMetricForView } from "../../core/workspace/service/heatmap/heatmap-scoring";
import { extractPerformanceMetrics } from "../../core/workspace/service/workflow-status/performance-metrics";
import type { VersionHighlights } from "../workflows/version-diff";
type StatusUI = Pick<
  JointUIService,
  | "changeOperatorState"
  | "changeOperatorColor"
  | "changeOperatorStatistics"
  | "changeOperatorDisableStatus"
  | "changeOperatorViewResultStatus"
  | "changeOperatorReuseCacheStatus"
  | "restoreOperatorFill"
  | "applyHeatmapColor"
>;
export function paintVersionHighlights(graph: joint.dia.Graph, highlights?: VersionHighlights): void {
  const added = new Set(highlights?.added),
    modified = new Set(highlights?.modified),
    left = new Set(highlights?.leftBrackets),
    right = new Set(highlights?.rightBrackets);
  for (const cell of graph.getElements()) {
    const id = String(cell.id);
    if (cell.get("texeraOverlay") || cell.get("texeraPresence")) continue;
    cell.attr(
      "rect.boundary/fill",
      modified.has(id) ? "rgba(255,118,20,0.5)" : added.has(id) ? "rgba(0,255,0,0.5)" : "transparent"
    );
    cell.attr("path.left-boundary/stroke", left.has(id) ? "rgba(255,0,0,0.5)" : "transparent");
    cell.attr("path.right-boundary/stroke", right.has(id) ? "rgba(255,0,0,0.5)" : "transparent");
  }
}
export function paintCanvasStatus(
  ui: StatusUI,
  paper: joint.dia.Paper,
  operators: readonly OperatorPredicate[],
  snapshot: { state: ExecutionState; operators: Readonly<Record<string, OperatorRuntimeStatus>> } | undefined,
  invalidIds: readonly string[],
  heatmap: HeatmapView | null
): void {
  const invalid = new Set(invalidIds),
    raw: Record<string, number> = {};
  if (heatmap)
    for (const op of operators) {
      const stats = snapshot?.operators[op.operatorID];
      if (stats && !op.isDisabled && !stats.reusedFromCache) {
        const metric = rawMetricForView(extractPerformanceMetrics(stats), heatmap);
        if (metric !== undefined) raw[op.operatorID] = metric;
      }
    }
  const scores = normalizeScores(raw);
  for (const op of operators) {
    const id = op.operatorID,
      stats = snapshot?.operators[id];
    ui.changeOperatorDisableStatus(paper, op);
    ui.changeOperatorViewResultStatus(paper, op);
    ui.changeOperatorReuseCacheStatus(paper, op, stats?.reusedFromCache ? "cache valid" : undefined);
    ui.changeOperatorState(
      paper,
      id,
      stats
        ? snapshot?.state === ExecutionState.Recovering
          ? OperatorState.Recovering
          : stats.operatorState
        : OperatorState.Uninitialized
    );
    ui.changeOperatorStatistics(paper, id, stats);
    if (invalid.has(id)) ui.changeOperatorColor(paper, id, false);
    else if (!stats) ui.changeOperatorColor(paper, id, true);
    if (heatmap) ui.applyHeatmapColor(paper, id, scores[id]);
    else ui.restoreOperatorFill(paper, op);
  }
}
