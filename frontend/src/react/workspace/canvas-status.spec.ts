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

import { describe, expect, it, vi } from "vitest";
import { paintCanvasStatus, paintVersionHighlights } from "./canvas-status";
import * as joint from "jointjs";
import { ExecutionState, OperatorState } from "../../core/workspace/types/execute-workflow.interface";
import type { OperatorPredicate } from "../../core/workspace/types/workflow-common.interface";
import { HeatmapView } from "../../core/workspace/service/heatmap/heatmap-scoring";
const op = {
  operatorID: "a",
  operatorType: "Test",
  operatorProperties: {},
  inputPorts: [],
  outputPorts: [],
  showAdvanced: false,
  operatorVersion: "1",
} satisfies OperatorPredicate;
function ui() {
  return {
    changeOperatorState: vi.fn(),
    changeOperatorColor: vi.fn(),
    changeOperatorStatistics: vi.fn(),
    changeOperatorDisableStatus: vi.fn(),
    changeOperatorViewResultStatus: vi.fn(),
    changeOperatorReuseCacheStatus: vi.fn(),
    restoreOperatorFill: vi.fn(),
    applyHeatmapColor: vi.fn(),
  };
}
describe("canvas execution status", () => {
  it("paints version additions/modifications and deleted-neighbor brackets, then clears them", () => {
    const graph = new joint.dia.Graph(),
      first = new joint.dia.Element({ id: "a", type: "standard.Rectangle" }),
      second = new joint.dia.Element({ id: "b", type: "standard.Rectangle" });
    graph.addCells([first, second]);
    paintVersionHighlights(graph, {
      added: ["a"],
      modified: ["b"],
      deleted: ["gone"],
      leftBrackets: ["b"],
      rightBrackets: ["a"],
      operatorPropertyDiff: {},
    });
    expect(first.attr("rect.boundary/fill")).toBe("rgba(0,255,0,0.5)");
    expect(second.attr("rect.boundary/fill")).toBe("rgba(255,118,20,0.5)");
    expect(second.attr("path.left-boundary/stroke")).toBe("rgba(255,0,0,0.5)");
    paintVersionHighlights(graph);
    expect(first.attr("rect.boundary/fill")).toBe("transparent");
    expect(second.attr("path.left-boundary/stroke")).toBe("transparent");
  });
  it("keeps invalid borders above cached/recovering state and uses the existing row/worker renderer", () => {
    const view = ui(),
      paper = {} as joint.dia.Paper;
    const status = { operatorState: OperatorState.Completed, aggregatedInputRowCount: 0, aggregatedOutputRowCount: 1 };
    paintCanvasStatus(view, paper, [op], { state: ExecutionState.Recovering, operators: { a: status } }, ["a"], null);
    expect(view.changeOperatorState).toHaveBeenCalledWith(paper, "a", OperatorState.Recovering);
    expect(view.changeOperatorColor).toHaveBeenLastCalledWith(paper, "a", false);
    expect(view.changeOperatorColor.mock.invocationCallOrder[0]).toBeGreaterThan(
      view.changeOperatorState.mock.invocationCallOrder[0]
    );
    expect(view.changeOperatorStatistics).toHaveBeenCalledWith(paper, "a", status);
  });
  it("restores default state for unknown operators and never paints disabled/cache-reused nodes as measured heat", () => {
    const view = ui(),
      paper = {} as joint.dia.Paper;
    paintCanvasStatus(
      view,
      paper,
      [{ ...op, isDisabled: true }],
      { state: ExecutionState.Completed, operators: {} },
      [],
      HeatmapView.TimePerRow
    );
    expect(view.changeOperatorState).toHaveBeenCalledWith(paper, "a", OperatorState.Uninitialized);
    expect(view.applyHeatmapColor).toHaveBeenCalledWith(paper, "a", undefined);
    paintCanvasStatus(view, paper, [op], undefined, [], null);
    expect(view.restoreOperatorFill).toHaveBeenCalledWith(paper, op);
  });
});
