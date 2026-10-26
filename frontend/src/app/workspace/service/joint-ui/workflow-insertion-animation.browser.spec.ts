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

import * as joint from "jointjs";
import { WorkflowInsertionAnimation } from "./workflow-insertion-animation";

const nextFrame = () => new Promise<void>(resolve => requestAnimationFrame(() => resolve()));

describe("workflow insertion animation", () => {
  let host: HTMLDivElement;
  let graph: joint.dia.Graph;
  let paper: joint.dia.Paper;
  let animation: WorkflowInsertionAnimation;
  let reloading: boolean;

  beforeEach(() => {
    reloading = false;
    host = document.createElement("div");
    document.body.append(host);
    graph = new joint.dia.Graph();
    paper = new joint.dia.Paper({ el: host, model: graph, width: 800, height: 500, async: false });
    animation = new WorkflowInsertionAnimation(
      paper,
      () => reloading,
      id => graph.getCell(id)?.get("type") === "devs.Model"
    );
  });

  afterEach(() => {
    animation.dispose();
    paper.remove();
    host.remove();
    vi.restoreAllMocks();
  });

  function addOperator() {
    const operator = new joint.shapes.devs.Model({ position: { x: 100, y: 100 }, size: { width: 200, height: 80 } });
    graph.addCell(operator);
    return operator;
  }

  it("fades in at its final position without writing animation state to the graph", async () => {
    const operator = addOperator();
    const changed = vi.fn();
    graph.on("change", changed);
    await nextFrame();
    const view = paper.findViewByModel(operator);
    const effects = view.el.getAnimations({ subtree: true });
    expect(effects.length).toBeGreaterThan(0);
    expect(operator.position()).toEqual({ x: 100, y: 100 });
    expect(changed).not.toHaveBeenCalled();
    effects.forEach(effect => effect.finish());
    await nextFrame();
    expect(view.el.querySelector(".workflow-insertion-halo")).toBeNull();
    expect(changed).not.toHaveBeenCalled();
  });

  it("skips initial loading and decorations, but animates a subsequent insertion", async () => {
    reloading = true;
    const existing = addOperator();
    reloading = false;
    const decoration = new joint.shapes.standard.Circle();
    graph.addCell(decoration);
    const added = addOperator();
    await nextFrame();
    expect(paper.findViewByModel(existing).el.getAnimations({ subtree: true })).toHaveLength(0);
    expect(paper.findViewByModel(decoration).el.getAnimations({ subtree: true })).toHaveLength(0);
    expect(paper.findViewByModel(added).el.getAnimations({ subtree: true }).length).toBeGreaterThan(0);
  });

  it("cancels the effect immediately when the user starts interacting", async () => {
    const operator = addOperator();
    await nextFrame();
    const view = paper.findViewByModel(operator);
    view.el.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    expect(view.el.getAnimations({ subtree: true })).toHaveLength(0);
    expect(view.el.querySelector(".workflow-insertion-halo")).toBeNull();
  });

  it("respects reduced motion", async () => {
    vi.spyOn(window, "matchMedia").mockReturnValue({ matches: true } as MediaQueryList);
    const operator = addOperator();
    await nextFrame();
    expect(paper.findViewByModel(operator).el.getAnimations({ subtree: true })).toHaveLength(0);
  });

  it("drops deleted pending nodes and cancels all effects on disposal", async () => {
    const removed = addOperator();
    removed.remove();
    const kept = addOperator();
    await nextFrame();
    const view = paper.findViewByModel(kept);
    animation.dispose();
    expect(view.el.getAnimations({ subtree: true })).toHaveLength(0);
    expect(view.el.querySelector(".workflow-insertion-halo")).toBeNull();
    addOperator();
    await nextFrame();
    expect(host.getAnimations({ subtree: true })).toHaveLength(0);
  });

  it("waits for asynchronously mounted views and bounds staggering for a batch", async () => {
    paper.freeze();
    const operators = Array.from({ length: 12 }, () => addOperator());
    await nextFrame();
    let animatedAtMount = false;
    paper.once("render:done", () => {
      animatedAtMount = operators.every(operator => paper.findViewByModel(operator).el.getAnimations().length > 0);
    });
    paper.unfreeze();
    // The first paint must already be animated, rather than flashing fully visible for a frame.
    expect(animatedAtMount).toBe(true);
    await nextFrame();
    const delays = operators.flatMap(operator =>
      paper
        .findViewByModel(operator)
        .el.getAnimations()
        .map(effect => effect.effect!.getTiming().delay ?? 0)
    );
    expect(delays).toHaveLength(12);
    expect(Math.max(...delays)).toBeLessThanOrEqual(120);
  });
});
