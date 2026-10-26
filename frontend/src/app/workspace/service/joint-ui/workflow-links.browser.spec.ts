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

import { userEvent } from "vitest/browser";
import { WorkflowLinkRouting, pathIntersectsRect } from "./workflow-link-routing";
import * as joint from "jointjs";
import { JointUIService } from "./joint-ui.service";

// Real SVG geometry is needed: jsdom cannot measure the legacy arrow transforms.
describe("workflow link geometry", () => {
  let graph: joint.dia.Graph;
  let paper: joint.dia.Paper;
  let host: HTMLDivElement;
  let routing: WorkflowLinkRouting;

  beforeEach(() => {
    host = document.createElement("div");
    document.body.append(host);
    graph = new joint.dia.Graph();
    paper = new joint.dia.Paper({ el: host, model: graph, width: 1000, height: 700, async: false });
    routing = new WorkflowLinkRouting(paper);
  });

  afterEach(() => {
    routing.dispose();
    paper.remove();
    host.remove();
  });

  it("keeps the arrow aligned with the rendered line while either operator moves", async () => {
    const source = new joint.shapes.devs.Model({
      position: { x: 50, y: 100 },
      size: { width: 200, height: 80 },
      outPorts: ["out"],
    });
    const target = new joint.shapes.devs.Model({
      position: { x: 550, y: 100 },
      size: { width: 200, height: 80 },
      inPorts: ["in"],
    });
    const link = JointUIService.getDefaultLinkCell();
    link.source({ id: source.id, port: "out" });
    link.target({ id: target.id, port: "in" });
    graph.addCells([source, target, link]);
    // A curved connector must orient the arrow from the final path, not a route waypoint.
    link.connector("smooth");
    const errors: string[] = [];
    for (const scale of [1, 0.6, 1.5]) {
      paper.scale(scale);
      for (const element of [source, target]) {
        for (const pos of Array.from({ length: 240 }, (_, i) => ({
          x: 270 + (i % 20) * 5,
          y: 80 + Math.floor(i / 20) * 5,
        }))) {
          element.position(pos.x, pos.y);
          const view = paper.findViewByModel(link);
          const path = view.el.querySelector(".connection") as SVGPathElement;
          const markerURL = path.getAttribute("marker-end");
          expect(markerURL).toBeTruthy();
          const markerID = markerURL!.slice(markerURL!.indexOf("#") + 1, -1);
          const marker = paper.svg.querySelector(`[id="${markerID}"]`)!;
          expect(marker.getAttribute("orient")).toBe("auto");
          // The tip is at the marker origin: SVG attaches it to the actual path endpoint.
          expect(marker.querySelector("path")!.getAttribute("d")).toBe("M 10 -5 L 0 0 L 10 5 Z");
          expect(view.el.querySelector(".marker-target")).toBeNull();
          const end = path.getPointAtLength(path.getTotalLength());
          const targetPoint = (view as joint.dia.LinkView).getEndConnectionPoint("target");
          const distance = Math.hypot(end.x - targetPoint.x, end.y - targetPoint.y);
          if (distance > 0.1) errors.push(JSON.stringify({ scale, pos, distance }));
        }
      }
    }
    expect(errors.slice(0, 5)).toEqual([]);
  });
  it("reroutes around an unrelated moved node without rearranging either endpoint", async () => {
    const source = new joint.shapes.devs.Model({
      position: { x: 50, y: 100 },
      size: { width: 200, height: 80 },
      outPorts: ["out"],
    });
    const target = new joint.shapes.devs.Model({
      position: { x: 650, y: 100 },
      size: { width: 200, height: 80 },
      inPorts: ["in"],
    });
    const obstacle = new joint.shapes.devs.Model({ position: { x: 380, y: 400 }, size: { width: 100, height: 100 } });
    const link = JointUIService.getDefaultLinkCell();
    link.source({ id: source.id, port: "out" });
    link.target({ id: target.id, port: "in" });
    graph.addCells([source, target, obstacle, link]);
    await new Promise(requestAnimationFrame);
    const view = paper.findViewByModel(link) as joint.dia.LinkView;
    const original = view.getConnection().serialize();
    obstacle.position(380, 95);
    await new Promise(requestAnimationFrame);
    expect(view.getConnection().serialize()).not.toBe(original);
    expect(pathIntersectsRect(view.getConnection(), obstacle.getBBox().inflate(12))).toBe(false);
    expect(source.position()).toEqual({ x: 50, y: 100 });
    expect(target.position()).toEqual({ x: 650, y: 100 });
    const detour = view.getConnection().serialize();
    obstacle.position(381, 95);
    await new Promise(requestAnimationFrame);
    expect(view.getConnection().serialize()).toBe(detour);
    obstacle.remove();
    await new Promise(requestAnimationFrame);
    expect(view.getConnection().serialize()).toBe(original);
  });

  it("inherits highlight colors without hiding or detaching the arrow on reset", () => {
    const link = JointUIService.getDefaultLinkCell();
    link.source({ x: 50, y: 100 });
    link.target({ x: 450, y: 200 });
    graph.addCell(link);
    const path = paper.findViewByModel(link).el.querySelector(".connection")!;
    for (const color of ["orange", "#FF6B35", "#919191"]) {
      link.attr(".connection/stroke", color);
      const markerURL = path.getAttribute("marker-end")!;
      const markerID = markerURL.slice(markerURL.indexOf("#") + 1, -1);
      expect(paper.svg.querySelector(`[id="${markerID}"] path`)!.getAttribute("fill")).toBe(color);
    }
  });
  it("keeps the endpoint draggable without painting a second arrow", async () => {
    const source = new joint.shapes.devs.Model({
      position: { x: 30, y: 100 },
      size: { width: 120, height: 80 },
      outPorts: ["out"],
    });
    const target = new joint.shapes.devs.Model({
      position: { x: 350, y: 100 },
      size: { width: 120, height: 80 },
      inPorts: ["in"],
    });
    const replacement = new joint.shapes.devs.Model({
      position: { x: 650, y: 300 },
      size: { width: 120, height: 80 },
      inPorts: ["in"],
    });
    const link = JointUIService.getDefaultLinkCell();
    link.source({ id: source.id, port: "out" });
    link.target({ id: target.id, port: "in" });
    graph.addCells([source, target, replacement, link]);
    const handle = paper.findViewByModel(link).el.querySelector(".marker-arrowhead-group-target .marker-arrowhead")!;
    expect(getComputedStyle(handle).opacity).toBe("0");
    await userEvent.dragAndDrop(handle, paper.findViewByModel(replacement).el.querySelector(".port-body")!);
    expect(link.target().id).toBe(replacement.id);
    expect(link.target().port).toBe("in");
  });
});
