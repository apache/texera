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

const CLEARANCE = 16;
const ROUTING_PADDING = 36;

function overlaps(a: joint.g.Rect, b: joint.g.Rect): boolean {
  return a.x <= b.x + b.width && a.x + a.width >= b.x && a.y <= b.y + b.height && a.y + a.height >= b.y;
}

function intersects(part: joint.g.Curve | joint.g.Line, rect: joint.g.Rect, depth = 0): boolean {
  const bounds = part.bbox();
  if (!overlaps(bounds, rect)) return false;
  // Conservative subdivision, rather than point sampling: even a thin obstacle is detected.
  if (depth === 12 || Math.max(bounds.width, bounds.height) < 0.25) return true;
  const [left, right] = part instanceof joint.g.Curve ? part.divideAtT(0.5) : part.divideAt(0.5);
  return intersects(left, rect, depth + 1) || intersects(right, rect, depth + 1);
}

export function pathIntersectsRect(path: joint.g.Path, rect: joint.g.Rect): boolean {
  const bounds = path.bbox();
  if (!bounds || !overlaps(bounds, rect)) return false;
  return path.segments.some(segment => {
    if (!segment.isVisible || !segment.start || !segment.end) return false;
    if (segment.type === "C") {
      const curve = segment as joint.g.Segment & { controlPoint1: joint.g.Point; controlPoint2: joint.g.Point };
      return intersects(new joint.g.Curve(curve.start!, curve.controlPoint1, curve.controlPoint2, curve.end!), rect);
    }
    return intersects(new joint.g.Line(segment.start, segment.end), rect);
  });
}

/** Prefer one cubic; keep a routed detour when that curve would cut through an operator. */
export function createWorkflowLinkPath(
  source: joint.dia.Point,
  target: joint.dia.Point,
  route: joint.dia.Point[],
  obstacles: joint.g.Rect[],
  allowDirect = true
): joint.g.Path {
  const clear = (path: joint.g.Path) => !obstacles.some(rect => pathIntersectsRect(path, rect));
  const dx = target.x - source.x;
  if (allowDirect && dx >= 40) {
    const reach = Math.min(180, Math.max(20, dx * 0.5));
    const direct = new joint.g.Path(
      new joint.g.Curve(source, { x: source.x + reach, y: source.y }, { x: target.x - reach, y: target.y }, target)
    );
    if (clear(direct)) return direct;
  }

  // Manhattan supplies a clear corridor. Check the rendered curve as well: rounding a
  // corner can cut inside that corridor. Reduce the radius if its safety margin is tight.
  for (const radius of [36, 18, 9, 4]) {
    const path = joint.connectors.rounded(source, target, route, { radius, raw: true }) as joint.g.Path;
    if (clear(path)) return path;
  }
  // Overlapping operators can make a safe route impossible. Keep the router's finite
  // fallback rather than moving nodes, inventing a disconnected path, or retrying forever.
  return joint.connectors.normal(source, target, route, { raw: true }) as joint.g.Path;
}

function isObstacle(cell: joint.dia.Cell): cell is joint.dia.Element {
  // Regions and co-editor cursors are decorations, not routing obstacles.
  return cell.isElement() && (cell.get("type") === "devs.Model" || cell.get("type") === "basic.Rect");
}

const previousRoutes = new WeakMap<
  joint.dia.LinkView,
  { source: joint.g.Point; target: joint.g.Point; route: joint.dia.Point[] }
>();

export function workflowConnector(
  this: joint.dia.LinkView,
  source: joint.dia.Point,
  target: joint.dia.Point,
  route: joint.dia.Point[]
): joint.g.Path {
  const link = this.model;
  const ends = [link.source().id, link.target().id];
  const obstacles = (this.paper?.model.getElements() ?? []).filter(isObstacle).map(element => {
    // Endpoints sit on the port boundary; only their interior is forbidden.
    return element.getBBox().inflate(ends.includes(element.id) ? -0.5 : CLEARANCE);
  });
  const allowDirect = link.vertices().length === 0;
  const previous = previousRoutes.get(this);
  if (
    allowDirect &&
    previous?.source.equals(new joint.g.Point(source)) &&
    previous.target.equals(new joint.g.Point(target))
  ) {
    const retained = createWorkflowLinkPath(source, target, previous.route, obstacles);
    // An unrelated obstacle moving slightly should not flip an already safe detour.
    if (!obstacles.some(rect => pathIntersectsRect(retained, rect))) return retained;
  }
  previousRoutes.set(this, { source: new joint.g.Point(source), target: new joint.g.Point(target), route });
  return createWorkflowLinkPath(source, target, route, obstacles, allowDirect);
}

export const workflowRouter: joint.routers.RouterJSON = {
  name: "manhattan",
  args: {
    padding: ROUTING_PADDING,
    startDirections: ["right"],
    endDirections: ["left"],
    excludeTypes: ["region", "basic.Circle", "standard.Circle", "basic.Text"],
  },
};

/** JointJS updates attached links; this also invalidates links crossed by an unrelated node. */
export class WorkflowLinkRouting {
  private frame: number | undefined;
  private dirtyBounds: joint.g.Rect[] = [];
  private previousBounds = new WeakMap<joint.dia.Cell, joint.g.Rect>();
  private readonly events = "change:position change:size change:ports add remove";

  constructor(private readonly paper: joint.dia.Paper) {
    paper.model
      .getElements()
      .filter(isObstacle)
      .forEach(element => this.previousBounds.set(element, element.getBBox()));
    paper.model.on(this.events, this.invalidate, this);
  }

  private invalidate = (cell: joint.dia.Cell): void => {
    if (!isObstacle(cell)) return;
    const previous = this.previousBounds.get(cell);
    if (previous) this.dirtyBounds.push(previous.clone().inflate(ROUTING_PADDING + CLEARANCE));
    const current = cell.getBBox();
    this.dirtyBounds.push(current.clone().inflate(ROUTING_PADDING + CLEARANCE));
    this.previousBounds.set(cell, current);
    if (this.frame === undefined) this.frame = requestAnimationFrame(this.flush);
  };

  private flush = (): void => {
    this.frame = undefined;
    const dirty = this.dirtyBounds;
    this.dirtyBounds = [];
    for (const link of this.paper.model.getLinks()) {
      const view = this.paper.findViewByModel(link) as joint.dia.LinkView | undefined;
      const bounds = view?.getConnection()?.bbox();
      if (bounds && dirty.some(rect => overlaps(bounds, rect))) view!.requestConnectionUpdate();
    }
  };

  dispose(): void {
    this.paper.model.off(this.events, this.invalidate, this);
    if (this.frame !== undefined) cancelAnimationFrame(this.frame);
    this.dirtyBounds = [];
  }
}
