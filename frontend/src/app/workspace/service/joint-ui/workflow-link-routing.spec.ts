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
import { createWorkflowLinkPath, pathIntersectsRect } from "./workflow-link-routing";

const point = (x: number, y: number) => new joint.g.Point(x, y);

describe("workflow curve routing", () => {
  it("uses a single cubic with horizontal end tangents for a clear forward link", () => {
    const path = createWorkflowLinkPath(point(0, 0), point(300, 100), [], []);
    expect(path.segments.map(segment => segment.type)).toEqual(["M", "C"]);
    expect(path.start).toEqual(point(0, 0));
    expect(path.end).toEqual(point(300, 100));
    const start = path.tangentAt(0)!;
    const end = path.tangentAt(1)!;
    expect(start.end.y).toBe(start.start.y);
    expect(end.end.y).toBe(end.start.y);
  });

  it("follows a safe detour instead of smoothing through an intervening operator", () => {
    const obstacle = new joint.g.Rect(100, -30, 100, 60);
    const path = createWorkflowLinkPath(
      point(0, 0),
      point(300, 0),
      [point(60, 0), point(60, -80), point(240, -80), point(240, 0)],
      [obstacle]
    );
    expect(pathIntersectsRect(path, obstacle)).toBe(false);
    expect(path.segments.filter(segment => segment.isVisible).every(segment => segment.type === "C")).toBe(true);
    const curves = path.segments.filter(segment => segment.type === "C") as Array<
      joint.g.Segment & { controlPoint1: joint.g.Point; controlPoint2: joint.g.Point }
    >;
    for (let i = 1; i < curves.length; i++) {
      const previous = curves[i - 1];
      const current = curves[i];
      expect(current.controlPoint1.x - current.start!.x).toBeCloseTo(previous.end!.x - previous.controlPoint2.x);
      expect(current.controlPoint1.y - current.start!.y).toBeCloseTo(previous.end!.y - previous.controlPoint2.y);
    }
    expect(
      curves.every(
        curve =>
          curve.start!.y !== curve.end!.y ||
          curve.controlPoint1.y !== curve.start!.y ||
          curve.controlPoint2.y !== curve.end!.y
      )
    ).toBe(true);
    expect(path.bbox()!.y).toBeLessThan(-30);
  });

  it("reduces spline handles when smoothing would cut into an obstacle", () => {
    const obstacle = new joint.g.Rect(75, 5, 20, 20);
    const path = createWorkflowLinkPath(point(0, 0), point(100, 100), [point(100, 0)], [obstacle], false);
    expect(pathIntersectsRect(path, obstacle)).toBe(false);
  });

  it("does not turn a backwards route into a curve through either endpoint operator", () => {
    const path = createWorkflowLinkPath(
      point(300, 0),
      point(0, 0),
      [point(340, 0), point(340, -100), point(-40, -100), point(-40, 0)],
      []
    );
    expect(path.bbox()!.y).toBeLessThan(0);
    expect(path.tangentAt(0)!.end.x).toBeGreaterThan(path.start!.x);
    expect(path.tangentAt(1)!.end.x).toBeGreaterThan(path.tangentAt(1)!.start.x);
  });

  it("preserves an explicit waypoint even when a direct route is clear", () => {
    const path = createWorkflowLinkPath(point(0, 0), point(300, 0), [point(150, -100)], [], false);
    expect(path.bbox()!.y).toBeLessThan(-50);
  });

  it("handles coincident endpoints and a blocked fallback without NaN or an unbounded search", () => {
    const path = createWorkflowLinkPath(point(10, 10), point(10, 10), [], [new joint.g.Rect(0, 0, 20, 20)]);
    expect(path.serialize()).not.toMatch(/NaN|Infinity/);
    expect(path.end).toEqual(point(10, 10));
  });

  it("detects thin obstacles along a cubic, but not empty space within its bounding box", () => {
    const path = createWorkflowLinkPath(point(0, 0), point(300, 100), [], []);
    expect(pathIntersectsRect(path, new joint.g.Rect(149.9, 49.9, 0.2, 0.2))).toBe(true);
    expect(pathIntersectsRect(path, new joint.g.Rect(20, 70, 10, 10))).toBe(false);
    expect(pathIntersectsRect(new joint.g.Path(), new joint.g.Rect(0, 0, 10, 10))).toBe(false);
  });
});
