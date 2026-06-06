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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import * as fs from "node:fs";
import * as path from "node:path";
import { FlameChartComponent, MIN_VISIBLE_PX, flattenFlame } from "./flame-chart.component";
import { FlameFrame } from "../../../../../service/user/observability/observability.types";

describe("flattenFlame (pure)", () => {
  it("returns an empty list for a null-ish root via value=0", () => {
    expect(flattenFlame({ name: "x", value: 0, children: [] }, 100)).toEqual([]);
  });

  it("returns an empty list when totalWidth is non-positive", () => {
    expect(flattenFlame({ name: "x", value: 10, children: [] }, 0)).toEqual([]);
    expect(flattenFlame({ name: "x", value: 10, children: [] }, -50)).toEqual([]);
  });

  it("emits a single full-width row for a leaf root", () => {
    const rows = flattenFlame({ name: "root", value: 100, children: [] }, 200);
    expect(rows.length).toBe(1);
    expect(rows[0]).toMatchObject({ depth: 0, x: 0, width: 200, name: "root" });
  });

  it("apportions children widths proportionally to value", () => {
    const root: FlameFrame = {
      name: "root",
      value: 100,
      children: [
        { name: "a", value: 30, children: [] },
        { name: "b", value: 70, children: [] },
      ],
    };
    const rows = flattenFlame(root, 200);
    const byName = new Map(rows.map(r => [r.name, r]));
    expect(byName.get("a")!.width).toBeCloseTo(60);
    expect(byName.get("b")!.width).toBeCloseTo(140);
    // 'a' starts at 0, 'b' starts after 'a'.
    expect(byName.get("a")!.x).toBeCloseTo(0);
    expect(byName.get("b")!.x).toBeCloseTo(60);
  });

  it("skips frames narrower than MIN_VISIBLE_PX (keeps DOM bounded)", () => {
    const tiny: FlameFrame = {
      name: "root",
      value: 1000,
      children: Array.from({ length: 1000 }, (_, i) => ({
        name: `f${i}`,
        value: 1,
        children: [],
      })),
    };
    const rows = flattenFlame(tiny, 100); // each child would be 0.1 px wide
    // only the root survives at MIN_VISIBLE_PX.
    expect(rows.length).toBe(1);
    expect(rows[0].name).toBe("root");
  });

  it("recurses into nested children with correct depth + offset", () => {
    const root: FlameFrame = {
      name: "root",
      value: 100,
      children: [
        {
          name: "outer",
          value: 100,
          children: [{ name: "inner", value: 50, children: [] }],
        },
      ],
    };
    const rows = flattenFlame(root, 200);
    const depths = rows.map(r => r.depth).sort();
    expect(depths).toEqual([0, 1, 2]);
    const inner = rows.find(r => r.name === "inner")!;
    expect(inner.depth).toBe(2);
    expect(inner.width).toBeCloseTo(100); // half of outer's 200
  });

  it("treats undefined children as empty (defensive)", () => {
    const malformed: FlameFrame = {
      name: "root",
      value: 100,
      children: undefined as unknown as ReadonlyArray<FlameFrame>,
    };
    const rows = flattenFlame(malformed, 100);
    expect(rows.length).toBe(1);
  });

  it("MIN_VISIBLE_PX is a positive small integer (sanity check)", () => {
    expect(MIN_VISIBLE_PX).toBeGreaterThan(0);
    expect(MIN_VISIBLE_PX).toBeLessThanOrEqual(8);
  });
});

describe("FlameChartComponent", () => {
  let component: FlameChartComponent;
  let fixture: ComponentFixture<FlameChartComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [FlameChartComponent],
    }).compileComponents();
    fixture = TestBed.createComponent(FlameChartComponent);
    component = fixture.componentInstance;
  });

  it("computes rows from the input root on ngOnChanges", () => {
    component.root = { name: "r", value: 10, children: [] };
    component.width = 100;
    component.ngOnChanges({
      root: { currentValue: component.root, previousValue: null, firstChange: true, isFirstChange: () => true },
    } as never);
    expect(component.rows.length).toBe(1);
    expect(component.totalDepth).toBe(1);
  });

  it("hueFor produces a deterministic hue in [0, 360)", () => {
    const h1 = component.hueFor("foo");
    const h2 = component.hueFor("foo");
    expect(h1).toBe(h2);
    expect(h1).toBeGreaterThanOrEqual(0);
    expect(h1).toBeLessThan(360);
  });

  // ----- security tripwire -------------------------------------------

  it("renders frame names via interpolation, never via [innerHTML]", () => {
    // The unit-test bundler rewrites `__dirname` to the bundle root, so the
    // template can only be located by its stable path under the frontend
    // working directory (cwd is always `frontend/` in CI and locally).
    const templatePath = path.resolve(
      process.cwd(),
      "src/app/dashboard/component/user/observability/profiles-panel/flame-chart/flame-chart.component.html"
    );
    const tpl = fs.readFileSync(templatePath, "utf-8");
    expect(tpl).not.toMatch(/\[innerHTML\]/);
    expect(tpl).toMatch(/{{\s*row\.name\s*}}/);
  });
});
