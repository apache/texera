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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { of } from "rxjs";
import * as fs from "node:fs";
import * as path from "node:path";
import { TracesPanelComponent, buildSpanTree } from "./traces-panel.component";
import { ObservabilityService } from "../../../../service/user/observability/observability.service";
import { TracesPivotService } from "../../../../service/user/observability/traces-pivot.service";
import { TraceSpan } from "../../../../service/user/observability/observability.types";

describe("TracesPanelComponent", () => {
  let component: TracesPanelComponent;
  let fixture: ComponentFixture<TracesPanelComponent>;
  let mockService: { getTrace: ReturnType<typeof vi.fn> };
  let pivot: TracesPivotService;

  const VALID = "0af7651916cd43dd8448eb211c80319c";

  beforeEach(async () => {
    mockService = { getTrace: vi.fn() };
    await TestBed.configureTestingModule({
      imports: [TracesPanelComponent, HttpClientTestingModule, NoopAnimationsModule],
      providers: [{ provide: ObservabilityService, useValue: mockService }],
    }).compileComponents();
    fixture = TestBed.createComponent(TracesPanelComponent);
    component = fixture.componentInstance;
    pivot = TestBed.inject(TracesPivotService);
  });

  it("rejects malformed trace id without dispatching", () => {
    fixture.detectChanges();
    component.form.patchValue({ traceId: "not-a-trace-id" });
    component.fetch();
    expect(component.errorMessage).toMatch(/Trace id/);
    expect(mockService.getTrace).not.toHaveBeenCalled();
  });

  it("fetches a valid trace and computes the span tree", () => {
    const spans: TraceSpan[] = [
      { spanId: "root", name: "root", startMs: 0, endMs: 100, attributes: {} },
      { spanId: "child", parentSpanId: "root", name: "child", startMs: 10, endMs: 50, attributes: {} },
    ];
    mockService.getTrace.mockReturnValue(of({ traceId: VALID, spans }));
    fixture.detectChanges();
    component.form.patchValue({ traceId: VALID });
    component.fetch();
    expect(mockService.getTrace).toHaveBeenCalledWith(VALID);
    expect(component.spanTree.length).toBe(1);
    expect(component.spanTree[0].span.name).toBe("root");
    expect(component.spanTree[0].children.length).toBe(1);
    expect(component.spanTree[0].children[0].span.name).toBe("child");
  });

  it("reacts to a TracesPivotService event by fetching the supplied trace id", () => {
    mockService.getTrace.mockReturnValue(of({ traceId: VALID, spans: [] }));
    fixture.detectChanges();
    pivot.pivot(VALID);
    expect(component.form.value.traceId).toBe(VALID);
    expect(mockService.getTrace).toHaveBeenCalledWith(VALID);
  });

  it("renders span name via {{ }} interpolation, never [innerHTML]", () => {
    const templatePath = path.resolve(__dirname, "traces-panel.component.html");
    const tpl = fs.readFileSync(templatePath, "utf-8");
    expect(tpl).not.toMatch(/\[innerHTML\]\s*=\s*['"][^'"]*span(\.|\?)/);
    expect(tpl).toMatch(/{{\s*n\.span\.name\s*}}/);
  });
});

describe("buildSpanTree (pure)", () => {
  it("handles an empty list", () => {
    expect(buildSpanTree([])).toEqual([]);
  });

  it("treats spans whose parent is missing as roots", () => {
    const spans: TraceSpan[] = [
      { spanId: "a", parentSpanId: "missing", name: "a", startMs: 0, endMs: 1, attributes: {} },
      { spanId: "b", name: "b", startMs: 2, endMs: 3, attributes: {} },
    ];
    const tree = buildSpanTree(spans);
    expect(tree.length).toBe(2);
    expect(tree.map(n => n.span.name).sort()).toEqual(["a", "b"]);
  });

  it("defends against self-parent cycles by treating them as roots", () => {
    const spans: TraceSpan[] = [
      { spanId: "x", parentSpanId: "x", name: "x", startMs: 0, endMs: 1, attributes: {} },
    ];
    const tree = buildSpanTree(spans);
    expect(tree.length).toBe(1);
    expect(tree[0].span.name).toBe("x");
    expect(tree[0].children.length).toBe(0);
  });

  it("computes duration = endMs - startMs (clamped at 0)", () => {
    const spans: TraceSpan[] = [
      { spanId: "a", name: "a", startMs: 100, endMs: 50, attributes: {} },
      { spanId: "b", name: "b", startMs: 10, endMs: 75, attributes: {} },
    ];
    const tree = buildSpanTree(spans);
    const byName = new Map(tree.map(n => [n.span.name, n]));
    expect(byName.get("a")!.durationMs).toBe(0); // end < start clamped
    expect(byName.get("b")!.durationMs).toBe(65);
  });

  it("sorts siblings ascending by startMs for visual stability", () => {
    const spans: TraceSpan[] = [
      { spanId: "root", name: "root", startMs: 0, endMs: 100, attributes: {} },
      { spanId: "c", parentSpanId: "root", name: "c", startMs: 80, endMs: 90, attributes: {} },
      { spanId: "a", parentSpanId: "root", name: "a", startMs: 10, endMs: 20, attributes: {} },
      { spanId: "b", parentSpanId: "root", name: "b", startMs: 20, endMs: 30, attributes: {} },
    ];
    const tree = buildSpanTree(spans);
    expect(tree[0].children.map(n => n.span.name)).toEqual(["a", "b", "c"]);
  });
});
