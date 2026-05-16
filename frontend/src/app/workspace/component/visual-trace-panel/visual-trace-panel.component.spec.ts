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
import { BehaviorSubject } from "rxjs";
import { VisualTracePanelComponent } from "./visual-trace-panel.component";
import { VisualTraceService } from "../../service/visual-trace/visual-trace.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { VisualTrace } from "../../types/visual-trace.interface";

describe("VisualTracePanelComponent", () => {
  let fixture: ComponentFixture<VisualTracePanelComponent>;
  let component: VisualTracePanelComponent;
  let traceSubject: BehaviorSubject<VisualTrace | undefined>;

  beforeEach(async () => {
    traceSubject = new BehaviorSubject<VisualTrace | undefined>(undefined);

    await TestBed.configureTestingModule({
      imports: [VisualTracePanelComponent],
      providers: [
        {
          provide: VisualTraceService,
          useValue: {
            trace$: traceSubject.asObservable(),
            closeTrace: vi.fn(),
          },
        },
        {
          provide: WorkflowActionService,
          useValue: {
            getTexeraGraph: () => ({
              hasOperator: vi.fn().mockReturnValue(true),
              getOperator: vi.fn().mockReturnValue({
                operatorID: "op1",
                operatorType: "PythonUDFV2",
                customDisplayName: "Battle Logic",
              }),
            }),
            highlightOperators: vi.fn(),
          },
        },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(VisualTracePanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("renders a visual journey with hero media, metrics, and ordered steps", () => {
    traceSubject.next({
      title: "Charizard wins",
      subtitle: "Fire matchup",
      heroImage: "data:image/png;base64,abc",
      heroMetric: { label: "Advantage", value: "2x" },
      steps: [
        {
          title: "Loaded sprite",
          operatorId: "op1",
          image: "data:image/png;base64,abc",
          metrics: [{ label: "Rows", value: "440" }],
        },
        {
          title: "Rendered result",
          kind: "render",
        },
      ],
    });
    fixture.detectChanges();

    const native = fixture.nativeElement as HTMLElement;
    expect(native.querySelector(".trace-panel")).toBeTruthy();
    expect(native.querySelector(".hero-title")?.textContent).toContain("Charizard wins");
    expect(native.querySelector(".hero-media img")).toBeTruthy();
    expect(native.querySelector(".hero-metric")?.textContent).toContain("2x");
    expect(native.querySelectorAll(".trace-step")).toHaveLength(2);
    expect(native.textContent).toContain("Loaded sprite");
    expect(native.textContent).toContain("Rendered result");
  });
});

