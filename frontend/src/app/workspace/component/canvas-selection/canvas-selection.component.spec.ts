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
import { NzModalModule } from "ng-zorro-antd/modal";
import * as joint from "jointjs";
import { CanvasSelectionComponent } from "./canvas-selection.component";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { OperatorMenuService } from "../../service/operator-menu/operator-menu.service";
import {
  mockScanPredicate,
  mockSentimentPredicate,
  mockScanSentimentLink,
} from "../../service/workflow-graph/model/mock-workflow-data";

describe("CanvasSelectionComponent event handling", () => {
  let fixture: ComponentFixture<CanvasSelectionComponent>;
  let actions: WorkflowActionService;
  let surface: HTMLDivElement;
  let viewport: SVGGElement;
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CanvasSelectionComponent, HttpClientTestingModule, NzModalModule],
      providers: [
        ...commonTestProviders,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: OperatorMenuService, useValue: { saveHighlightedElements: vi.fn() } },
      ],
    }).compileComponents();
    actions = TestBed.inject(WorkflowActionService);
    actions.addOperator(mockScanPredicate, { x: 100, y: 100 });
    actions.addOperator(mockSentimentPredicate, { x: 300, y: 100 });
    actions.addLink(mockScanSentimentLink);
    surface = document.createElement("div");
    viewport = document.createElementNS("http://www.w3.org/2000/svg", "g");
    fixture = TestBed.createComponent(CanvasSelectionComponent);
    fixture.componentRef.setInput("paper", {
      el: surface,
      viewport,
      model: actions.getJointGraph(),
      findView: () => undefined,
      // Simulate zoom + offset. Real SVG transforms are covered by the browser spec.
      clientToLocalPoint: (p: { x: number; y: number }) => ({ x: (p.x - 20) / 2, y: (p.y - 20) / 2 }),
    } as unknown as joint.dia.Paper);
    fixture.detectChanges();
    actions.getJointGraphWrapper().unhighlightElements(actions.getJointGraphWrapper().getCurrentHighlights());
  });
  afterEach(() => fixture?.destroy());
  function down(button = 2): void {
    surface.dispatchEvent(
      new MouseEvent("mousedown", { button, clientX: 20, clientY: 20, bubbles: true, cancelable: true })
    );
  }
  function finish(): void {
    document.dispatchEvent(new MouseEvent("mousemove", { clientX: 1220, clientY: 820 }));
    document.dispatchEvent(new MouseEvent("mouseup", { button: 2, clientX: 1220, clientY: 820 }));
    fixture.detectChanges();
  }
  it("uses transformed coordinates and exposes general actions without opening the snippet library", () => {
    down();
    finish();
    expect(actions.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()).toHaveLength(2);
    expect(actions.getJointGraphWrapper().getCurrentHighlightedLinkIDs()).toEqual([mockScanSentimentLink.linkID]);
    expect(viewport.firstElementChild?.classList.contains("canvas-selection-preview")).toBe(true);
    expect(viewport.firstElementChild?.getAttribute("stroke")).toBe("#1677ff");
    expect(viewport.firstElementChild?.getAttribute("stroke-width")).toBe("1.25");
    expect(viewport.querySelector("animate")?.getAttribute("attributeName")).toBe("stroke-dashoffset");
    expect(actions.getJointGraph().getCells()).toHaveLength(3);
    expect(fixture.nativeElement.querySelector('[title="Copy selected elements"]')).not.toBeNull();
    expect(fixture.nativeElement.querySelector('[title="Create a reusable snippet"]')).toBeNull();
    expect(getComputedStyle(fixture.nativeElement.querySelector(".selection-actions")).width).toBe("max-content");
  });
  it("leaves primary drag to the paper and Escape cancels an unfinished gesture", () => {
    down(0);
    finish();
    expect(actions.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()).toHaveLength(0);
    down();
    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    finish();
    expect(actions.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()).toHaveLength(0);
  });
  it("keeps a stationary secondary click for the context menu", () => {
    const spy = vi.spyOn(fixture.componentInstance.contextRequested, "emit");
    down();
    document.dispatchEvent(new MouseEvent("mouseup", { button: 2, clientX: 20, clientY: 20 }));
    expect(spy).toHaveBeenCalledOnce();
    expect(fixture.componentInstance.showActions).toBe(false);
  });
  it("does not delete selected operators after the workflow becomes read-only", () => {
    down();
    finish();
    actions.disableWorkflowModification();
    fixture.componentInstance.remove();
    expect(actions.getTexeraGraph().getAllOperators()).toHaveLength(2);
  });
});
