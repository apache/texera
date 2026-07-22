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
import { ElementRef } from "@angular/core";
import { CdkDragEnd } from "@angular/cdk/drag-drop";
import { NzResizeEvent } from "ng-zorro-antd/resizable";

import { DEFAULT_HEIGHT, DEFAULT_WIDTH, ResultPanelComponent } from "./result-panel.component";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { PanelResizeService } from "../../service/workflow-result/panel-resize/panel-resize.service";
import { ResultTableFrameComponent } from "./result-table-frame/result-table-frame.component";
import { VisualizationFrameContentComponent } from "../visualization-panel-content/visualization-frame-content.component";
import { ErrorFrameComponent } from "./error-frame/error-frame.component";
import { ConsoleFrameComponent } from "./console-frame/console-frame.component";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { By } from "@angular/platform-browser";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalModule } from "ng-zorro-antd/modal";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { mockPoint, mockResultPredicate } from "../../service/workflow-graph/model/mock-workflow-data";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("ResultPanelComponent", () => {
  let component: ResultPanelComponent;
  let fixture: ComponentFixture<ResultPanelComponent>;
  let executeWorkflowService: ExecuteWorkflowService;
  let workflowActionService: WorkflowActionService;
  let workflowResultService: WorkflowResultService;
  let resizeService: PanelResizeService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ResultPanelComponent, HttpClientTestingModule, NzModalModule],
      providers: [
        WorkflowActionService,
        ExecuteWorkflowService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ResultPanelComponent);
    component = fixture.componentInstance;
    executeWorkflowService = TestBed.inject(ExecuteWorkflowService);
    workflowActionService = TestBed.inject(WorkflowActionService);
    workflowResultService = TestBed.inject(WorkflowResultService);
    resizeService = TestBed.inject(PanelResizeService);
    fixture.detectChanges();
  });

  it("should create", () => expect(component).toBeTruthy());

  it("should show nothing by default", () => {
    expect(component.frameComponentConfigs.size).toBe(0);
  });

  it("should show the result panel if a workflow finishes execution", () => {
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    executeWorkflowService["updateExecutionState"]({
      state: ExecutionState.Running,
    });
    executeWorkflowService["updateExecutionState"]({
      state: ExecutionState.Completed,
    });
    fixture.detectChanges();
    const resultPanelDiv = fixture.debugElement.query(By.css("#result-container"));
    const resultPanelHtmlElement: HTMLElement = resultPanelDiv.nativeElement;
    expect(resultPanelHtmlElement).toBeTruthy();
  });

  it("wipes the panel and operator selection when results are cleared, e.g. on a computing-unit switch (#3120)", () => {
    // Simulate a result frame on screen for a currently-highlighted operator.
    // ResultPanelComponent stands in as a throwaway frame component; it's cleared before it renders.
    component.currentOperatorId = "op1";
    component.operatorTitle = "Operator 1";
    component.frameComponentConfigs.set("Result", { component: ResultPanelComponent, componentInputs: {} });
    expect(component.frameComponentConfigs.size).toBe(1);

    // A unit switch drops the cached results and emits on the cleared stream. The operator
    // stays highlighted, so the normal rerender path won't tear the frame down — only this
    // handler does, which is the part that actually fixes the lingering-stale-frame bug.
    workflowResultService.clearResults();

    expect(component.frameComponentConfigs.size).toBe(0);
    expect(component.currentOperatorId).toBeUndefined();
    expect(component.operatorTitle).toBe("");
  });

  describe("visibility", () => {
    it("openPanel sets default dimensions and notifies the resize service", () => {
      const resizeSpy = vi.spyOn(resizeService, "changePanelSize");

      component.openPanel();

      expect(component.height).toBe(DEFAULT_HEIGHT);
      expect(component.width).toBe(DEFAULT_WIDTH);
      expect(resizeSpy).toHaveBeenCalledWith(DEFAULT_WIDTH, DEFAULT_HEIGHT);
    });

    it("closePanel collapses the panel", () => {
      component.openPanel();

      component.closePanel();

      expect(component.height).toBe(32.5);
      expect(component.width).toBe(0);
    });

    it("isPanelDocked is true only when the drag position matches the return position", () => {
      component.returnPosition = { x: 5, y: 7 };
      component.dragPosition = { x: 5, y: 7 };
      expect(component.isPanelDocked()).toBe(true);

      component.dragPosition = { x: 5, y: 8 };
      expect(component.isPanelDocked()).toBe(false);
    });

    it("clearResultPanel empties the frame configs", () => {
      component.frameComponentConfigs.set("Result", { component: ResultTableFrameComponent, componentInputs: {} });

      component.clearResultPanel();

      expect(component.frameComponentConfigs.size).toBe(0);
    });
  });

  describe("content frames", () => {
    it("displayConsole registers a console frame", () => {
      component.displayConsole("op1", true);

      const config = component.frameComponentConfigs.get("Console");
      expect(config?.component).toBe(ConsoleFrameComponent);
      expect(config?.componentInputs).toEqual({ operatorId: "op1", consoleInputEnabled: true });
    });

    it("displayError registers a static error frame", () => {
      component.displayError("op1");

      const config = component.frameComponentConfigs.get("Static Error");
      expect(config?.component).toBe(ErrorFrameComponent);
      expect(config?.componentInputs).toEqual({ operatorId: "op1" });
    });

    it("displayResult uses the table frame when a paginated result exists", () => {
      vi.spyOn(workflowResultService, "getPaginatedResultService").mockReturnValue(
        {} as unknown as ReturnType<typeof workflowResultService.getPaginatedResultService>
      );

      component.displayResult("op1");

      expect(component.frameComponentConfigs.get("Result")?.component).toBe(ResultTableFrameComponent);
    });

    it("displayResult uses the visualization frame when only a non-paginated result exists", () => {
      vi.spyOn(workflowResultService, "getPaginatedResultService").mockReturnValue(undefined);
      vi.spyOn(workflowResultService, "getResultService").mockReturnValue(
        {} as unknown as ReturnType<typeof workflowResultService.getResultService>
      );

      component.displayResult("op1");

      expect(component.frameComponentConfigs.get("Result")?.component).toBe(VisualizationFrameContentComponent);
    });

    it("displayResult registers nothing when the operator has no result", () => {
      vi.spyOn(workflowResultService, "getPaginatedResultService").mockReturnValue(undefined);
      vi.spyOn(workflowResultService, "getResultService").mockReturnValue(undefined);

      component.displayResult("op1");

      expect(component.frameComponentConfigs.has("Result")).toBe(false);
    });
  });

  describe("position & resize", () => {
    it("resetPanelPosition moves the drag position back to the return position", () => {
      component.returnPosition = { x: 3, y: 9 };
      component.dragPosition = { x: 100, y: 200 };

      component.resetPanelPosition();

      expect(component.dragPosition).toEqual({ x: 3, y: 9 });
    });

    it("updateReturnPosition shifts y by the height delta", () => {
      component.returnPosition = { x: 4, y: 10 };

      component.updateReturnPosition(500, 300); // y + (500 - 300)

      expect(component.returnPosition).toEqual({ x: 4, y: 210 });
    });

    it("updateReturnPosition is a no-op when the new height is undefined", () => {
      component.returnPosition = { x: 4, y: 10 };

      component.updateReturnPosition(500, undefined);

      expect(component.returnPosition).toEqual({ x: 4, y: 10 });
    });

    it("onResize applies the new size and notifies the resize service", () => {
      // Run the requestAnimationFrame callback synchronously so the assertion is deterministic.
      // These spies patch the global `window`, so restore them locally to avoid leaking across tests.
      const cancelSpy = vi.spyOn(window, "cancelAnimationFrame").mockImplementation(() => {});
      const rafSpy = vi.spyOn(window, "requestAnimationFrame").mockImplementation(cb => {
        cb(0);
        return 1;
      });
      const resizeSpy = vi.spyOn(resizeService, "changePanelSize");

      try {
        component.onResize({ width: 900, height: 600 } as NzResizeEvent);

        expect(component.width).toBe(900);
        expect(component.height).toBe(600);
        expect(resizeSpy).toHaveBeenCalledWith(900, 600);
      } finally {
        rafSpy.mockRestore();
        cancelSpy.mockRestore();
      }
    });
  });

  describe("drag", () => {
    it("handleStartDrag hides the visualization overlay when it is present", () => {
      const vizEl = { style: { zIndex: 0 } };
      component.componentOutlets = {
        nativeElement: { querySelector: () => vizEl },
      } as unknown as ElementRef;

      component.handleStartDrag();

      expect(vizEl.style.zIndex).toBe(-1);
    });

    it("handleEndDrag records the final free-drag position", () => {
      component.componentOutlets = {
        nativeElement: { querySelector: () => null },
      } as unknown as ElementRef;
      const source = { getFreeDragPosition: () => ({ x: 12, y: 34 }) };

      component.handleEndDrag({ source } as unknown as CdkDragEnd);

      expect(component.dragPosition).toEqual({ x: 12, y: 34 });
    });
  });

  describe("rerenderResultPanel", () => {
    it("does nothing while previewing a workflow version", () => {
      component.previewWorkflowVersion = true;
      const clearSpy = vi.spyOn(component, "clearResultPanel");

      component.rerenderResultPanel();

      expect(clearSpy).not.toHaveBeenCalled();
    });
  });
});
