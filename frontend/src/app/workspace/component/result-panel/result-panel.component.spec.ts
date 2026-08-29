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
import { Component, ElementRef, Input } from "@angular/core";
import { CdkDrag, CdkDragEnd } from "@angular/cdk/drag-drop";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";

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
import {
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
} from "../../service/workflow-graph/model/mock-workflow-data";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { PanelService } from "../../service/panel/panel.service";
import { WorkflowCompilingService } from "../../service/compile-workflow/workflow-compiling.service";
import { WorkflowConsoleService } from "../../service/workflow-console/workflow-console.service";
import { CompilationState } from "../../types/workflow-compiling.interface";
import { WorkflowFatalError } from "../../types/workflow-websocket.interface";
import { PYTHON_UDF_V2_OP_TYPE } from "../../service/workflow-graph/model/workflow-graph";
import { OperatorPredicate } from "../../types/workflow-common.interface";

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

  afterEach(() => {
    // Several added tests install spies on the global `window` / shared service
    // instances and drive real streams; restore everything and tear the fixture
    // down so state never leaks into the next test.
    vi.restoreAllMocks();
    fixture.destroy();
    // fixture.destroy() runs ngOnDestroy, which persists the panel dimensions/style
    // into localStorage; those keys are read back in the constructor/ngOnInit and would
    // otherwise leak across specs. Remove only the keys this component writes.
    localStorage.removeItem("result-panel-width");
    localStorage.removeItem("result-panel-height");
    localStorage.removeItem("result-panel-style");
  });

  const makeFatalError = (operatorId: string): WorkflowFatalError => ({
    message: "boom",
    details: "detail",
    operatorId,
    workerId: "",
    type: { name: "ExecutionError" },
    timestamp: { nanos: 0, seconds: 0 },
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

      // vary one axis at a time so each half of the conjunction is individually load-bearing
      component.dragPosition = { x: 5, y: 8 };
      expect(component.isPanelDocked()).toBe(false);

      component.dragPosition = { x: 6, y: 7 };
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

    it("resets the operator title when the highlight selection is cleared", () => {
      // Nothing highlighted -> the single-highlight ternary takes its `undefined` branch,
      // currentOperatorId flips to undefined and the title is wiped.
      vi.spyOn(workflowActionService.getJointGraphWrapper(), "getCurrentHighlightedOperatorIDs").mockReturnValue([]);
      component.currentOperatorId = "3";
      component.operatorTitle = "Old Title";

      component.rerenderResultPanel();

      expect(component.currentOperatorId).toBeUndefined();
      expect(component.operatorTitle).toBe("");
    });

    it("shows an error frame for the whole workflow when execution failed with no operator selected", () => {
      vi.spyOn(workflowActionService.getJointGraphWrapper(), "getCurrentHighlightedOperatorIDs").mockReturnValue([]);
      vi.spyOn(executeWorkflowService, "getExecutionState").mockReturnValue({
        state: ExecutionState.Failed,
        errorMessages: [],
      });

      component.rerenderResultPanel();

      const errorConfig = component.frameComponentConfigs.get("Static Error");
      expect(errorConfig?.component).toBe(ErrorFrameComponent);
      expect(errorConfig?.componentInputs).toEqual({ operatorId: undefined });
    });

    it("shows an operator error frame when the failed execution has a matching fatal error", () => {
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      vi.spyOn(workflowActionService.getJointGraphWrapper(), "getCurrentHighlightedOperatorIDs").mockReturnValue(["3"]);
      vi.spyOn(executeWorkflowService, "getExecutionState").mockReturnValue({
        state: ExecutionState.Failed,
        errorMessages: [makeFatalError("3")],
      });
      // getWorkflowFatalErrors reads getErrorMessages() (guarded on the real currentState),
      // so it must be stubbed alongside getExecutionState.
      vi.spyOn(executeWorkflowService, "getErrorMessages").mockReturnValue([makeFatalError("3")]);

      component.rerenderResultPanel();

      const errorConfig = component.frameComponentConfigs.get("Static Error");
      expect(errorConfig?.component).toBe(ErrorFrameComponent);
      expect(errorConfig?.componentInputs).toEqual({ operatorId: "3" });
    });

    it("tears down a stale error frame when the failed execution has no error for the selected operator", () => {
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      // Keep the highlight aligned with currentOperatorId so the clear-on-selection-change
      // path is skipped and we can prove the delete branch (not the wholesale clear) fires.
      component.currentOperatorId = "3";
      component.frameComponentConfigs.set("Static Error", {
        component: ErrorFrameComponent,
        componentInputs: { operatorId: "3" },
      });
      vi.spyOn(workflowActionService.getJointGraphWrapper(), "getCurrentHighlightedOperatorIDs").mockReturnValue(["3"]);
      vi.spyOn(executeWorkflowService, "getExecutionState").mockReturnValue({
        state: ExecutionState.Failed,
        errorMessages: [makeFatalError("someOtherOperator")],
      });
      // A real fatal error exists, but it belongs to a different operator, so the id filter
      // strips it and the stale frame is removed.
      vi.spyOn(executeWorkflowService, "getErrorMessages").mockReturnValue([makeFatalError("someOtherOperator")]);

      component.rerenderResultPanel();

      expect(component.frameComponentConfigs.has("Static Error")).toBe(false);
    });

    it("shows an operator error frame when the compilation failed with a matching error", () => {
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      const compilingService = TestBed.inject(WorkflowCompilingService);
      vi.spyOn(workflowActionService.getJointGraphWrapper(), "getCurrentHighlightedOperatorIDs").mockReturnValue(["3"]);
      vi.spyOn(compilingService, "getWorkflowCompilationState").mockReturnValue(CompilationState.Failed);
      vi.spyOn(compilingService, "getWorkflowCompilationErrors").mockReturnValue({ "3": makeFatalError("3") });

      component.rerenderResultPanel();

      const errorConfig = component.frameComponentConfigs.get("Static Error");
      expect(errorConfig?.component).toBe(ErrorFrameComponent);
      expect(errorConfig?.componentInputs).toEqual({ operatorId: "3" });
    });

    it("displays a console frame when the selected operator has console messages", () => {
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      const consoleService = TestBed.inject(WorkflowConsoleService);
      vi.spyOn(workflowActionService.getJointGraphWrapper(), "getCurrentHighlightedOperatorIDs").mockReturnValue(["3"]);
      vi.spyOn(consoleService, "hasConsoleMessages").mockReturnValue(true);

      component.rerenderResultPanel();

      const consoleConfig = component.frameComponentConfigs.get("Console");
      expect(consoleConfig?.component).toBe(ConsoleFrameComponent);
      // A SimpleSink is not a Python UDF, so the console input stays disabled.
      expect(consoleConfig?.componentInputs).toEqual({ operatorId: "3", consoleInputEnabled: false });
    });

    it("enables console input when the selected operator is a Python UDF", () => {
      const pythonOp: OperatorPredicate = {
        ...mockResultPredicate,
        operatorID: "3",
        operatorType: PYTHON_UDF_V2_OP_TYPE,
      };
      const consoleService = TestBed.inject(WorkflowConsoleService);
      vi.spyOn(workflowActionService.getJointGraphWrapper(), "getCurrentHighlightedOperatorIDs").mockReturnValue(["3"]);
      vi.spyOn(workflowActionService.getTexeraGraph(), "getOperator").mockReturnValue(pythonOp);
      // No stored console messages -> the Python-UDF branch of the OR is what enables the console.
      vi.spyOn(consoleService, "hasConsoleMessages").mockReturnValue(false);

      component.rerenderResultPanel();

      const consoleConfig = component.frameComponentConfigs.get("Console");
      expect(consoleConfig?.component).toBe(ConsoleFrameComponent);
      expect(consoleConfig?.componentInputs).toEqual({ operatorId: "3", consoleInputEnabled: true });
    });
  });

  describe("auto-open reactions to execution state", () => {
    it("highlights the first active sink when a run completes and it is not already the sole selection", () => {
      // Add the operator first: adding auto-highlights it, so spy only after that settles.
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      const wrapper = workflowActionService.getJointGraphWrapper();
      vi.spyOn(wrapper, "getCurrentHighlightedOperatorIDs").mockReturnValue([]);
      const highlightSpy = vi.spyOn(wrapper, "highlightOperators").mockImplementation(() => {});
      const unhighlightSpy = vi.spyOn(wrapper, "unhighlightOperators").mockImplementation(() => {});

      executeWorkflowService["updateExecutionState"]({ state: ExecutionState.Running });
      executeWorkflowService["updateExecutionState"]({ state: ExecutionState.Completed });

      expect(unhighlightSpy).toHaveBeenCalled();
      expect(highlightSpy).toHaveBeenCalledWith(mockResultPredicate.operatorID);
    });

    it("does not re-highlight a sink that is already the sole highlighted operator", () => {
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      const wrapper = workflowActionService.getJointGraphWrapper();
      vi.spyOn(wrapper, "getCurrentHighlightedOperatorIDs").mockReturnValue([mockResultPredicate.operatorID]);
      const highlightSpy = vi.spyOn(wrapper, "highlightOperators").mockImplementation(() => {});

      executeWorkflowService["updateExecutionState"]({ state: ExecutionState.Running });
      executeWorkflowService["updateExecutionState"]({ state: ExecutionState.Completed });

      expect(highlightSpy).not.toHaveBeenCalled();
    });

    it("highlights an active Python UDF operator when the workflow starts running", () => {
      const pythonOp: OperatorPredicate = {
        ...mockResultPredicate,
        operatorID: "py1",
        operatorType: PYTHON_UDF_V2_OP_TYPE,
      };
      const wrapper = workflowActionService.getJointGraphWrapper();
      vi.spyOn(wrapper, "getCurrentHighlightedOperatorIDs").mockReturnValue([]);
      const highlightSpy = vi.spyOn(wrapper, "highlightOperators").mockImplementation(() => {});
      vi.spyOn(workflowActionService.getTexeraGraph(), "getAllOperators").mockReturnValue([pythonOp]);

      executeWorkflowService["updateExecutionState"]({ state: ExecutionState.Running });

      expect(highlightSpy).toHaveBeenCalledWith("py1");
    });

    it("does not re-highlight a Python UDF when exactly one operator is already highlighted", () => {
      const pythonOp: OperatorPredicate = {
        ...mockResultPredicate,
        operatorID: "py1",
        operatorType: PYTHON_UDF_V2_OP_TYPE,
      };
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      const wrapper = workflowActionService.getJointGraphWrapper();
      // A single (real) operator is already selected, so the guard short-circuits and skips highlighting.
      vi.spyOn(wrapper, "getCurrentHighlightedOperatorIDs").mockReturnValue([mockResultPredicate.operatorID]);
      const highlightSpy = vi.spyOn(wrapper, "highlightOperators").mockImplementation(() => {});
      vi.spyOn(workflowActionService.getTexeraGraph(), "getAllOperators").mockReturnValue([pythonOp]);

      executeWorkflowService["updateExecutionState"]({ state: ExecutionState.Running });

      expect(highlightSpy).not.toHaveBeenCalled();
    });

    it("does not touch the highlight selection when a run completes with no sink operators", () => {
      // A source-only workflow has no sink, so the sink-highlight guard takes its empty branch.
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const wrapper = workflowActionService.getJointGraphWrapper();
      const highlightSpy = vi.spyOn(wrapper, "highlightOperators").mockImplementation(() => {});

      executeWorkflowService["updateExecutionState"]({ state: ExecutionState.Running });
      executeWorkflowService["updateExecutionState"]({ state: ExecutionState.Completed });

      expect(highlightSpy).not.toHaveBeenCalled();
    });
  });

  describe("state-change driven rerender", () => {
    it("renders an error frame when the execution transitions into the failed state", () => {
      vi.spyOn(workflowActionService.getJointGraphWrapper(), "getCurrentHighlightedOperatorIDs").mockReturnValue([]);

      executeWorkflowService["updateExecutionState"]({ state: ExecutionState.Failed, errorMessages: [] });

      expect(component.frameComponentConfigs.has("Static Error")).toBe(true);
    });

    it("clears the error frame when the execution recovers out of the failed state", () => {
      vi.spyOn(workflowActionService.getJointGraphWrapper(), "getCurrentHighlightedOperatorIDs").mockReturnValue([]);

      // into Failed: the error frame appears...
      executeWorkflowService["updateExecutionState"]({ state: ExecutionState.Failed, errorMessages: [] });
      expect(component.frameComponentConfigs.has("Static Error")).toBe(true);

      // ...and leaving Failed forces a rerender that tears it back down.
      executeWorkflowService["updateExecutionState"]({ state: ExecutionState.Running });
      expect(component.frameComponentConfigs.has("Static Error")).toBe(false);
    });
  });

  describe("operator display name changes", () => {
    beforeEach(() => {
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      // Drive a rerender with the operator selected so the display-name handler subscribes.
      vi.spyOn(workflowActionService.getJointGraphWrapper(), "getCurrentHighlightedOperatorIDs").mockReturnValue(["3"]);
      executeWorkflowService["updateExecutionState"]({ state: ExecutionState.Running });
    });

    it("updates the operator title when the selected operator is renamed", () => {
      // The subject is hidden from the readonly graph type; reach it to drive the stream.
      (workflowActionService.getTexeraGraph() as any).operatorDisplayNameChangedSubject.next({
        operatorID: "3",
        newDisplayName: "Renamed Result",
      });

      expect(component.operatorTitle).toBe("Renamed Result");
    });

    it("ignores rename events for other operators", () => {
      const titleBefore = component.operatorTitle;

      (workflowActionService.getTexeraGraph() as any).operatorDisplayNameChangedSubject.next({
        operatorID: "someOtherOperator",
        newDisplayName: "Should Not Apply",
      });

      expect(component.operatorTitle).toBe(titleBefore);
    });
  });

  describe("panel open/close reactions", () => {
    it("collapses the panel when the panel service requests a close", () => {
      const panelService = TestBed.inject(PanelService);
      component.openPanel();

      panelService.closePanels();

      expect(component.width).toBe(0);
      expect(component.height).toBe(32.5);
    });

    it("re-docks and re-opens the panel when the panel service requests a reset", () => {
      const panelService = TestBed.inject(PanelService);
      component.returnPosition = { x: 11, y: 22 };
      component.dragPosition = { x: 99, y: 88 };

      panelService.resetPanels();

      expect(component.dragPosition).toEqual({ x: 11, y: 22 });
      expect(component.width).toBe(DEFAULT_WIDTH);
      expect(component.height).toBe(DEFAULT_HEIGHT);
    });

    it("opens the panel when the result-panel-open stream emits true", () => {
      component.closePanel();

      workflowActionService.openResultPanel();

      expect(component.width).toBe(DEFAULT_WIDTH);
      expect(component.height).toBe(DEFAULT_HEIGHT);
    });

    it("closes the panel when the result-panel-open stream emits false", () => {
      component.openPanel();

      workflowActionService.closeResultPanel();

      expect(component.width).toBe(0);
      expect(component.height).toBe(32.5);
    });
  });

  describe("drag overlay restore", () => {
    it("handleEndDrag restores the visualization overlay z-index when it is present", () => {
      const vizEl = { style: { zIndex: -1 } };
      component.componentOutlets = {
        nativeElement: { querySelector: () => vizEl },
      } as unknown as ElementRef;
      const source = { getFreeDragPosition: () => ({ x: 7, y: 8 }) };

      component.handleEndDrag({ source } as unknown as CdkDragEnd);

      expect(vizEl.style.zIndex).toBe(0);
      expect(component.dragPosition).toEqual({ x: 7, y: 8 });
    });

    it("handleStartDrag is a no-op when no visualization overlay is present", () => {
      component.componentOutlets = {
        nativeElement: { querySelector: () => null },
      } as unknown as ElementRef;

      expect(() => component.handleStartDrag()).not.toThrow();
    });
  });

  describe("getWorkflowFatalErrors filtering", () => {
    it("returns every fatal error unfiltered when no operator id is provided", () => {
      vi.spyOn(executeWorkflowService, "getErrorMessages").mockReturnValue([
        makeFatalError("opA"),
        makeFatalError("opB"),
      ]);

      const errors = (component as any).getWorkflowFatalErrors();

      expect(errors.map((e: WorkflowFatalError) => e.operatorId)).toEqual(["opA", "opB"]);
    });
  });

  describe("persistence on unload", () => {
    it("writes the current panel dimensions to localStorage on window beforeunload", () => {
      const setItemSpy = vi.spyOn(Storage.prototype, "setItem");
      component.width = 640;
      component.height = 480;

      window.dispatchEvent(new Event("beforeunload"));

      expect(setItemSpy).toHaveBeenCalledWith("result-panel-width", "640");
      expect(setItemSpy).toHaveBeenCalledWith("result-panel-height", "480");
    });

    it("persists dimensions but skips the style entry when the result container is gone", () => {
      // If the container element has already been torn down, only the width/height are saved.
      vi.spyOn(document, "getElementById").mockReturnValue(null);
      const setItemSpy = vi.spyOn(Storage.prototype, "setItem");
      component.width = 640;
      component.height = 480;

      component.ngOnDestroy();

      expect(setItemSpy).toHaveBeenCalledWith("result-panel-width", "640");
      expect(setItemSpy).toHaveBeenCalledWith("result-panel-height", "480");
      expect(setItemSpy).not.toHaveBeenCalledWith("result-panel-style", expect.anything());
    });
  });

  describe("template rendering", () => {
    // Query, assert the element is present, then dispatch the event (a real MouseEvent
    // for clicks so handlers calling stopPropagation()/preventDefault() work).
    const fire = (css: string, event: string, payload: unknown): void => {
      const el = fixture.debugElement.query(By.css(css));
      expect(el).toBeTruthy();
      el.triggerEventHandler(event, payload);
    };

    it("renders the docked panel chrome and wires the close / reset actions", () => {
      component.width = DEFAULT_WIDTH;
      fixture.detectChanges();
      const closeSpy = vi.spyOn(component, "closePanel").mockImplementation(() => {});
      const resetSpy = vi.spyOn(component, "resetPanelPosition").mockImplementation(() => {});

      expect(fixture.debugElement.query(By.css("#content"))).toBeTruthy();
      // Assert after each fire so the three controls are individually attributable: a
      // single spy call cannot stand in for all of them, and swapping the two in-panel
      // handlers changes which counter moves.
      fire("#result-buttons li[nz-menu-item]", "click", new MouseEvent("click")); // header close
      expect(closeSpy).toHaveBeenCalledTimes(1);
      expect(resetSpy).not.toHaveBeenCalled();

      fire("#panel-button button", "click", new MouseEvent("click")); // reset-position
      expect(resetSpy).toHaveBeenCalledTimes(1);
      expect(closeSpy).toHaveBeenCalledTimes(1);

      fire("#panel-button li[nz-menu-item]", "click", new MouseEvent("click")); // in-panel close
      expect(closeSpy).toHaveBeenCalledTimes(2);
      expect(resetSpy).toHaveBeenCalledTimes(1);
    });

    it("renders only the collapsed open button and wires openPanel when width is 0", () => {
      component.width = 0;
      fixture.detectChanges();
      const openSpy = vi.spyOn(component, "openPanel").mockImplementation(() => {});

      // the panel body is hidden while collapsed
      expect(fixture.debugElement.query(By.css("#content"))).toBeNull();
      fire("#result-buttons li[nz-menu-item]", "click", new MouseEvent("click")); // the open item

      expect(openSpy).toHaveBeenCalled();
    });

    it("interpolates the operator title into the panel title", () => {
      component.width = DEFAULT_WIDTH;
      component.operatorTitle = "My Operator";
      fixture.detectChanges();

      const title = fixture.debugElement.query(By.css("#title")).nativeElement as HTMLElement;
      expect(title.textContent).toContain("Result Panel: My Operator");
    });

    it("omits the separator in the title when no operator is selected", () => {
      component.width = DEFAULT_WIDTH;
      component.operatorTitle = "";
      fixture.detectChanges();

      const title = fixture.debugElement.query(By.css("#title")).nativeElement as HTMLElement;
      expect(title.textContent?.trim()).toBe("Result Panel");
    });

    it("shows the no-results tab when there are no frames", () => {
      component.width = DEFAULT_WIDTH;
      component.frameComponentConfigs.clear();
      fixture.detectChanges();

      expect((fixture.nativeElement as HTMLElement).textContent).toContain("No results available to display.");
    });

    it("renders a tab per frame when frames are present", () => {
      component.width = DEFAULT_WIDTH;
      // Seed real inputs: the outlet binds `inputs: config.value.componentInputs`, and that is
      // the only thing that delivers operatorId into the real result / console / error frames.
      component.frameComponentConfigs.set("Result", {
        component: StubFrame,
        componentInputs: { operatorId: "op-result" },
      });
      component.frameComponentConfigs.set("Console", {
        component: StubFrame,
        componentInputs: { operatorId: "op-console" },
      });
      fixture.detectChanges();

      const text = (fixture.nativeElement as HTMLElement).textContent ?? "";
      expect(text).toContain("Result");
      expect(text).toContain("Console");
      expect(text).not.toContain("No results available to display.");

      const renderedInputs = fixture.debugElement
        .queryAll(By.directive(StubFrame))
        .map(frame => (frame.componentInstance as StubFrame).operatorId);
      expect(renderedInputs.length).toBeGreaterThan(0);
      expect(renderedInputs).not.toContain(undefined);
      renderedInputs.forEach(operatorId => expect(["op-result", "op-console"]).toContain(operatorId));
    });

    it("renders the resize handles when the panel is docked", () => {
      component.width = DEFAULT_WIDTH;
      vi.spyOn(component, "isPanelDocked").mockReturnValue(true);
      fixture.detectChanges();

      expect(fixture.debugElement.query(By.css("nz-resize-handles"))).toBeTruthy();
    });

    // The two tests below leave isPanelDocked unmocked on purpose: the real predicate has to run
    // for the template's `isPanelDocked() ? ... : ...` direction list to be exercised both ways.
    // The spy is installed without an implementation, so behaviour is untouched; asserting that
    // it was called is what stops the call being replaced by an inline template expression.
    it("offers only the right handle while the panel sits docked in its return position", () => {
      component.width = DEFAULT_WIDTH;
      component.returnPosition = { x: 0, y: 0 };
      component.dragPosition = { x: 0, y: 0 };
      const dockedSpy = vi.spyOn(component, "isPanelDocked");
      fixture.detectChanges();

      const handles = fixture.debugElement.queryAll(By.css("nz-resize-handle"));
      expect(dockedSpy).toHaveBeenCalled();
      expect(handles.length).toBe(1);
      expect((handles[0].nativeElement as HTMLElement).className).toContain("nz-resizable-handle-right");
    });

    it("offers all three resize directions once the panel has been dragged away from its return position", () => {
      component.width = DEFAULT_WIDTH;
      component.returnPosition = { x: 0, y: 0 };
      // Only the y axis moves, so a direction list keyed off the x axis alone would still
      // report the panel as docked and render a single handle.
      component.dragPosition = { x: 0, y: 60 };
      const dockedSpy = vi.spyOn(component, "isPanelDocked");
      fixture.detectChanges();

      const classNames = fixture.debugElement
        .queryAll(By.css("nz-resize-handle"))
        .map(handle => (handle.nativeElement as HTMLElement).className);
      expect(dockedSpy).toHaveBeenCalled();
      expect(classNames.length).toBe(3);
      expect(classNames.some(c => c.includes("nz-resizable-handle-right"))).toBe(true);
      expect(classNames.some(c => c.includes("nz-resizable-handle-bottom"))).toBe(true);
      expect(classNames.some(c => c.includes("nz-resizable-handle-bottomRight"))).toBe(true);
    });

    it("names the selected operator in the collapsed open button's tooltip", () => {
      component.width = 0;
      component.operatorTitle = "My Operator";
      fixture.detectChanges();

      const openButton = fixture.debugElement.query(By.css("#result-buttons li[nz-menu-item]"));
      expect(openButton.injector.get(NzTooltipDirective).directiveTitle).toBe("Open Result Panel: My Operator");
    });

    it("omits the separator from the collapsed open button's tooltip when no operator is selected", () => {
      component.width = 0;
      component.operatorTitle = "";
      fixture.detectChanges();

      const openButton = fixture.debugElement.query(By.css("#result-buttons li[nz-menu-item]"));
      expect(openButton.injector.get(NzTooltipDirective).directiveTitle).toBe("Open Result Panel");
    });

    // The expanded panel swaps the same menu slot for a Close item carrying its own,
    // near-identical tooltip; both arms of its ternary need their own assertions or the
    // interpolation is only executed, never read.
    it("names the selected operator in the expanded close button's tooltip", () => {
      component.width = DEFAULT_WIDTH;
      component.operatorTitle = "My Operator";
      fixture.detectChanges();

      const closeButton = fixture.debugElement.query(By.css("#result-buttons li[nz-menu-item]"));
      expect(closeButton.injector.get(NzTooltipDirective).directiveTitle).toBe("Close Result Panel: My Operator");
    });

    it("omits the separator from the expanded close button's tooltip when no operator is selected", () => {
      component.width = DEFAULT_WIDTH;
      component.operatorTitle = "";
      fixture.detectChanges();

      const closeButton = fixture.debugElement.query(By.css("#result-buttons li[nz-menu-item]"));
      expect(closeButton.injector.get(NzTooltipDirective).directiveTitle).toBe("Close Result Panel");
    });

    it("routes the container's drag and resize outputs to the component handlers", () => {
      // onResize defers its assignment through requestAnimationFrame; run the callback
      // synchronously so the width/height assertions are deterministic.
      const cancelSpy = vi.spyOn(window, "cancelAnimationFrame").mockImplementation(() => {});
      const rafSpy = vi.spyOn(window, "requestAnimationFrame").mockImplementation(cb => {
        cb(0);
        return 1;
      });
      // handleStartDrag/handleEndDrag dereference the #dynamicComponent ViewChild, which only
      // resolves once the panel is open and has at least one frame to render.
      component.width = DEFAULT_WIDTH;
      component.returnPosition = { x: 0, y: 0 };
      component.frameComponentConfigs.set("Result", { component: StubFrame, componentInputs: {} });
      fixture.detectChanges();
      const startDragSpy = vi.spyOn(component, "handleStartDrag");

      try {
        fire("#result-container", "cdkDragStarted", {});
        fire("#result-container", "cdkDragEnded", {
          source: { getFreeDragPosition: () => ({ x: 11, y: 22 }) },
        });
        fire("#result-container", "nzResize", { width: 700, height: 400 });

        expect(startDragSpy).toHaveBeenCalledTimes(1);
        expect(component.dragPosition).toEqual({ x: 11, y: 22 });
        expect(component.width).toBe(700);
        expect(component.height).toBe(400);

        // Recording the fields is only half the wiring: push a change-detection pass and
        // assert the recorded values actually reach the DOM, i.e. that the container is
        // sized from width/height and keeps the dropped position rather than snapping back.
        fixture.detectChanges();
        const container = fixture.debugElement.query(By.css("#result-container"));
        expect(container.injector.get(CdkDrag).freeDragPosition).toEqual({ x: 11, y: 22 });
        const host = container.nativeElement as HTMLElement;
        expect(host.style.width).toBe("700px");
        expect(host.style.height).toBe("400px");
      } finally {
        rafSpy.mockRestore();
        cancelSpy.mockRestore();
      }
    });
  });
});

// A trivial standalone frame so *ngComponentOutlet can instantiate a tab's content
// without pulling in a real result-frame component's dependency graph. It declares the
// one input every real frame takes, so the outlet's `inputs:` half is observable.
@Component({ standalone: true, template: "" })
class StubFrame {
  @Input() operatorId?: string;
}
