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
import { CdkDrag, CdkDragEnd } from "@angular/cdk/drag-drop";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";

import { DEFAULT_HEIGHT, DEFAULT_WIDTH, ResultPanelComponent } from "./result-panel.component";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { PanelResizeService } from "../../service/workflow-result/panel-resize/panel-resize.service";
import { ConsoleFrameComponent } from "./console-frame/console-frame.component";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { By } from "@angular/platform-browser";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalModule } from "ng-zorro-antd/modal";
import {
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
} from "../../service/workflow-graph/model/mock-workflow-data";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { PanelService } from "../../service/panel/panel.service";
import { PYTHON_UDF_V2_OP_TYPE } from "../../service/workflow-graph/model/workflow-graph";
import { OperatorPredicate } from "../../types/workflow-common.interface";

describe("ResultPanelComponent", () => {
  let component: ResultPanelComponent;
  let fixture: ComponentFixture<ResultPanelComponent>;
  let workflowActionService: WorkflowActionService;
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
    workflowActionService = TestBed.inject(WorkflowActionService);
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

  it("should create", () => expect(component).toBeTruthy());

  // The highlight is kept in a root-provided service, so a panel mounted after a hand-over between
  // a workflow's two views can come up beside an operator already shown selected.
  it("reads the highlight once on init, without waiting for an event", () => {
    const update = vi.spyOn(ResultPanelComponent.prototype, "updateShownOperator");

    const fresh = TestBed.createComponent(ResultPanelComponent);
    fresh.detectChanges();

    expect(update).toHaveBeenCalledTimes(1);
    fresh.destroy();
  });

  describe("the operator the console shows", () => {
    const wrapper = () => workflowActionService.getJointGraphWrapper();
    const highlightOnly = (...operatorIds: string[]) => {
      wrapper().unhighlightOperators(...wrapper().getCurrentHighlightedOperatorIDs());
      wrapper().highlightOperators(...operatorIds);
    };

    it("shows every operator's messages while none is highlighted", () => {
      expect(component.currentOperatorId).toBeUndefined();
      expect(component.consoleInputEnabled).toBe(false);
    });

    it("narrows to the one highlighted operator", () => {
      workflowActionService.addOperator(mockResultPredicate, mockPoint);

      highlightOnly(mockResultPredicate.operatorID);

      expect(component.currentOperatorId).toBe(mockResultPredicate.operatorID);
      // a SimpleSink is not a Python UDF, so it takes no debugger input
      expect(component.consoleInputEnabled).toBe(false);
    });

    it("goes back to every operator once the highlight is cleared", () => {
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      highlightOnly(mockResultPredicate.operatorID);

      wrapper().unhighlightOperators(mockResultPredicate.operatorID);

      expect(component.currentOperatorId).toBeUndefined();
    });

    it("shows every operator's messages while several are highlighted", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      // without it, each highlight replaces the one before, as a plain click does
      wrapper().setMultiSelectMode(true);

      highlightOnly(mockScanPredicate.operatorID, mockResultPredicate.operatorID);

      expect(component.currentOperatorId).toBeUndefined();
    });

    it("enables the console input when the highlighted operator is a Python UDF", () => {
      const pythonOp: OperatorPredicate = {
        ...mockResultPredicate,
        operatorID: "py1",
        operatorType: PYTHON_UDF_V2_OP_TYPE,
      };
      // The stub metadata has no Python UDF type, so the graph is stubbed rather than added to.
      vi.spyOn(wrapper(), "getCurrentHighlightedOperatorIDs").mockReturnValue(["py1"]);
      vi.spyOn(workflowActionService.getTexeraGraph(), "hasOperator").mockReturnValue(true);
      vi.spyOn(workflowActionService.getTexeraGraph(), "getOperator").mockReturnValue(pythonOp);

      component.updateShownOperator();

      expect(component.currentOperatorId).toBe("py1");
      expect(component.consoleInputEnabled).toBe(true);
    });

    it("hands the operator and the input switch to the console", () => {
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      component.width = DEFAULT_WIDTH;
      highlightOnly(mockResultPredicate.operatorID);
      fixture.detectChanges();

      const console = fixture.debugElement.query(By.directive(ConsoleFrameComponent))
        .componentInstance as ConsoleFrameComponent;
      expect(console.operatorId).toBe(mockResultPredicate.operatorID);
      expect(console.consoleInputEnabled).toBe(false);
    });
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

    it("handleEndDrag records the final free-drag position", () => {
      const source = { getFreeDragPosition: () => ({ x: 12, y: 34 }) };

      component.handleEndDrag({ source } as unknown as CdkDragEnd);

      expect(component.dragPosition).toEqual({ x: 12, y: 34 });
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

    // The return position is computed against the default height. Docked a different distance
    // above the workspace's edge, a panel opened at that height runs off the bottom of the window.
    it("docks the panel one default height above the workspace's bottom edge", () => {
      const container: HTMLElement = fixture.debugElement.query(By.css("#result-container")).nativeElement;
      expect(container.style.top).toBe(`-${DEFAULT_HEIGHT}px`);
    });

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

    it("titles the panel Console and holds only the console", () => {
      component.width = DEFAULT_WIDTH;
      fixture.detectChanges();

      const title = fixture.debugElement.query(By.css("#title")).nativeElement as HTMLElement;
      expect(title.textContent?.trim()).toBe("Console");
      expect(fixture.debugElement.queryAll(By.directive(ConsoleFrameComponent)).length).toBe(1);
      expect(fixture.debugElement.query(By.css("nz-tabs"))).toBeNull();
    });

    it("names the console in the collapsed open button's tooltip", () => {
      component.width = 0;
      fixture.detectChanges();

      const openButton = fixture.debugElement.query(By.css("#result-buttons li[nz-menu-item]"));
      expect(openButton.injector.get(NzTooltipDirective).directiveTitle).toBe("Open Console");
    });

    it("names the console in the expanded close button's tooltip", () => {
      component.width = DEFAULT_WIDTH;
      fixture.detectChanges();

      const closeButton = fixture.debugElement.query(By.css("#result-buttons li[nz-menu-item]"));
      expect(closeButton.injector.get(NzTooltipDirective).directiveTitle).toBe("Close Console");
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

    it("routes the container's drag and resize outputs to the component handlers", () => {
      // onResize defers its assignment through requestAnimationFrame; run the callback
      // synchronously so the width/height assertions are deterministic.
      const cancelSpy = vi.spyOn(window, "cancelAnimationFrame").mockImplementation(() => {});
      const rafSpy = vi.spyOn(window, "requestAnimationFrame").mockImplementation(cb => {
        cb(0);
        return 1;
      });
      component.width = DEFAULT_WIDTH;
      component.returnPosition = { x: 0, y: 0 };
      fixture.detectChanges();

      try {
        fire("#result-container", "cdkDragEnded", {
          source: { getFreeDragPosition: () => ({ x: 11, y: 22 }) },
        });
        fire("#result-container", "nzResize", { width: 700, height: 400 });

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
