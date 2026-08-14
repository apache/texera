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

import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../../service/undo-redo/undo-redo.service";
import { DragDropService } from "../../service/drag-drop/drag-drop.service";
import { WorkflowUtilService } from "../../service/workflow-graph/util/workflow-util.service";
import { ComponentFixture, fakeAsync, TestBed, tick } from "@angular/core/testing";
import { ValidationWorkflowService } from "../../service/validation/validation-workflow.service";
import { WorkflowEditorComponent } from "./workflow-editor.component";
import { workflowEditorTestImports, workflowEditorTestProviders } from "./workflow-editor.test-utils";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import {
  JointUIService,
  operatorAgentActionProgressClass,
  operatorNameClass,
} from "../../service/joint-ui/joint-ui.service";
import { AgentService, OperatorResultSummary } from "../../service/agent/agent.service";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";
import { Overlay } from "@angular/cdk/overlay";
import * as joint from "jointjs";
import { marbles } from "rxjs-marbles";
import {
  mockCommentBox,
  mockMultiInputOutputPredicate,
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink,
  mockScanSentimentLink,
  mockSentimentPredicate,
} from "../../service/workflow-graph/model/mock-workflow-data";
import { WorkflowStatusService } from "../../service/workflow-status/workflow-status.service";
import { ExecutionState, OperatorState } from "../../types/execute-workflow.interface";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { OperatorLink, OperatorPredicate } from "../../types/workflow-common.interface";
import { tap } from "rxjs/operators";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { config as rxjsConfig, of, Subject } from "rxjs";
import { NzContextMenuService, NzDropDownModule } from "ng-zorro-antd/dropdown";
import { ActivatedRoute, Router } from "@angular/router";
import { RouterTestingModule } from "@angular/router/testing";
import { ContextMenuComponent } from "./context-menu/context-menu/context-menu.component";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { OperatorMenuService } from "../../service/operator-menu/operator-menu.service";
import { GuiConfigService } from "src/app/common/service/gui-config.service";
import { MockGuiConfigService } from "src/app/common/service/gui-config.service.mock";
import {
  OperatorRecommendation,
  OperatorRecommendationService,
} from "../../service/operator-recommendation/operator-recommendation.service";
import type { MockInstance } from "vitest";

describe("WorkflowEditorComponent", () => {
  /**
   * This sub test suite test if the JointJS paper is integrated with our Angular component well.
   * It uses a fake stub Workflow model that only provides the binding of JointJS graph.
   * It tests if manipulating the JointJS graph is correctly shown in the UI.
   */
  describe("JointJS Paper", () => {
    let component: WorkflowEditorComponent;
    let fixture: ComponentFixture<WorkflowEditorComponent>;
    let jointGraph: joint.dia.Graph;

    beforeEach(async () => {
      await TestBed.configureTestingModule({
        imports: [
          RouterTestingModule,
          HttpClientTestingModule,
          NzModalModule,
          NzDropDownModule,
          WorkflowEditorComponent,
          ContextMenuComponent,
        ],
        providers: [
          JointUIService,
          WorkflowUtilService,
          UndoRedoService,
          DragDropService,
          ValidationWorkflowService,
          WorkflowActionService,
          NzContextMenuService,
          Overlay,
          {
            provide: OperatorMetadataService,
            useClass: StubOperatorMetadataService,
          },
          { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
          WorkflowStatusService,
          ExecuteWorkflowService,
          ...commonTestProviders,
        ],
      }).compileComponents();
    });

    beforeEach(() => {
      fixture = TestBed.createComponent(WorkflowEditorComponent);
      component = fixture.componentInstance;
      // detect changes first to run ngAfterViewInit and bind Model
      fixture.detectChanges();
      jointGraph = component.paper.model;
    });

    it("should create", () => {
      expect(component).toBeTruthy();
    });

    it("should hide operator status on the canvas by default", () => {
      // keeps the Status toggle off until the user enables it
      const editor = (component as any).editor as HTMLElement;
      expect(editor.classList.contains("hide-operator-status")).toBe(true);
    });

    // Drives the region-update stream the editor subscribes to in handleRegionEvents, creating
    // region-<id> elements around the given operator, and returns the operator id used.
    function emitRegionUpdate(regionId: number): string {
      const operatorID = `region_op_${regionId}`;
      const operator = new joint.shapes.basic.Rect({ position: { x: 0, y: 0 }, size: { width: 80, height: 40 } });
      operator.set("id", operatorID);
      jointGraph.addCell(operator);
      const executeWorkflowService = TestBed.inject(ExecuteWorkflowService);
      (executeWorkflowService as any).regionUpdateStream.next({ regions: [[regionId, [operatorID]]] });
      return operatorID;
    }

    it("should create region elements hidden so the Regions toggle starts off on canvas and mini-map", () => {
      emitRegionUpdate(1);

      const region = jointGraph.getCell("region-1");
      expect(region).toBeTruthy();
      // region visibility is a shared-model attribute, so hidden-by-default applies to both surfaces
      expect(region.attr("body/visibility")).toBe("hidden");
    });

    it("should show regions created during execution when the toggle is already on", () => {
      // user enables Regions, then execution emits region updates
      const wrapper = TestBed.inject(WorkflowActionService).getJointGraphWrapper();
      wrapper.setRegionsDisplayed(true);
      emitRegionUpdate(1);

      expect(jointGraph.getCell("region-1").attr("body/visibility")).toBe("visible");
    });

    it("should keep regions visible when they are recreated on a later execution update", () => {
      const wrapper = TestBed.inject(WorkflowActionService).getJointGraphWrapper();
      wrapper.setRegionsDisplayed(true);
      emitRegionUpdate(1);
      // a subsequent update removes and recreates the region elements
      emitRegionUpdate(2);

      expect(jointGraph.getCell("region-2").attr("body/visibility")).toBe("visible");
    });

    it("should toggle visibility of existing regions when the displayed flag changes", () => {
      const wrapper = TestBed.inject(WorkflowActionService).getJointGraphWrapper();
      emitRegionUpdate(1);
      expect(jointGraph.getCell("region-1").attr("body/visibility")).toBe("hidden");

      wrapper.setRegionsDisplayed(true);
      expect(jointGraph.getCell("region-1").attr("body/visibility")).toBe("visible");

      wrapper.setRegionsDisplayed(false);
      expect(jointGraph.getCell("region-1").attr("body/visibility")).toBe("hidden");
    });

    it("should create element in the UI after adding operator in the model", () => {
      const operatorID = "test_one_operator_1";

      const element = new joint.shapes.basic.Rect();
      element.set("id", operatorID);

      jointGraph.addCell(element);

      expect(component.paper.findViewByModel(element.id)).toBeTruthy();
    });

    it("should create a graph of multiple cells in the UI", () => {
      const operator1 = "test_multiple_1_op_1";
      const operator2 = "test_multiple_1_op_2";

      const element1 = new joint.shapes.basic.Rect({
        size: { width: 100, height: 50 },
        position: { x: 100, y: 400 },
      });
      element1.set("id", operator1);

      const element2 = new joint.shapes.basic.Rect({
        size: { width: 100, height: 50 },
        position: { x: 100, y: 400 },
      });
      element2.set("id", operator2);

      const link1 = new joint.dia.Link({
        source: { id: operator1 },
        target: { id: operator2 },
      });

      jointGraph.addCell(element1);
      jointGraph.addCell(element2);
      jointGraph.addCell(link1);

      // check the model is added correctly
      expect(jointGraph.getElements().find(el => el.id === operator1)).toBeTruthy();
      expect(jointGraph.getElements().find(el => el.id === operator2)).toBeTruthy();
      expect(jointGraph.getLinks().find(link => link.id === link1.id)).toBeTruthy();

      // check the view is updated correctly
      expect(component.paper.findViewByModel(element1.id)).toBeTruthy();
      expect(component.paper.findViewByModel(element2.id)).toBeTruthy();
      expect(component.paper.findViewByModel(link1.id)).toBeTruthy();
    });
  });

  /**
   * This sub test suites test the Integration of WorkflowEditorComponent with external modules,
   *  such as drag and drop module, and highlight operator module.
   */
  describe("External Module Integration", () => {
    let component: WorkflowEditorComponent;
    let fixture: ComponentFixture<WorkflowEditorComponent>;
    let workflowActionService: WorkflowActionService;
    let validationWorkflowService: ValidationWorkflowService;
    let dragDropService: DragDropService;
    let jointUIService: JointUIService;
    let undoRedoService: UndoRedoService;
    let workflowVersionService: WorkflowVersionService;

    beforeEach(async () => {
      await TestBed.configureTestingModule({
        imports: workflowEditorTestImports,
        providers: workflowEditorTestProviders,
      }).compileComponents();
    });

    beforeEach(() => {
      fixture = TestBed.createComponent(WorkflowEditorComponent);
      component = fixture.componentInstance;
      workflowActionService = TestBed.inject(WorkflowActionService);
      workflowActionService.setHighlightingEnabled(true);
      validationWorkflowService = TestBed.inject(ValidationWorkflowService);
      dragDropService = TestBed.inject(DragDropService);
      // detect changes to run ngAfterViewInit and bind Model
      jointUIService = TestBed.inject(JointUIService);
      undoRedoService = TestBed.inject(UndoRedoService);
      workflowVersionService = TestBed.inject(WorkflowVersionService);
      fixture.detectChanges();
    });

    it("should react to operator highlight event and change the appearance of the operator to be highlighted", () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
      workflowActionService.addOperator(mockScanPredicate, mockPoint);

      // highlight the operator
      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

      // find the joint Cell View object of the operator element
      const jointCellView = component.paper.findViewByModel(mockScanPredicate.operatorID);

      // find the cell's child element with the joint highlighter class name `joint-highlight-stroke`
      const jointHighlighterElements = jointCellView.$el.children(".joint-highlight-stroke");

      // the element should have the highlighter element in it
      expect(jointHighlighterElements.length).toEqual(1);
    });

    it("should react to operator unhighlight event and change the appearance of the operator to be unhighlighted", () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
      workflowActionService.addOperator(mockScanPredicate, mockPoint);

      // highlight the oprator first
      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

      // find the joint Cell View object of the operator element
      const jointCellView = component.paper.findViewByModel(mockScanPredicate.operatorID);

      // find the cell's child element with the joint highlighter class name `joint-highlight-stroke`
      const jointHighlighterElements = jointCellView.$el.children(".joint-highlight-stroke");

      // the element should have the highlighter element in it right now
      expect(jointHighlighterElements.length).toEqual(1);

      // then unhighlight the operator
      jointGraphWrapper.unhighlightOperators(mockScanPredicate.operatorID);

      // the highlighter element should not exist
      const jointHighlighterElementAfterUnhighlight = jointCellView.$el.children(".joint-highlight-stroke");
      expect(jointHighlighterElementAfterUnhighlight.length).toEqual(0);
    });

    it("pulls the active agent's operator results when an operator's chat popover opens", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const jointCellView = component.paper.findViewByModel(mockScanPredicate.operatorID);

      const agentService = TestBed.inject(AgentService);
      vi.spyOn(agentService, "getActivelyConnectedAgentIds").mockReturnValue(["agent-1"]);
      const fetchSpy = vi.spyOn(agentService, "fetchOperatorResults").mockImplementation(() => {});

      // The operator's chat button fires `element:chat` (cell view, DOM event, x, y);
      // opening the popover should pull the active agent's results on demand.
      (component.paper as any).trigger("element:chat", jointCellView, new Event("click"), 0, 0);

      expect(fetchSpy).toHaveBeenCalledWith("agent-1");
    });

    it("does not pull operator results when no agent is connected", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const jointCellView = component.paper.findViewByModel(mockScanPredicate.operatorID);

      const agentService = TestBed.inject(AgentService);
      vi.spyOn(agentService, "getActivelyConnectedAgentIds").mockReturnValue([]);
      const fetchSpy = vi.spyOn(agentService, "fetchOperatorResults").mockImplementation(() => {});

      (component.paper as any).trigger("element:chat", jointCellView, new Event("click"), 0, 0);

      expect(fetchSpy).not.toHaveBeenCalled();
    });

    it("should react to operator validation and change the color of operator box if the operator is valid ", () => {
      workflowActionService.getJointGraphWrapper();
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      workflowActionService.addLink(mockScanResultLink);
      const newProperty = { tableName: "test-table" };
      workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, newProperty);
      const operator1 = component.paper.getModelById(mockScanPredicate.operatorID);
      const operator2 = component.paper.getModelById(mockResultPredicate.operatorID);
      expect(operator1.attr("rect/stroke")).not.toEqual("red");
      expect(operator2.attr("rect/stroke")).not.toEqual("red");
    });

    it("should validate operator connections correctly", () => {
      const mockScan2Predicate = {
        ...mockScanPredicate,
        operatorID: "mockScan2",
      };

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockScan2Predicate, mockPoint);
      workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);

      // should allow a link from scan to sentiment
      expect(
        component["validateOperatorConnection"](
          mockScanPredicate.operatorID,
          "output-0",
          mockSentimentPredicate.operatorID,
          "input-0"
        )
      ).toBe(true);

      // add a link from scan to sentiment
      workflowActionService.addLink(mockScanSentimentLink);

      // should not allow a link from scan to sentiment anymore
      expect(
        component["validateOperatorConnection"](
          mockScanPredicate.operatorID,
          "output-0",
          mockSentimentPredicate.operatorID,
          "input-0"
        )
      ).toBe(false);

      // should not allow a link from scan 2 to sentiment anymore
      expect(
        component["validateOperatorConnection"](
          mockScan2Predicate.operatorID,
          "output-0",
          mockSentimentPredicate.operatorID,
          "input-0"
        )
      ).toBe(true);

      // should still allow a link from scan to view result
      expect(
        component["validateOperatorConnection"](
          mockScanPredicate.operatorID,
          "output-0",
          mockResultPredicate.operatorID,
          "input-0"
        )
      ).toBe(true);

      // add a link from scan to view result
      workflowActionService.addLink(mockScanResultLink);

      // should not allow a link from scan to view result anymore
      expect(
        component["validateOperatorConnection"](
          mockScanPredicate.operatorID,
          "output-0",
          mockResultPredicate.operatorID,
          "input-0"
        )
      ).toBe(false);

      // should not allow a link from sentiment to view result anymore
      expect(
        component["validateOperatorConnection"](
          mockSentimentPredicate.operatorID,
          "output-0",
          mockResultPredicate.operatorID,
          "input-0"
        )
      ).toBe(true);
    });

    it("should validate operator connections with ports that allow multi-inputs correctly", () => {
      // union operator metadata specifys that input-0 port allows multiple inputs connected to the same port
      const mockUnionPredicate: OperatorPredicate = {
        operatorID: "union-1",
        operatorType: "Union",
        operatorVersion: "u1",
        operatorProperties: {},
        inputPorts: [{ portID: "input-0" }],
        outputPorts: [{ portID: "output-0" }],
        showAdvanced: false,
        isDisabled: false,
      };
      workflowActionService.getJointGraphWrapper();
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
      workflowActionService.addOperator(mockUnionPredicate, mockPoint);

      // should allow a link from scan to union
      expect(
        component["validateOperatorConnection"](
          mockScanPredicate.operatorID,
          "output-0",
          mockUnionPredicate.operatorID,
          "input-0"
        )
      ).toBe(true);

      // should allow a link from sentiment to union
      expect(
        component["validateOperatorConnection"](
          mockSentimentPredicate.operatorID,
          "output-0",
          mockUnionPredicate.operatorID,
          "input-0"
        )
      ).toBe(true);

      // add a link from scan to union
      const mockScanUnionLink: OperatorLink = {
        linkID: "mockScanUnion",
        source: {
          operatorID: mockScanPredicate.operatorID,
          portID: "output-0",
        },
        target: {
          operatorID: mockUnionPredicate.operatorID,
          portID: "input-0",
        },
      };
      workflowActionService.addLink(mockScanUnionLink);

      // should still allow a link from sentiment to union
      expect(
        component["validateOperatorConnection"](
          mockSentimentPredicate.operatorID,
          "output-0",
          mockUnionPredicate.operatorID,
          "input-0"
        )
      ).toBe(true);
    });

    it(
      "should react to jointJS paper zoom event",
      marbles(m => {
        const mockScaleRatio = 0.5;
        m.hot("-e-")
          .pipe(tap(() => workflowActionService.getJointGraphWrapper().setZoomProperty(mockScaleRatio)))
          .subscribe(() => {
            const currentScale = component.paper.scale();
            expect(currentScale.sx).toEqual(mockScaleRatio);
            expect(currentScale.sy).toEqual(mockScaleRatio);
          });
      })
    );

    it(
      "should react to jointJS paper restore default offset event",
      marbles(m => {
        const mockTranslation = 20;
        const originalOffset = component.paper.translate();
        component.paper.translate(mockTranslation, mockTranslation);
        expect(component.paper.translate().tx).not.toEqual(originalOffset.tx);
        expect(component.paper.translate().ty).not.toEqual(originalOffset.ty);
        m.hot("-e-")
          .pipe(tap(() => workflowActionService.getJointGraphWrapper().restoreDefaultZoomAndOffset()))
          .subscribe(() => {
            expect(component.paper.translate().tx).toEqual(originalOffset.tx);
            expect(component.paper.translate().ty).toEqual(originalOffset.ty);
          });
      })
    );

    //   // TODO: this test case related to websocket is not stable, find out why and fix it
    // xdescribe('when executionStatus is enabled', () => {
    //   beforeAll(() => {
    //     environment.executionStatusEnabled = true;
    //     workflowStatusService = TestBed.get(WorkflowStatusService);
    //   });

    //   afterAll(() => {
    //     environment.executionStatusEnabled = false;
    //   });

    //   it('should display/hide operator status tooltip when cursor hovers/leaves an operator', () => {
    //     // install a spy on the highlight operator function and pass the call through
    //     const showTooltipFunctionSpy = vi.spyOn(jointUIService, 'showOperatorStatusToolTip');
    //     const hideTooltipFunctionSpy = vi.spyOn(jointUIService, 'hideOperatorStatusToolTip');

    //     workflowActionService.addOperator(mockScanPredicate, mockPoint);
    //     // find the joint Cell View object of the operator element
    //     const jointCellView = component.getJointPaper().findViewByModel(mockScanPredicate.operatorID);
    //     const tooltipView = component.getJointPaper().findViewByModel(
    //       JointUIService.getOperatorStatusTooltipElementID(mockScanPredicate.operatorID));

    //     // workflow has not started yet
    //     // trigger a mouseenter on the cell view using its jQuery element
    //     jointCellView.$el.trigger('mouseenter');
    //     fixture.detectChanges();
    //     // assert the function is not called yet
    //     expect(showTooltipFunctionSpy).not.toHaveBeenCalled();
    //     expect(tooltipView.model.attr('polygon')['display']).toBe('none');

    //     // mock start the workflow
    //     component['operatorStatusTooltipDisplayEnabled'] = true;
    //     // trigger event mouse enter
    //     jointCellView.$el.trigger('mouseenter');
    //     fixture.detectChanges();
    //     // assert the function is called
    //     expect(showTooltipFunctionSpy).toHaveBeenCalled();
    //     expect(tooltipView.model.attr('polygon')['display']).toBeUndefined();

    //     // trigger event mouse leave
    //     jointCellView.$el.trigger('mouseleave');
    //     // assert the function is called
    //     expect(hideTooltipFunctionSpy).toHaveBeenCalled();
    //     expect(tooltipView.model.attr('polygon')['display']).toBe('none');
    //   });

    //   it('should update operator status tooltip content when workflow-status.service emits processState', () => {
    //     // spy on key function, create simple workflow
    //     const changeOperatorTooltipInfoSpy = vi.spyOn(jointUIService, 'changeOperatorStatusTooltipInfo');
    //     workflowActionService.addOperator(mockScanPredicateForStatus, mockPoint);
    //     const tooltipView = component.getJointPaper().findViewByModel(
    //       JointUIService.getOperatorStatusTooltipElementID(mockScanPredicateForStatus.operatorID));

    //     // workflowStatusService emits a mock status
    //     workflowStatusService['status'].next(mockStatus1 as ProcessStatus);
    //     fixture.detectChanges();
    //     // function should be called and content should be updated properly
    //     expect(component['operatorStatusTooltipDisplayEnabled']).toBeTruthy();
    //     expect(changeOperatorTooltipInfoSpy).toHaveBeenCalledTimes(1);
    //     expect(tooltipView.model.attr('#operatorCount/text'))
    //       .toBe('Output:' + (mockStatus1 as ProcessStatus).operatorStatistics[mockScanOperatorID].outputCount + ' tuples');
    //     expect(tooltipView.model.attr('#operatorSpeed/text'))
    //       .toBe('Speed:' + (mockStatus1 as ProcessStatus).operatorStatistics[mockScanOperatorID].speed + ' tuples/ms');

    //     // workflowStatusService emits another mock status
    //     workflowStatusService['status'].next(mockStatus2 as ProcessStatus);
    //     fixture.detectChanges();
    //     // function should be called again and content should be updated properly
    //     expect(changeOperatorTooltipInfoSpy).toHaveBeenCalledTimes(2);
    //     expect(tooltipView.model.attr('#operatorCount/text'))
    //       .toBe('Output:' + (mockStatus2 as ProcessStatus).operatorStatistics[mockScanOperatorID].outputCount + ' tuples');
    //     expect(tooltipView.model.attr('#operatorSpeed/text'))
    //       .toBe('Speed:' + (mockStatus2 as ProcessStatus).operatorStatistics[mockScanOperatorID].speed + ' tuples/ms');
    //   });

    //   it('should change operator state when workflow-status.service emits processState', () => {
    //     // spy on key function, create simple workflow
    //     const changeOperatorStatesSpy = vi.spyOn(jointUIService, 'changeOperatorStates');
    //     workflowActionService.addOperator(mockScanPredicateForStatus, mockPoint);
    //     const jointCellView = component.getJointPaper().findViewByModel(mockScanPredicateForStatus.operatorID);

    //     // workflowStatusService emits a mock status
    //     workflowStatusService['status'].next(mockStatus1 as ProcessStatus);
    //     fixture.detectChanges();
    //     // function should be called and state name should be updated properly
    //     expect(changeOperatorStatesSpy).toHaveBeenCalledTimes(1);
    //     expect(jointCellView.model.attr('#operatorStates')['text'])
    //     .toEqual(OperatorStates[(mockStatus1 as ProcessStatus).operatorStates[mockScanOperatorID]]);

    //     // workflowStatusService emits another mock status
    //     workflowStatusService['status'].next(mockStatus2 as ProcessStatus);
    //     fixture.detectChanges();
    //     // function should be called again and state name should be updated properly
    //     expect(changeOperatorStatesSpy).toHaveBeenCalledTimes(2);
    //     expect(jointCellView.model.attr('#operatorStates')['text'])
    //     .toEqual(OperatorStates[OperatorStates.Completed]);
    //   });

    //   it('should throw error when processState contains non-existing operatorID', () => {
    //     // workflowStatusService emits a processStatus with info for a scan operator
    //     // however there is no scan operator on the joinGraph/texeraGraph
    //     // an error should be thrown
    //     workflowStatusService['status'].next(mockStatus1 as ProcessStatus);
    //     fixture.detectChanges();
    //     expect(component['handleOperatorStatisticsUpdate']).toThrowError();
    //     expect(component['handleOperatorStatesChange']).toThrowError();
    //   });
    // });

    it("should delete the highlighted operator when user presses the backspace key", () => {
      const texeraGraph = workflowActionService.getTexeraGraph();
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

      // dispatch a keydown event on the backspace key
      const event = new KeyboardEvent("keydown", { key: "Backspace" });

      (document.activeElement as HTMLElement)?.blur();
      document.dispatchEvent(event);

      fixture.detectChanges();

      // assert the highlighted operator is deleted
      expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBeFalsy();
    });

    it("should delete the highlighted operator when user presses the delete key", () => {
      const texeraGraph = workflowActionService.getTexeraGraph();
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

      // dispatch a keydown event on the backspace key
      const event = new KeyboardEvent("keydown", { key: "Delete" });

      (document.activeElement as HTMLElement)?.blur();
      document.dispatchEvent(event);

      fixture.detectChanges();

      // assert the highlighted operator is deleted
      expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBeFalsy();
    });

    it("should delete all highlighted operators when user presses the backspace key", () => {
      const texeraGraph = workflowActionService.getTexeraGraph();
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      workflowActionService.addOperatorsAndLinks(
        [
          { op: mockScanPredicate, pos: mockPoint },
          { op: mockResultPredicate, pos: mockPoint },
        ],
        []
      );
      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID, mockResultPredicate.operatorID);

      // assert that all operators are highlighted
      expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockScanPredicate.operatorID);
      expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockResultPredicate.operatorID);

      // dispatch a keydown event on the backspace key
      const event = new KeyboardEvent("keydown", { key: "Backspace" });

      (document.activeElement as HTMLElement)?.blur();
      document.dispatchEvent(event);

      fixture.detectChanges();

      // assert that all highlighted operators are deleted
      expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBeFalsy();
      expect(texeraGraph.hasOperator(mockResultPredicate.operatorID)).toBeFalsy();
    });

    // the new method of copying and pasting would not pass this unit test, since the permisssion
    // to write access to system clipboard is needed, and in the unit test, there is no way of turning
    // on the permission as far as I am concerned
    // it(`should create and highlight a new operator with the same metadata when user
    //     copies and pastes the highlighted operator`, () => {
    //   const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
    //   const texeraGraph = workflowActionService.getTexeraGraph();

    //   workflowActionService.addOperator(mockScanPredicate, mockPoint);
    //   jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

    //   // dispatch clipboard events for copy and paste
    //   const copyEvent = new ClipboardEvent("copy");

    //   (document.activeElement as HTMLElement)?.blur();
    //   document.dispatchEvent(copyEvent);
    //   const pasteEvent = new ClipboardEvent("paste");

    //   (document.activeElement as HTMLElement)?.blur();
    //   document.dispatchEvent(pasteEvent);

    //   // the pasted operator should be highlighted
    //   const pastedOperatorID = jointGraphWrapper.getCurrentHighlightedOperatorIDs()[0];
    //   expect(pastedOperatorID).toBeDefined();

    //   // get the pasted operator
    //   let pastedOperator = null;
    //   if (pastedOperatorID) {
    //     pastedOperator = texeraGraph.getOperator(pastedOperatorID);
    //   }
    //   expect(pastedOperator).toBeDefined();

    //   // two operators should have same metadata
    //   expect(pastedOperatorID).not.toEqual(mockScanPredicate.operatorID);
    //   if (pastedOperator) {
    //     expect(pastedOperator.operatorType).toEqual(mockScanPredicate.operatorType);
    //     expect(pastedOperator.operatorProperties).toEqual(mockScanPredicate.operatorProperties);
    //     expect(pastedOperator.inputPorts).toEqual(mockScanPredicate.inputPorts);
    //     expect(pastedOperator.outputPorts).toEqual(mockScanPredicate.outputPorts);
    //     expect(pastedOperator.showAdvanced).toEqual(mockScanPredicate.showAdvanced);
    //   }
    // });

    // the new method won't pass the unit test because as far as I am concerned, there's no way
    // to grant the permission to the system clipboard in the Karma framework
    // it(`should delete the highlighted operator, create and highlight a new operator with the same metadata
    //     when user cuts and pastes the highlighted operator`, () => {
    //   const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
    //   const texeraGraph = workflowActionService.getTexeraGraph();

    //   workflowActionService.addOperator(mockScanPredicate, mockPoint);
    //   jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

    //   // dispatch clipboard events for cut and paste
    //   const cutEvent = new ClipboardEvent("cut");

    //   (document.activeElement as HTMLElement)?.blur();
    //   document.dispatchEvent(cutEvent);
    //   const pasteEvent = new ClipboardEvent("paste");

    //   (document.activeElement as HTMLElement)?.blur();
    //   document.dispatchEvent(pasteEvent);

    //   // the copied operator should be deleted
    //   expect(() => {
    //     texeraGraph.getOperator(mockScanPredicate.operatorID);
    //   }).toThrowError(new RegExp("does not exist"));

    //   // the pasted operator should be highlighted
    //   const pastedOperatorID = jointGraphWrapper.getCurrentHighlightedOperatorIDs()[0];
    //   expect(pastedOperatorID).toBeDefined();

    //   // get the pasted operator
    //   let pastedOperator = null;
    //   if (pastedOperatorID) {
    //     pastedOperator = texeraGraph.getOperator(pastedOperatorID);
    //   }
    //   expect(pastedOperator).toBeDefined();

    //   // two operators should have same metadata
    //   expect(pastedOperatorID).not.toEqual(mockScanPredicate.operatorID);
    //   if (pastedOperator) {
    //     expect(pastedOperator.operatorType).toEqual(mockScanPredicate.operatorType);
    //     expect(pastedOperator.operatorProperties).toEqual(mockScanPredicate.operatorProperties);
    //     expect(pastedOperator.inputPorts).toEqual(mockScanPredicate.inputPorts);
    //     expect(pastedOperator.outputPorts).toEqual(mockScanPredicate.outputPorts);
    //     expect(pastedOperator.showAdvanced).toEqual(mockScanPredicate.showAdvanced);
    //   }
    // });

    // TODO: this test is unstable, find out why and fix it
    // same reason as above: can't grant clipboard access when pasting during unit-testing
    // it("should place the pasted operator in a non-overlapping position", () => {
    //   const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    //   workflowActionService.addOperator(mockScanPredicate, mockPoint);
    //   jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

    //   // dispatch clipboard events for copy and paste
    //   const copyEvent = new ClipboardEvent("copy");

    //   (document.activeElement as HTMLElement)?.blur();
    //   document.dispatchEvent(copyEvent);
    //   const pasteEvent = new ClipboardEvent("paste");

    //   (document.activeElement as HTMLElement)?.blur();
    //   document.dispatchEvent(pasteEvent);
    //   fixture.detectChanges();
    //   // get the pasted operator
    //   const pastedOperatorID = jointGraphWrapper.getCurrentHighlightedOperatorIDs()[0];
    //   if (pastedOperatorID) {
    //     const pastedOperatorPosition = jointGraphWrapper.getElementPosition(pastedOperatorID);
    //     expect(pastedOperatorPosition).not.toEqual(mockPoint);
    //   }
    // });

    it("should highlight all operators when user presses command + A", () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);

      // unhighlight operators in case of automatic highlight
      jointGraphWrapper.unhighlightOperators(mockScanPredicate.operatorID, mockResultPredicate.operatorID);

      // dispatch a keydown event on the command + A key comb
      const event = new KeyboardEvent("keydown", { key: "a", metaKey: true });

      (document.activeElement as HTMLElement)?.blur();
      document.dispatchEvent(event);

      fixture.detectChanges();

      // assert that all operators are highlighted
      expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockScanPredicate.operatorID);
      expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockResultPredicate.operatorID);
    });

    //undo
    it("should undo action when user presses command + Z or control + Z", () => {
      vi.spyOn(workflowVersionService, "getDisplayParticularVersionStream").mockReturnValue(of(false));
      vi.spyOn(undoRedoService, "canUndo").mockReturnValue(true);
      let undoSpy = vi.spyOn(undoRedoService, "undoAction");
      fixture.detectChanges();
      const commandZEvent = new KeyboardEvent("keydown", { key: "Z", metaKey: true, shiftKey: false });
      (document.activeElement as HTMLElement)?.blur();
      document.dispatchEvent(commandZEvent);
      fixture.detectChanges();
      expect(undoSpy).toHaveBeenCalledTimes(1);

      const controlZEvent = new KeyboardEvent("keydown", { key: "Z", ctrlKey: true, shiftKey: false });
      (document.activeElement as HTMLElement)?.blur();
      document.dispatchEvent(controlZEvent);
      fixture.detectChanges();
      expect(undoSpy).toHaveBeenCalledTimes(2);
    });

    //redo
    it("should redo action when user presses command/control + Y or command/control + shift + Z", () => {
      vi.spyOn(workflowVersionService, "getDisplayParticularVersionStream").mockReturnValue(of(false));
      vi.spyOn(undoRedoService, "canRedo").mockReturnValue(true);
      let redoSpy = vi.spyOn(undoRedoService, "redoAction");
      fixture.detectChanges();
      const commandYEvent = new KeyboardEvent("keydown", { key: "y", metaKey: true, shiftKey: false });
      (document.activeElement as HTMLElement)?.blur();
      document.dispatchEvent(commandYEvent);
      fixture.detectChanges();
      expect(redoSpy).toHaveBeenCalledTimes(1);

      const controlYEvent = new KeyboardEvent("keydown", { key: "y", ctrlKey: true, shiftKey: false });
      (document.activeElement as HTMLElement)?.blur();
      document.dispatchEvent(controlYEvent);
      fixture.detectChanges();
      expect(redoSpy).toHaveBeenCalledTimes(2);

      const commandShitZEvent = new KeyboardEvent("keydown", { key: "z", metaKey: true, shiftKey: true });
      (document.activeElement as HTMLElement)?.blur();
      document.dispatchEvent(commandShitZEvent);
      fixture.detectChanges();
      expect(redoSpy).toHaveBeenCalledTimes(3);

      const controlShitZEvent = new KeyboardEvent("keydown", { key: "z", ctrlKey: true, shiftKey: true });
      (document.activeElement as HTMLElement)?.blur();
      document.dispatchEvent(controlShitZEvent);
      fixture.detectChanges();
      expect(redoSpy).toHaveBeenCalledTimes(4);
    });

    /**
     * Regression coverage for the bug where the operator border resets to the
     * default (gray) when the user navigates away from and back to a workflow
     * that has already finished executing. Both the operator-add stream and
     * the validation stream route their final border decision through
     * applyOperatorBorder, which encodes the priority: invalid > cached
     * execution state > default valid. These tests assert the operator's
     * actual final rect.body/stroke on the paper, so they pin down the visible
     * outcome rather than the internal helper calls.
     */
    describe("operator border restoration after navigation", () => {
      let workflowStatusService: WorkflowStatusService;
      const cachedStatus = (operatorState: OperatorState) => ({
        [mockScanPredicate.operatorID]: {
          operatorState,
          aggregatedInputRowCount: 0,
          inputPortMetrics: {},
          aggregatedOutputRowCount: 0,
          outputPortMetrics: {},
        },
      });
      const cachedCompleted = cachedStatus(OperatorState.Completed);
      const getStroke = (operatorID: string): string =>
        component.paper.getModelById(operatorID).attr("rect.body/stroke") as string;

      beforeEach(() => {
        workflowStatusService = TestBed.inject(WorkflowStatusService);
      });

      it("paints the execution-state stroke (green) for a valid operator with a cached Completed status", () => {
        vi.spyOn(workflowStatusService, "getCurrentStatus").mockReturnValue(cachedCompleted);
        vi.spyOn(validationWorkflowService, "validateOperator").mockReturnValue({ isValid: true });

        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        fixture.detectChanges();

        expect(getStroke(mockScanPredicate.operatorID)).toBe("green");
      });

      it("paints the execution-state stroke (orange) for a valid operator with a cached Running status", () => {
        // Navigation-return with a mid-run operator: the border must be restored
        // to the running color, not the default (see #3614).
        vi.spyOn(workflowStatusService, "getCurrentStatus").mockReturnValue(cachedStatus(OperatorState.Running));
        vi.spyOn(validationWorkflowService, "validateOperator").mockReturnValue({ isValid: true });

        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        fixture.detectChanges();

        expect(getStroke(mockScanPredicate.operatorID)).toBe("orange");
      });

      it("falls back to the default valid stroke (#CFCFCF) when no cached status exists", () => {
        vi.spyOn(workflowStatusService, "getCurrentStatus").mockReturnValue({});
        vi.spyOn(validationWorkflowService, "validateOperator").mockReturnValue({ isValid: true });

        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        fixture.detectChanges();

        expect(getStroke(mockScanPredicate.operatorID)).toBe("#CFCFCF");
      });

      it("paints the invalid stroke (red) for an invalid operator with no cached status", () => {
        vi.spyOn(workflowStatusService, "getCurrentStatus").mockReturnValue({});
        vi.spyOn(validationWorkflowService, "validateOperator").mockReturnValue({ isValid: false, messages: {} });

        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        fixture.detectChanges();

        expect(getStroke(mockScanPredicate.operatorID)).toBe("red");
      });

      it("prioritizes invalid (red) over cached Completed status", () => {
        // Regression case: operator is both invalid AND has a cached Completed
        // status. applyOperatorBorder must pick red regardless of the order in
        // which the operator-add and validation streams fire.
        vi.spyOn(workflowStatusService, "getCurrentStatus").mockReturnValue(cachedCompleted);
        vi.spyOn(validationWorkflowService, "validateOperator").mockReturnValue({ isValid: false, messages: {} });

        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        fixture.detectChanges();

        expect(getStroke(mockScanPredicate.operatorID)).toBe("red");
      });

      it("relies solely on the passed-in Validation (never recomputes inside the helper)", () => {
        // Let the validation chain settle from the operator-add so the spy
        // below is created after those calls and starts with a clean slate.
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        fixture.detectChanges();

        const validateSpy = vi.spyOn(validationWorkflowService, "validateOperator");

        // The helper takes the Validation as a required argument and must use it
        // directly — it has no fallback path that calls validateOperator itself.
        (component as any).applyOperatorBorder(mockScanPredicate.operatorID, { isValid: true });

        expect(validateSpy).not.toHaveBeenCalled();
      });

      it("honors the passed-in Validation result (paints red when it is invalid)", () => {
        // Proves the passed-in value actually drives the border: an invalid
        // result must paint red.
        vi.spyOn(workflowStatusService, "getCurrentStatus").mockReturnValue({});
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        fixture.detectChanges();

        (component as any).applyOperatorBorder(mockScanPredicate.operatorID, { isValid: false, messages: {} });

        expect(getStroke(mockScanPredicate.operatorID)).toBe("red");
      });

      it("always supplies a Validation to applyOperatorBorder when an operator is added", () => {
        // Both subscribers (operator-add and the validation stream) call
        // applyOperatorBorder on add with identical args, so this asserts the
        // required-parameter contract holds through the add flow — every call
        // carries a Validation, never undefined — rather than isolating the
        // operator-add caller specifically.
        vi.spyOn(workflowStatusService, "getCurrentStatus").mockReturnValue({});
        vi.spyOn(validationWorkflowService, "validateOperator").mockReturnValue({ isValid: true });
        const applyBorderSpy = vi.spyOn(component as any, "applyOperatorBorder");

        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        fixture.detectChanges();

        expect(applyBorderSpy).toHaveBeenCalledWith(mockScanPredicate.operatorID, { isValid: true });
      });
    });

    /**
     * Covers the JointJS paper event handlers wired in ngAfterViewInit. Each test
     * drives the real paper by triggering the callback event the handler subscribes
     * to (element:delete, element:*-port, element:magnet:pointerclick, cell:pointerdown,
     * cell:pointerdblclick, link:mouseenter/leave, center-event) and asserts the
     * resulting graph / router / paper state. Mouse-wheel pan/zoom and clipboard
     * copy/cut/paste are intentionally excluded — those need real-browser DOM.
     */
    describe("joint paper event handlers", () => {
      // A predicate whose type exists in the stub metadata but with dynamic ports
      // enabled, so the add/remove-port handlers' addPort calls are accepted.
      const dynamicPortPredicate: OperatorPredicate = {
        ...mockMultiInputOutputPredicate,
        operatorID: "dynamic-port-op",
        inputPorts: [{ portID: "input-0" }],
        outputPorts: [{ portID: "output-0" }],
        dynamicInputPorts: true,
        dynamicOutputPorts: true,
      };

      it("deletes the operator when its element:delete button fires", () => {
        const texeraGraph = workflowActionService.getTexeraGraph();
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        const view = component.paper.findViewByModel(mockScanPredicate.operatorID);

        // The `.delete-button` fires `element:delete` (cell view, DOM event, x, y);
        // fromJointPaperEvent only emits the arg array when several args are passed.
        (component.paper as any).trigger("element:delete", view, new Event("click"), 0, 0);

        expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBe(false);
      });

      it("adds then removes an input port on the matching element port events", () => {
        const texeraGraph = workflowActionService.getTexeraGraph();
        const opID = dynamicPortPredicate.operatorID;
        workflowActionService.addOperator(dynamicPortPredicate, mockPoint);
        const view = component.paper.findViewByModel(opID);
        expect(texeraGraph.getOperator(opID).inputPorts.length).toEqual(1);

        // The port buttons fire `element:*-port` (cell view, DOM event, x, y);
        // fromJointPaperEvent only emits the arg array when several args are passed.
        (component.paper as any).trigger("element:add-input-port", view, new Event("click"), 0, 0);
        expect(texeraGraph.getOperator(opID).inputPorts.length).toEqual(2);

        (component.paper as any).trigger("element:remove-input-port", view, new Event("click"), 0, 0);
        expect(texeraGraph.getOperator(opID).inputPorts.length).toEqual(1);
      });

      it("adds then removes an output port on the matching element port events", () => {
        const texeraGraph = workflowActionService.getTexeraGraph();
        const opID = dynamicPortPredicate.operatorID;
        workflowActionService.addOperator(dynamicPortPredicate, mockPoint);
        const view = component.paper.findViewByModel(opID);
        expect(texeraGraph.getOperator(opID).outputPorts.length).toEqual(1);

        // The port buttons fire `element:*-port` (cell view, DOM event, x, y);
        // fromJointPaperEvent only emits the arg array when several args are passed.
        (component.paper as any).trigger("element:add-output-port", view, new Event("click"), 0, 0);
        expect(texeraGraph.getOperator(opID).outputPorts.length).toEqual(2);

        (component.paper as any).trigger("element:remove-output-port", view, new Event("click"), 0, 0);
        expect(texeraGraph.getOperator(opID).outputPorts.length).toEqual(1);
      });

      it("highlights the clicked port when a port magnet is clicked", () => {
        const wrapper = workflowActionService.getJointGraphWrapper();
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        const view = component.paper.findViewByModel(mockScanPredicate.operatorID);
        const magnet = { getAttribute: (name: string) => (name === "port" ? "output-0" : null) };

        (component.paper as any).trigger("element:magnet:pointerclick", view, { shiftKey: false }, magnet);

        expect(wrapper.getCurrentHighlightedPortIDs()).toContainEqual({
          operatorID: mockScanPredicate.operatorID,
          portID: "output-0",
        });
      });

      it("supports shift-click multiselect, toggle-off, and blank-area unhighlight", () => {
        const wrapper = workflowActionService.getJointGraphWrapper();
        workflowActionService.addOperatorsAndLinks(
          [
            { op: mockScanPredicate, pos: mockPoint },
            { op: mockResultPredicate, pos: mockPoint },
          ],
          []
        );
        wrapper.unhighlightOperators(...wrapper.getCurrentHighlightedOperatorIDs());
        const viewA = component.paper.findViewByModel(mockScanPredicate.operatorID);
        const viewB = component.paper.findViewByModel(mockResultPredicate.operatorID);

        // plain click highlights only operator A
        (component.paper as any).trigger("cell:pointerdown", viewA, { shiftKey: false });
        expect(wrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockScanPredicate.operatorID]);

        // shift-click adds operator B to the selection
        (component.paper as any).trigger("cell:pointerdown", viewB, { shiftKey: true });
        expect([...wrapper.getCurrentHighlightedOperatorIDs()].sort()).toEqual(
          [mockScanPredicate.operatorID, mockResultPredicate.operatorID].sort()
        );

        // shift-clicking an already-highlighted operator toggles it off
        (component.paper as any).trigger("cell:pointerdown", viewB, { shiftKey: true });
        expect(wrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockScanPredicate.operatorID]);

        // clicking the blank canvas unhighlights everything
        (component.paper as any).trigger("blank:pointerdown");
        expect(wrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);
        // blank:pointerdown starts the paper-pan gesture, which listens on document.mousemove
        // until a mouseup; fire mouseup so that listener does not leak into later tests.
        document.dispatchEvent(new MouseEvent("mouseup"));
      });

      it("opens the comment box modal on a comment box double-click", () => {
        const nzModalService = TestBed.inject(NzModalService);
        const createSpy = vi.spyOn(nzModalService, "create").mockReturnValue({ afterClose: of(undefined) } as any);
        workflowActionService.addCommentBox(mockCommentBox);
        const view = component.paper.findViewByModel(mockCommentBox.commentBoxID);

        (component.paper as any).trigger("cell:pointerdblclick", view, { shiftKey: false });

        expect(createSpy).toHaveBeenCalledTimes(1);
        expect(createSpy.mock.calls[0][0]).toEqual(expect.objectContaining({ nzTitle: "Comments" }));
      });

      it("opens the comment box modal when the URL fragment matches an added comment box", () => {
        const nzModalService = TestBed.inject(NzModalService);
        const createSpy = vi.spyOn(nzModalService, "create").mockReturnValue({ afterClose: of(undefined) } as any);
        const route = TestBed.inject(ActivatedRoute);
        (route.snapshot as any).fragment = mockCommentBox.commentBoxID;

        workflowActionService.addCommentBox(mockCommentBox);

        expect(createSpy).toHaveBeenCalledTimes(1);
      });

      it("attaches link tools when the cursor enters a link", () => {
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanResultLink);
        const linkView = component.paper.findViewByModel(mockScanResultLink.linkID);

        // `link:mouseenter` fires (link view, DOM event, x, y); fromJointPaperEvent
        // only emits the arg array when several args are passed.
        (component.paper as any).trigger("link:mouseenter", linkView, new Event("mouseenter"), 0, 0);

        expect((linkView as any).hasTools()).toBe(true);
      });

      it("hides link tools when the cursor leaves a link", () => {
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanResultLink);
        const linkView = component.paper.findViewByModel(mockScanResultLink.linkID);
        // Enter first so tools are actually attached, then leave. Both events fire with the
        // full (link view, DOM event, x, y) payload, since fromJointPaperEvent only emits the
        // arg array when several args are passed.
        (component.paper as any).trigger("link:mouseenter", linkView, new Event("mouseenter"), 0, 0);
        expect((linkView as any).hasTools()).toBe(true);

        // On leave the handler hides (does not remove) the tools and marks the remove button
        // hidden; spy so the assertion reflects the handler running, not the default markup.
        const hideToolsSpy = vi.spyOn(linkView as any, "hideTools");
        (component.paper as any).trigger("link:mouseleave", linkView, new Event("mouseleave"), 0, 0);

        expect(hideToolsSpy).toHaveBeenCalled();
        expect(linkView.model.attr(".tool-remove/display")).toEqual("none");
      });

      it("writes the highlighted operator to the URL fragment and clears it on unhighlight", () => {
        const router = TestBed.inject(Router);
        const navigateSpy = vi.spyOn(router, "navigate").mockResolvedValue(true);
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        const wrapper = workflowActionService.getJointGraphWrapper();
        wrapper.unhighlightOperators(...wrapper.getCurrentHighlightedOperatorIDs());

        // highlighting exactly one element sets the fragment to that element's ID
        navigateSpy.mockClear();
        wrapper.highlightOperators(mockScanPredicate.operatorID);
        expect(navigateSpy).toHaveBeenLastCalledWith(
          [],
          expect.objectContaining({ fragment: mockScanPredicate.operatorID })
        );

        // dropping back to zero highlighted elements clears the fragment
        navigateSpy.mockClear();
        wrapper.unhighlightOperators(mockScanPredicate.operatorID);
        expect(navigateSpy).toHaveBeenLastCalledWith([], expect.objectContaining({ fragment: undefined }));
      });

      it("translates the paper toward the computed center on a center event", () => {
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        const translateSpy = vi.spyOn(component.paper, "translate");

        (workflowActionService.getTexeraGraph() as any).triggerCenterEvent();

        const center = workflowActionService.getCenterPoint();
        const editor = (component as any).editor as HTMLElement;
        const offsetX = editor.offsetWidth * 0.15;
        const offsetY = editor.offsetHeight * 0.15;
        expect(translateSpy).toHaveBeenCalledWith(-(center.x - offsetX), -(center.y - offsetY));
      });

      it("exposes seeded agent operator result summaries and falls back to undefined", () => {
        const agentService = TestBed.inject(AgentService);
        const summaries = new Map<string, OperatorResultSummary>();
        summaries.set("op-a", {
          state: "Completed",
          inputTuples: 1,
          outputTuples: 2,
          sampleRecords: [{ colA: "x" }],
          resultStatistics: { rowCount: "2" },
        });
        (agentService as any).operatorResultSummariesSubject.next(summaries);

        expect(component.getOperatorSampleRecords("op-a")).toEqual([{ colA: "x" }]);
        expect(component.getOperatorResultStatistics("op-a")).toEqual({ rowCount: "2" });
        expect(component.getOperatorSampleRecords("missing")).toBeUndefined();
        expect(component.getOperatorResultStatistics("missing")).toBeUndefined();
      });

      it("detects visualization operators from the __is_visualization__ marker", () => {
        const agentService = TestBed.inject(AgentService);
        const summaries = new Map<string, OperatorResultSummary>();
        summaries.set("viz-op", {
          state: "Completed",
          inputTuples: 0,
          outputTuples: 1,
          sampleRecords: [{ __is_visualization__: true }],
        });
        summaries.set("plain-op", {
          state: "Completed",
          inputTuples: 0,
          outputTuples: 1,
          sampleRecords: [{ colA: "x" }],
        });
        (agentService as any).operatorResultSummariesSubject.next(summaries);

        expect(component.isOperatorVisualization("viz-op")).toBe(true);
        expect(component.isOperatorVisualization("plain-op")).toBe(false);
        expect(component.isOperatorVisualization("missing")).toBe(false);
      });

      it("closes the chat popover", () => {
        component.chatPopoverOperator = { operatorId: "x", displayName: "X", position: { x: 1, y: 2 } };

        component.closeChatPopover();

        expect(component.chatPopoverOperator).toBeNull();
      });

      it("clears agent action labels from every operator", () => {
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        const element = component.paper.getModelById(mockScanPredicate.operatorID);
        jointUIService.showAgentActionLabel(component.paper, mockScanPredicate.operatorID, "viewed", "TestAgent");
        expect(element.attr(`.${operatorAgentActionProgressClass}/visibility`)).toEqual("visible");

        (component as any).clearAllAgentActionLabels();

        expect(element.attr(`.${operatorAgentActionProgressClass}/visibility`)).toEqual("hidden");
      });
    });

    /**
     * The editor's non-rendering logic: connection validation, the clipboard
     * handlers, and the delete / select-all paths that the keyboard tests above
     * never reach (links and comment boxes). None of these need pointer geometry,
     * so they run under jsdom.
     */
    describe("connection validation, clipboard, and delete/select-all", () => {
      /** A port magnet as JointJS hands it to validateConnection. */
      function magnet(attributes: Record<string, string>): SVGElement {
        const element = document.createElementNS("http://www.w3.org/2000/svg", "rect");
        Object.entries(attributes).forEach(([name, value]) => element.setAttribute(name, value));
        return element as unknown as SVGElement;
      }

      function validateJointConnection(
        sourceOperatorID: string,
        sourceMagnet: SVGElement | undefined,
        targetOperatorID: string,
        targetMagnet: SVGElement | undefined
      ): boolean {
        const cellView = (id: string) => ({ model: { id } }) as unknown as joint.dia.CellView;
        return component["validateJointOperatorConnection"](
          cellView(sourceOperatorID),
          sourceMagnet,
          cellView(targetOperatorID),
          targetMagnet,
          "target" as joint.dia.LinkEnd,
          {} as joint.dia.LinkView
        );
      }

      /** These handlers only run while the body has focus (i.e. no input is being edited). */
      function dispatchOnBody(event: Event): void {
        (document.activeElement as HTMLElement)?.blur();
        document.dispatchEvent(event);
        fixture.detectChanges();
      }

      describe("validateJointOperatorConnection", () => {
        beforeEach(() => {
          workflowActionService.addOperator(mockScanPredicate, mockPoint);
          workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
        });

        it("rejects a link drawn out of an input port", () => {
          expect(
            validateJointConnection(
              mockScanPredicate.operatorID,
              magnet({ "port-group": "in", port: "input-0" }),
              mockSentimentPredicate.operatorID,
              magnet({ "port-group": "in", port: "input-0" })
            )
          ).toBe(false);
        });

        it("rejects a link dropped onto an output port", () => {
          expect(
            validateJointConnection(
              mockScanPredicate.operatorID,
              magnet({ "port-group": "out", port: "output-0" }),
              mockSentimentPredicate.operatorID,
              magnet({ "port-group": "out", port: "output-0" })
            )
          ).toBe(false);
        });

        it("delegates an output-to-input pair to the operator-level validation", () => {
          const outMagnet = () => magnet({ "port-group": "out", port: "output-0" });
          const inMagnet = () => magnet({ "port-group": "in", port: "input-0" });

          expect(
            validateJointConnection(
              mockScanPredicate.operatorID,
              outMagnet(),
              mockSentimentPredicate.operatorID,
              inMagnet()
            )
          ).toBe(true);

          // Once the link exists the same pair is rejected — proving the call is
          // really delegated to validateOperatorConnection rather than hardcoded.
          workflowActionService.addLink(mockScanSentimentLink);
          expect(
            validateJointConnection(
              mockScanPredicate.operatorID,
              outMagnet(),
              mockSentimentPredicate.operatorID,
              inMagnet()
            )
          ).toBe(false);
        });
      });

      describe("validateOperatorConnection guards", () => {
        beforeEach(() => {
          workflowActionService.addOperator(mockScanPredicate, mockPoint);
          workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
        });

        it("rejects a connection from an operator to itself", () => {
          expect(
            component["validateOperatorConnection"](
              mockScanPredicate.operatorID,
              "output-0",
              mockScanPredicate.operatorID,
              "input-0"
            )
          ).toBe(false);
        });

        it("rejects a connection that is missing a port on either end", () => {
          expect(
            component["validateOperatorConnection"](
              mockScanPredicate.operatorID,
              undefined,
              mockSentimentPredicate.operatorID,
              "input-0"
            )
          ).toBe(false);
          expect(
            component["validateOperatorConnection"](
              mockScanPredicate.operatorID,
              "output-0",
              mockSentimentPredicate.operatorID,
              null
            )
          ).toBe(false);
        });

        it("rejects a connection whose endpoint is not an operator", () => {
          expect(
            component["validateOperatorConnection"](
              "not-an-operator",
              "output-0",
              mockSentimentPredicate.operatorID,
              "input-0"
            )
          ).toBe(false);
          expect(
            component["validateOperatorConnection"](
              mockScanPredicate.operatorID,
              "output-0",
              "not-an-operator",
              "input-0"
            )
          ).toBe(false);
        });
      });

      describe("delete", () => {
        it("deletes highlighted links and comment boxes, not just operators", () => {
          const texeraGraph = workflowActionService.getTexeraGraph();
          const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
          workflowActionService.addOperatorsAndLinks(
            [
              { op: mockScanPredicate, pos: mockPoint },
              { op: mockResultPredicate, pos: mockPoint },
            ],
            [mockScanResultLink]
          );
          workflowActionService.addCommentBox(mockCommentBox);
          // multi-select keeps both selections alive: highlighting with it off
          // unhighlights everything else first, which would silently drop the link.
          jointGraphWrapper.setMultiSelectMode(true);
          jointGraphWrapper.unhighlightOperators(...jointGraphWrapper.getCurrentHighlightedOperatorIDs());
          jointGraphWrapper.highlightLinks(mockScanResultLink.linkID);
          jointGraphWrapper.highlightCommentBoxes(mockCommentBox.commentBoxID);

          // guard the setup so the assertions below cannot pass vacuously
          expect(texeraGraph.hasLinkWithID(mockScanResultLink.linkID)).toBe(true);
          expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);
          expect(jointGraphWrapper.getCurrentHighlightedLinkIDs()).toContain(mockScanResultLink.linkID);
          expect(jointGraphWrapper.getCurrentHighlightedCommentBoxIDs()).toContain(mockCommentBox.commentBoxID);

          dispatchOnBody(new KeyboardEvent("keydown", { key: "Delete" }));

          expect(texeraGraph.hasLinkWithID(mockScanResultLink.linkID)).toBe(false);
          expect(texeraGraph.hasCommentBox(mockCommentBox.commentBoxID)).toBe(false);
          // the operators were not highlighted, so they survive
          expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBe(true);
        });
      });

      describe("select all", () => {
        it("highlights links and comment boxes as well as operators", () => {
          const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
          workflowActionService.addOperatorsAndLinks(
            [
              { op: mockScanPredicate, pos: mockPoint },
              { op: mockResultPredicate, pos: mockPoint },
            ],
            [mockScanResultLink]
          );
          workflowActionService.addCommentBox(mockCommentBox);

          dispatchOnBody(new KeyboardEvent("keydown", { key: "a", metaKey: true }));

          expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockScanPredicate.operatorID);
          expect(jointGraphWrapper.getCurrentHighlightedLinkIDs()).toContain(mockScanResultLink.linkID);
          expect(jointGraphWrapper.getCurrentHighlightedCommentBoxIDs()).toContain(mockCommentBox.commentBoxID);
        });
      });

      describe("disabled-operator stream", () => {
        it("repaints an operator when it is disabled and again when it is re-enabled", () => {
          workflowActionService.addOperator(mockScanPredicate, mockPoint);
          const changeSpy = vi.spyOn(jointUIService, "changeOperatorDisableStatus");
          try {
            workflowActionService.disableOperators([mockScanPredicate.operatorID]);
            expect(changeSpy).toHaveBeenCalledTimes(1);

            workflowActionService.enableOperators([mockScanPredicate.operatorID]);
            expect(changeSpy).toHaveBeenCalledTimes(2);
          } finally {
            changeSpy.mockRestore();
          }
        });
      });

      describe("clipboard", () => {
        let operatorMenu: OperatorMenuService;

        beforeEach(() => {
          operatorMenu = TestBed.inject(OperatorMenuService);
          workflowActionService.addOperator(mockScanPredicate, mockPoint);
          // the copy/cut handlers read the menu's latest highlighted-element snapshot
          workflowActionService.getJointGraphWrapper().highlightOperators(mockScanPredicate.operatorID);
        });

        it("caches the highlighted elements on copy", () => {
          const saveSpy = vi.spyOn(operatorMenu, "saveHighlightedElements").mockImplementation(() => {});
          try {
            dispatchOnBody(new Event("copy"));
            expect(saveSpy).toHaveBeenCalledTimes(1);
            expect(workflowActionService.getTexeraGraph().hasOperator(mockScanPredicate.operatorID)).toBe(true);
          } finally {
            saveSpy.mockRestore();
          }
        });

        it("caches and then deletes the highlighted elements on cut", () => {
          const saveSpy = vi.spyOn(operatorMenu, "saveHighlightedElements").mockImplementation(() => {});
          try {
            dispatchOnBody(new Event("cut"));
            expect(saveSpy).toHaveBeenCalledTimes(1);
            expect(workflowActionService.getTexeraGraph().hasOperator(mockScanPredicate.operatorID)).toBe(false);
          } finally {
            saveSpy.mockRestore();
          }
        });

        it("pastes the cached elements on paste", () => {
          const pasteSpy = vi.spyOn(operatorMenu, "performPasteOperation").mockImplementation(() => {});
          try {
            dispatchOnBody(new Event("paste"));
            expect(pasteSpy).toHaveBeenCalledTimes(1);
          } finally {
            pasteSpy.mockRestore();
          }
        });
      });
    });
  });
});
/**
 * Link breakpoints are a shipped feature — `gui.conf` sets `link-breakpoint-enabled = true` — but
 * `MockGuiConfigService` defaults it to false, so `handleLinkBreakpoint` and the four handlers it
 * installs have never run in any test. Turning the flag on before the first change-detection cycle
 * (which is when ngAfterViewInit wires them) reaches the whole block.
 */
describe("WorkflowEditorComponent link breakpoints", () => {
  let fixture: ComponentFixture<WorkflowEditorComponent>;
  let component: WorkflowEditorComponent;
  let workflowActionService: WorkflowActionService;

  beforeEach(async () => {
    TestBed.resetTestingModule();
    await TestBed.configureTestingModule({
      imports: [
        RouterTestingModule,
        HttpClientTestingModule,
        NzModalModule,
        NzDropDownModule,
        WorkflowEditorComponent,
        ContextMenuComponent,
      ],
      providers: [
        JointUIService,
        WorkflowUtilService,
        UndoRedoService,
        DragDropService,
        ValidationWorkflowService,
        WorkflowActionService,
        NzContextMenuService,
        Overlay,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        WorkflowStatusService,
        ExecuteWorkflowService,
        ...commonTestProviders,
      ],
    }).compileComponents();

    // Both halves of the guard at workflow-editor.component.ts:202 must be satisfied, and both must
    // be set before the first detectChanges: ngAfterViewInit reads them once, when it decides
    // whether to install the breakpoint handlers at all.
    (TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService).setConfig({
      linkBreakpointEnabled: true,
    });
    workflowActionService = TestBed.inject(WorkflowActionService);
    workflowActionService.setHighlightingEnabled(true);

    fixture = TestBed.createComponent(WorkflowEditorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  /** Adds scan -> result and returns the link plus its rendered view. */
  function withLink() {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    workflowActionService.addLink(mockScanResultLink);
    const model = component.paper.getModelById(mockScanResultLink.linkID);
    return { linkID: mockScanResultLink.linkID, model, view: model.findView(component.paper) as any };
  }

  it("attaches a breakpoint tool to every link, hidden until it is wanted", () => {
    // The tool is what the user clicks to set a breakpoint; without it the feature has no entry
    // point, and leaving it visible would put a button on every link on the canvas.
    const { view } = withLink();

    expect(view.hasTools()).toBe(true);
    // The tool exists but stays out of sight until the cursor hovers the link or a breakpoint is
    // set; a visible one would put a button on every link on the canvas.
    expect(view._toolsView.tools[0].isVisible()).toBe(false);
  });

  it("highlights the link whose breakpoint button was clicked", () => {
    const { linkID, view } = withLink();

    (component.paper as any).trigger("tool:breakpoint", view, { shiftKey: false });

    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs()).toEqual([linkID]);
  });

  it("unhighlights an already-highlighted link on a shift-click", () => {
    // Shift is the multi-select modifier, so a second shift-click on the same link is how the user
    // removes it from the selection rather than re-adding it.
    const { linkID, view } = withLink();
    (component.paper as any).trigger("tool:breakpoint", view, { shiftKey: true });
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs()).toEqual([linkID]);

    (component.paper as any).trigger("tool:breakpoint", view, { shiftKey: true });

    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs()).toEqual([]);
  });

  it("carries the shift modifier into multi-select mode", () => {
    // Routed through the unhighlight branch deliberately. On the highlight branch
    // `WorkflowActionService.highlightLinks` sets multi-select itself, so the handler's own
    // `setMultiSelectMode` could be deleted and the assertion would still pass; `unhighlightLinks`
    // does not touch it, leaving this handler as the only writer.
    const { view } = withLink();
    // multiSelect is private and has no getter; read it directly rather than adding an accessor.
    const wrapper = workflowActionService.getJointGraphWrapper() as any;

    (component.paper as any).trigger("tool:breakpoint", view, { shiftKey: false });
    expect(wrapper.multiSelect).toBe(false);
    (component.paper as any).trigger("tool:breakpoint", view, { shiftKey: true });

    expect(wrapper.multiSelect).toBe(true);
  });

  it("shows and hides the tool as the breakpoint streams ask", () => {
    // These two streams are how a link that already has a breakpoint keeps its marker visible after
    // the cursor leaves it.
    const { linkID, view } = withLink();
    const wrapper = workflowActionService.getJointGraphWrapper();
    const show = vi.spyOn(view, "showTools");
    const hide = vi.spyOn(view, "hideTools");

    (wrapper as any).jointLinkBreakpointShowStream.next({ linkID });
    (wrapper as any).jointLinkBreakpointHideStream.next({ linkID });

    expect(show).toHaveBeenCalledTimes(1);
    expect(hide).toHaveBeenCalledTimes(1);
    // Order matters, otherwise a handler pair wired to each other's stream passes: both would
    // still be called once, just for the opposite reason.
    expect(show.mock.invocationCallOrder[0]).toBeLessThan(hide.mock.invocationCallOrder[0]);
  });
});

/**
 * The rest of the wiring installed by `ngAfterViewInit`: the JointJS paper option
 * callbacks, the status / execution / region streams, the pointer, selection and
 * port handlers, the agent hover labels, and the operator chat popover.
 *
 * Link breakpoints are switched on for the whole block because `handleLinkCursorHover`
 * only pushes the breakpoint tool onto a hovered link when the flag is set, and the
 * two link-tool buttons are only reachable through that tool set.
 */
describe("WorkflowEditorComponent editor wiring", () => {
  let fixture: ComponentFixture<WorkflowEditorComponent>;
  let component: WorkflowEditorComponent;
  let workflowActionService: WorkflowActionService;
  let jointUIService: JointUIService;
  let dragDropService: DragDropService;
  let executeWorkflowService: ExecuteWorkflowService;
  let workflowStatusService: WorkflowStatusService;
  let agentService: AgentService;

  beforeEach(async () => {
    TestBed.resetTestingModule();
    await TestBed.configureTestingModule({
      imports: workflowEditorTestImports,
      providers: workflowEditorTestProviders,
    }).compileComponents();

    // ngAfterViewInit reads both of these once, so they have to be set before the first
    // detectChanges: the flag decides whether the breakpoint tool is ever attached, and
    // highlighting decides whether the cell-highlight handlers are installed at all.
    (TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService).setConfig({
      linkBreakpointEnabled: true,
    });
    workflowActionService = TestBed.inject(WorkflowActionService);
    workflowActionService.setHighlightingEnabled(true);
    jointUIService = TestBed.inject(JointUIService);
    dragDropService = TestBed.inject(DragDropService);
    executeWorkflowService = TestBed.inject(ExecuteWorkflowService);
    workflowStatusService = TestBed.inject(WorkflowStatusService);
    agentService = TestBed.inject(AgentService);

    fixture = TestBed.createComponent(WorkflowEditorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  // mockCommentBox's id is "1", the same as mockScanPredicate's, and a joint graph cannot hold
  // two cells under one id — these tests put both on the canvas, so they need a distinct box.
  const commentBox = { ...mockCommentBox, commentBoxID: "comment-box-1" };

  /** A port magnet as JointJS hands it to the paper's validate callbacks. */
  function magnet(attributes: Record<string, string>): SVGElement {
    const element = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    Object.entries(attributes).forEach(([name, value]) => element.setAttribute(name, value));
    return element as unknown as SVGElement;
  }

  /** Adds scan -> result and returns the link's id, model and rendered view. */
  function addLinkedPair() {
    workflowActionService.addOperatorsAndLinks(
      [
        { op: mockScanPredicate, pos: mockPoint },
        { op: mockResultPredicate, pos: mockPoint },
      ],
      [mockScanResultLink]
    );
    const model = component.paper.getModelById(mockScanResultLink.linkID);
    return { linkID: mockScanResultLink.linkID, model, view: model.findView(component.paper) as any };
  }

  /** Adds a placeholder cell per region member, then drives the region-update stream. */
  function addRegions(regions: readonly [number, string[]][]): void {
    regions
      .flatMap(([, operators]) => operators)
      .forEach(operatorID => {
        const cell = new joint.shapes.basic.Rect({ position: { x: 0, y: 0 }, size: { width: 80, height: 40 } });
        cell.set("id", operatorID);
        component.paper.model.addCell(cell);
      });
    (executeWorkflowService as any).regionUpdateStream.next({ regions });
  }

  /** An OperatorStatistics payload in the given state. */
  function statisticsIn(state: OperatorState) {
    return {
      operatorState: state,
      aggregatedInputRowCount: 0,
      inputPortMetrics: {},
      aggregatedOutputRowCount: 0,
      outputPortMetrics: {},
    };
  }

  /** Clicks the chat button of a cell, the way `.chat-button` does. */
  function clickChatButton(cellID: string): void {
    const view = component.paper.findViewByModel(cellID);
    (component.paper as any).trigger("element:chat", view, new Event("click"), 0, 0);
  }

  /**
   * A blank:pointerdown starts the paper-pan gesture, which listens on document mousemove
   * until a mouseup; fire the mouseup so the listener does not leak into later tests.
   */
  function clickBlankCanvas(): void {
    (component.paper as any).trigger("blank:pointerdown");
    document.dispatchEvent(new MouseEvent("mouseup"));
  }

  describe("paper options", () => {
    it("only lets a link start from an output-port magnet", () => {
      // validateMagnet is what stops the user dragging a new link out of an input port.
      const validateMagnet = (component.paper.options as any).validateMagnet;
      const cellView = {} as joint.dia.CellView;

      expect(
        validateMagnet.call(component.paper, cellView, magnet({ "port-group": "out" }), new Event("mousedown"))
      ).toBe(true);
      expect(
        validateMagnet.call(component.paper, cellView, magnet({ "port-group": "in" }), new Event("mousedown"))
      ).toBe(false);
    });

    it("routes the validateConnection option through the operator-level validation", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
      const cellView = (id: string) => ({ model: { id } }) as unknown as joint.dia.CellView;
      const validateConnection = (component.paper.options as any).validateConnection;
      const args = () => [
        cellView(mockScanPredicate.operatorID),
        magnet({ "port-group": "out", port: "output-0" }),
        cellView(mockSentimentPredicate.operatorID),
        magnet({ "port-group": "in", port: "input-0" }),
        "target",
        {} as joint.dia.LinkView,
      ];

      expect(validateConnection.apply(component.paper, args())).toBe(true);

      // Once the link exists the same pair is rejected, which only the delegated
      // duplicate-link check knows about.
      workflowActionService.addLink(mockScanSentimentLink);
      expect(validateConnection.apply(component.paper, args())).toBe(false);
    });

    it("locks the paper and ignores element buttons while workflow modification is disabled", () => {
      const texeraGraph = workflowActionService.getTexeraGraph();
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const view = component.paper.findViewByModel(mockScanPredicate.operatorID);

      workflowActionService.disableWorkflowModification();
      expect((component.paper.options.interactive as any).elementMove).toBe(false);
      (component.paper as any).trigger("element:delete", view, new Event("click"), 0, 0);
      expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBe(true);

      workflowActionService.enableWorkflowModification();
      // the default option leaves element dragging alone, so the key is simply absent
      expect((component.paper.options.interactive as any).elementMove).toBeUndefined();
      (component.paper as any).trigger("element:delete", view, new Event("click"), 0, 0);
      expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBe(false);
    });
  });

  describe("execution status streams", () => {
    it("forwards each operator's statistics, tagging which end of the graph it sits on", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint); // no input ports  -> source
      workflowActionService.addOperator(mockResultPredicate, mockPoint); // no output ports -> sink
      const changeStatistics = vi.spyOn(jointUIService, "changeOperatorStatistics");

      (workflowStatusService as any).statusSubject.next({
        [mockScanPredicate.operatorID]: statisticsIn(OperatorState.Running),
        [mockResultPredicate.operatorID]: statisticsIn(OperatorState.Completed),
      });

      expect(changeStatistics).toHaveBeenCalledWith(
        component.paper,
        mockScanPredicate.operatorID,
        statisticsIn(OperatorState.Running),
        true,
        false
      );
      expect(changeStatistics).toHaveBeenCalledWith(
        component.paper,
        mockResultPredicate.operatorID,
        statisticsIn(OperatorState.Completed),
        false,
        true
      );
    });

    it("overrides the reported state with Recovering while the execution is recovering", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      vi.spyOn(executeWorkflowService, "getExecutionState").mockReturnValue({
        state: ExecutionState.Recovering,
      } as any);
      const changeStatistics = vi.spyOn(jointUIService, "changeOperatorStatistics");

      (workflowStatusService as any).statusSubject.next({
        [mockScanPredicate.operatorID]: statisticsIn(OperatorState.Running),
      });

      expect(changeStatistics).toHaveBeenCalledWith(
        component.paper,
        mockScanPredicate.operatorID,
        expect.objectContaining({ operatorState: OperatorState.Recovering }),
        true,
        false
      );
    });

    it("does not invent statistics for an operator missing from the status payload", () => {
      // The isDefined guard matters most while recovering: without it the operator would be
      // handed a synthesized `{ operatorState: Recovering }` instead of nothing at all.
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      vi.spyOn(executeWorkflowService, "getExecutionState").mockReturnValue({
        state: ExecutionState.Recovering,
      } as any);
      const changeStatistics = vi.spyOn(jointUIService, "changeOperatorStatistics");

      (workflowStatusService as any).statusSubject.next({
        [mockScanPredicate.operatorID]: statisticsIn(OperatorState.Running),
      });

      expect(changeStatistics).toHaveBeenCalledWith(
        component.paper,
        mockResultPredicate.operatorID,
        undefined,
        false,
        true
      );
    });

    it("repaints every operator with the state the execution recovered into", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      const changeState = vi.spyOn(jointUIService, "changeOperatorState");
      const recoverInto = (current: ExecutionState) =>
        (executeWorkflowService as any).executionStateStream.next({
          previous: { state: ExecutionState.Recovering },
          current: { state: current },
        });

      recoverInto(ExecutionState.Paused);
      expect(changeState).toHaveBeenCalledWith(component.paper, mockScanPredicate.operatorID, OperatorState.Paused);
      expect(changeState).toHaveBeenCalledWith(component.paper, mockResultPredicate.operatorID, OperatorState.Paused);

      recoverInto(ExecutionState.Completed);
      expect(changeState).toHaveBeenCalledWith(component.paper, mockScanPredicate.operatorID, OperatorState.Completed);

      recoverInto(ExecutionState.Running);
      expect(changeState).toHaveBeenCalledWith(component.paper, mockScanPredicate.operatorID, OperatorState.Running);
    });

    it("refuses to guess a color for an unrecognized transition out of recovering", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      // By default rxjs reports an error thrown inside a subscriber asynchronously, which would
      // slip past the assertion and surface as an unhandled error; this flag makes it rethrow
      // out of the next() call instead. Restored in the finally so later specs are unaffected.
      rxjsConfig.useDeprecatedSynchronousErrorHandling = true;
      try {
        expect(() =>
          (executeWorkflowService as any).executionStateStream.next({
            previous: { state: ExecutionState.Recovering },
            current: { state: ExecutionState.Terminated },
          })
        ).toThrowError("unknown state transition from recovering state: Terminated");
      } finally {
        rxjsConfig.useDeprecatedSynchronousErrorHandling = false;
      }
    });

    it("leaves operator colors alone for a transition that did not come out of recovering", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const changeState = vi.spyOn(jointUIService, "changeOperatorState");

      (executeWorkflowService as any).executionStateStream.next({
        previous: { state: ExecutionState.Running },
        current: { state: ExecutionState.Completed },
      });

      expect(changeState).not.toHaveBeenCalled();
    });
  });

  describe("region overlays", () => {
    it("reshapes the region drawn around an operator that moved", () => {
      // Two regions so the handler's filter is exercised on both sides. Nothing can be asserted
      // about the untouched one: its outline is recomputed from its own, unmoved operator, so a
      // filter that matched every region would produce the identical path anyway.
      addRegions([
        [1, ["region-op-a"]],
        [2, ["region-op-b"]],
      ]);
      const movedRegion = component.paper.getModelById("region-1");
      const before = movedRegion.attr("body/d");
      expect(before).toBeTruthy();

      (<joint.dia.Element>component.paper.getModelById("region-op-a")).translate(150, 0);

      expect(movedRegion.attr("body/d")).not.toEqual(before);
    });

    it("tints a region with the color of the phase it reports", () => {
      addRegions([[1, ["region-op-a"]]]);
      const region = component.paper.getModelById("region-1");

      (executeWorkflowService as any).regionStateStream.next({ id: 1, state: "ExecutingDependeePortsPhase" });
      expect(region.attr("body/fill")).toEqual("rgba(33,150,243,0.2)");

      (executeWorkflowService as any).regionStateStream.next({ id: 1, state: "Completed" });
      expect(region.attr("body/fill")).toEqual("rgba(76,175,80,0.2)");
    });
  });

  describe("paper viewport", () => {
    it("pans the paper by the pointer delta scaled back into paper coordinates", () => {
      // At 50% zoom a 10px screen drag has to move the paper 20 paper-units, otherwise the
      // canvas drifts away from the cursor.
      workflowActionService.getJointGraphWrapper().setZoomProperty(0.5);
      const before = component.paper.translate();
      (component.paper as any).trigger("blank:pointerdown");

      const drag = new MouseEvent("mousemove");
      Object.defineProperty(drag, "movementX", { value: 10 });
      Object.defineProperty(drag, "movementY", { value: -6 });
      document.dispatchEvent(drag);
      document.dispatchEvent(new MouseEvent("mouseup"));

      expect(component.paper.translate().tx).toEqual(before.tx + 20);
      expect(component.paper.translate().ty).toEqual(before.ty - 12);
    });

    it("resizes the paper to the wrapper's dimensions on a window resize", fakeAsync(() => {
      // The paper follows the wrapper, not the editor: the wrapper is what shrinks when the
      // result panel opens.
      Object.defineProperty(component.editorWrapper, "offsetWidth", { value: 321, configurable: true });
      Object.defineProperty(component.editorWrapper, "offsetHeight", { value: 123, configurable: true });
      const setDimensions = vi.spyOn(component.paper, "setDimensions");

      window.dispatchEvent(new Event("resize"));
      tick(30); // the handler audits the resize stream down to one event every 30ms

      expect(setDimensions).toHaveBeenCalledWith(321, 123);
    }));
  });

  describe("operator badge streams", () => {
    it("repaints the result icon when an operator is marked and unmarked for viewing", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const changeViewResult = vi.spyOn(jointUIService, "changeOperatorViewResultStatus");

      workflowActionService.setViewOperatorResults([mockScanPredicate.operatorID]);
      expect(changeViewResult).toHaveBeenLastCalledWith(
        component.paper,
        expect.objectContaining({ operatorID: mockScanPredicate.operatorID, viewResult: true }),
        true
      );

      workflowActionService.unsetViewOperatorResults([mockScanPredicate.operatorID]);
      expect(changeViewResult).toHaveBeenLastCalledWith(
        component.paper,
        expect.objectContaining({ operatorID: mockScanPredicate.operatorID, viewResult: false }),
        false
      );
    });

    it("repaints the cache icon when an operator is marked and unmarked for reuse", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const changeReuseCache = vi.spyOn(jointUIService, "changeOperatorReuseCacheStatus");

      workflowActionService.markReuseResults([mockScanPredicate.operatorID]);
      expect(changeReuseCache).toHaveBeenLastCalledWith(
        component.paper,
        expect.objectContaining({ operatorID: mockScanPredicate.operatorID, markedForReuse: true })
      );

      workflowActionService.removeMarkReuseResults([mockScanPredicate.operatorID]);
      expect(changeReuseCache).toHaveBeenLastCalledWith(
        component.paper,
        expect.objectContaining({ operatorID: mockScanPredicate.operatorID, markedForReuse: false })
      );
    });

    it("renders a renamed operator's display name on its joint element", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const texeraGraph = workflowActionService.getTexeraGraph();

      (texeraGraph as any).operatorDisplayNameChangedSubject.next({
        operatorID: mockScanPredicate.operatorID,
        newDisplayName: "Renamed Scan",
      });

      expect(component.paper.getModelById(mockScanPredicate.operatorID).attr(`.${operatorNameClass}/text`)).toEqual(
        "Renamed Scan"
      );
    });

    it("renders a renamed port's display name on the port label", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const texeraGraph = workflowActionService.getTexeraGraph();
      const element = <joint.dia.Element>workflowActionService.getJointGraph().getCell(mockScanPredicate.operatorID);

      texeraGraph.portDisplayNameChangedSubject.next({
        operatorID: mockScanPredicate.operatorID,
        portID: "output-0",
        newDisplayName: "results",
      });

      expect(element.portProp("output-0", "attrs/.port-label/text")).toEqual("results");
    });
  });

  describe("selection and highlight handlers", () => {
    it("opens the result panel when an operator is double-clicked, but not for a link", () => {
      const { view: linkView } = addLinkedPair();
      const opened: boolean[] = [];
      workflowActionService.resultPanelOpen$.subscribe(value => opened.push(value));
      const operatorView = component.paper.findViewByModel(mockScanPredicate.operatorID);

      (component.paper as any).trigger("cell:pointerdblclick", operatorView, { shiftKey: false });
      expect(opened).toEqual([true]);

      // a link is not an element, so the handler skips it entirely
      (component.paper as any).trigger("cell:pointerdblclick", linkView, { shiftKey: false });
      expect(opened).toEqual([true]);
    });

    it("highlights the link running between two shift-selected operators", () => {
      const { linkID } = addLinkedPair();
      const wrapper = workflowActionService.getJointGraphWrapper();
      wrapper.unhighlightOperators(...wrapper.getCurrentHighlightedOperatorIDs());
      const scanView = component.paper.findViewByModel(mockScanPredicate.operatorID);
      const resultView = component.paper.findViewByModel(mockResultPredicate.operatorID);

      (component.paper as any).trigger("cell:pointerdown", scanView, { shiftKey: true });
      (component.paper as any).trigger("cell:pointerdown", resultView, { shiftKey: true });

      expect(wrapper.getCurrentHighlightedLinkIDs()).toEqual([linkID]);
    });

    it("toggles a comment box in and out of a shift-selection", () => {
      const wrapper = workflowActionService.getJointGraphWrapper();
      workflowActionService.addCommentBox(commentBox);
      wrapper.unhighlightCommentBoxes(...wrapper.getCurrentHighlightedCommentBoxIDs());
      const view = component.paper.findViewByModel(commentBox.commentBoxID);

      (component.paper as any).trigger("cell:pointerdown", view, { shiftKey: true });
      expect(wrapper.getCurrentHighlightedCommentBoxIDs()).toEqual([commentBox.commentBoxID]);

      (component.paper as any).trigger("cell:pointerdown", view, { shiftKey: true });
      expect(wrapper.getCurrentHighlightedCommentBoxIDs()).toEqual([]);
    });

    it("replaces the whole selection when a comment box is clicked without shift", () => {
      const wrapper = workflowActionService.getJointGraphWrapper();
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addCommentBox(commentBox);
      wrapper.unhighlightCommentBoxes(...wrapper.getCurrentHighlightedCommentBoxIDs());
      wrapper.highlightOperators(mockScanPredicate.operatorID);
      const view = component.paper.findViewByModel(commentBox.commentBoxID);

      (component.paper as any).trigger("cell:pointerdown", view, { shiftKey: false });

      expect(wrapper.getCurrentHighlightedCommentBoxIDs()).toEqual([commentBox.commentBoxID]);
      expect(wrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);
    });

    it("grows a port when it is highlighted and shrinks it back when it is not", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const element = <joint.dia.Element>workflowActionService.getJointGraph().getCell(mockScanPredicate.operatorID);
      const port = { operatorID: mockScanPredicate.operatorID, portID: "output-0" };

      workflowActionService.highlightPorts(false, port);
      expect(element.portProp("output-0", "attrs/.port-body/r")).toEqual(8);

      workflowActionService.unhighlightPorts(port);
      expect(element.portProp("output-0", "attrs/.port-body/r")).toEqual(5);
      expect(element.portProp("output-0", "attrs/.port-body/stroke")).toEqual("none");
    });

    it("toggles a port in and out of a shift-selection when its magnet is clicked", () => {
      const wrapper = workflowActionService.getJointGraphWrapper();
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const view = component.paper.findViewByModel(mockScanPredicate.operatorID);
      const portMagnet = { getAttribute: (name: string) => (name === "port" ? "output-0" : null) };
      const shiftClickPort = () =>
        (component.paper as any).trigger("element:magnet:pointerclick", view, { shiftKey: true }, portMagnet);

      shiftClickPort();
      expect(wrapper.getCurrentHighlightedPortIDs()).toContainEqual({
        operatorID: mockScanPredicate.operatorID,
        portID: "output-0",
      });

      shiftClickPort();
      expect(wrapper.getCurrentHighlightedPortIDs()).toEqual([]);
    });

    it("outlines an operator suggested as a drop target and clears the outline afterwards", () => {
      const wrapper = workflowActionService.getJointGraphWrapper();
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      wrapper.unhighlightOperators(...wrapper.getCurrentHighlightedOperatorIDs());
      const view = component.paper.findViewByModel(mockScanPredicate.operatorID);
      const outlines = () => view.$el.children(".joint-highlight-stroke").length;
      expect(outlines()).toEqual(0);

      (dragDropService as any).operatorSuggestionHighlightStream.next(mockScanPredicate.operatorID);
      expect(outlines()).toEqual(1);

      (dragDropService as any).operatorSuggestionUnhighlightStream.next(mockScanPredicate.operatorID);
      expect(outlines()).toEqual(0);
    });

    it("deletes a comment box when its delete button fires", () => {
      const texeraGraph = workflowActionService.getTexeraGraph();
      workflowActionService.addCommentBox(commentBox);
      const view = component.paper.findViewByModel(commentBox.commentBoxID);

      (component.paper as any).trigger("element:delete", view, new Event("click"), 0, 0);

      expect(texeraGraph.hasCommentBox(commentBox.commentBoxID)).toBe(false);
    });

    it("unfolds the clicked operator's details and folds the one opened before it", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
      const deleteButton = (operatorID: string) =>
        component.paper.getModelById(operatorID).attr(".delete-button/visibility");

      (component.paper as any).trigger(
        "element:pointerdown",
        component.paper.findViewByModel(mockScanPredicate.operatorID),
        {}
      );
      expect(deleteButton(mockScanPredicate.operatorID)).toEqual("visible");

      (component.paper as any).trigger(
        "element:pointerdown",
        component.paper.findViewByModel(mockSentimentPredicate.operatorID),
        {}
      );
      expect(deleteButton(mockScanPredicate.operatorID)).toEqual("hidden");
      expect(deleteButton(mockSentimentPredicate.operatorID)).toEqual("visible");
    });

    it("unfolds operator details on a right-click too", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
      const deleteButton = (operatorID: string) =>
        component.paper.getModelById(operatorID).attr(".delete-button/visibility");

      (component.paper as any).trigger(
        "element:contextmenu",
        component.paper.findViewByModel(mockScanPredicate.operatorID),
        {}
      );
      expect(deleteButton(mockScanPredicate.operatorID)).toEqual("visible");

      (component.paper as any).trigger(
        "element:contextmenu",
        component.paper.findViewByModel(mockSentimentPredicate.operatorID),
        {}
      );
      expect(deleteButton(mockScanPredicate.operatorID)).toEqual("hidden");
      expect(deleteButton(mockSentimentPredicate.operatorID)).toEqual("visible");
    });

    it("highlights a right-clicked link so the context menu acts on it", () => {
      const { linkID, view } = addLinkedPair();
      const wrapper = workflowActionService.getJointGraphWrapper();

      (component.paper as any).trigger("link:contextmenu", view, {});

      expect(wrapper.getCurrentHighlightedLinkIDs()).toEqual([linkID]);
    });

    it("folds the open operator's details when the blank canvas is clicked", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const deleteButton = () =>
        component.paper.getModelById(mockScanPredicate.operatorID).attr(".delete-button/visibility");
      (component.paper as any).trigger(
        "element:pointerdown",
        component.paper.findViewByModel(mockScanPredicate.operatorID),
        {}
      );
      expect(deleteButton()).toEqual("visible");

      clickBlankCanvas();

      expect(deleteButton()).toEqual("hidden");
    });
  });

  describe("link tools", () => {
    /** Hovers the link so handleLinkCursorHover attaches the tool set, and returns the tools. */
    function hoverLink(view: any): any[] {
      (component.paper as any).trigger("link:mouseenter", view, new Event("mouseenter"), 0, 0);
      return view._toolsView.tools;
    }

    it("offers both the remove and the breakpoint tool when the cursor enters a link", () => {
      const { view } = addLinkedPair();

      expect(hoverLink(view).map(tool => tool.name)).toEqual(["remove-button", "info-button"]);
    });

    it("deletes the link when the remove tool is activated", () => {
      const texeraGraph = workflowActionService.getTexeraGraph();
      const { linkID, view } = addLinkedPair();
      const removeTool = hoverLink(view).find(tool => tool.name === "remove-button");

      removeTool.options.action.call(removeTool, new Event("click"), view);

      expect(texeraGraph.hasLinkWithID(linkID)).toBe(false);
    });

    it("highlights the link when the breakpoint tool is activated", () => {
      const { linkID, view } = addLinkedPair();
      const wrapper = workflowActionService.getJointGraphWrapper();
      wrapper.unhighlightElements(wrapper.getCurrentHighlights());
      const breakpointTool = hoverLink(view).find(tool => tool.name === "info-button");

      breakpointTool.options.action.call(breakpointTool, new Event("click"), view);

      expect(wrapper.getCurrentHighlightedLinkIDs()).toEqual([linkID]);
    });
  });

  describe("shared-editing presence", () => {
    it("publishes the cursor position and the enter/leave presence flag", () => {
      const texeraGraph = workflowActionService.getTexeraGraph();
      const updateAwareness = vi.spyOn(texeraGraph, "updateSharedModelAwareness");
      // the real clientToLocalPoint needs SVGGraphicsElement.getScreenCTM, which jsdom lacks;
      // stubbing it still pins that the client coordinates go through the paper's conversion
      const toLocalPoint = vi.spyOn(component.paper, "clientToLocalPoint").mockReturnValue({ x: 7, y: 9 } as any);

      component.editor.dispatchEvent(new MouseEvent("mousemove", { clientX: 40, clientY: 60 }));
      expect(toLocalPoint).toHaveBeenCalledWith({ x: 40, y: 60 });
      expect(updateAwareness).toHaveBeenLastCalledWith("userCursor", { x: 7, y: 9 });

      component.editor.dispatchEvent(new MouseEvent("mouseenter"));
      expect(updateAwareness).toHaveBeenLastCalledWith("isActive", true);

      component.editor.dispatchEvent(new MouseEvent("mouseleave"));
      expect(updateAwareness).toHaveBeenLastCalledWith("isActive", false);
    });
  });

  describe("agent hover labels", () => {
    const agentName = "Agent One";
    let hovered: Subject<{ viewedOperatorIds: string[]; addedOperatorIds: string[]; modifiedOperatorIds: string[] }>;

    beforeEach(() => {
      hovered = new Subject();
      // getAllAgents is an HTTP call that never resolves under HttpClientTestingModule, so the
      // per-agent hover subscriptions are only reachable by standing in for it.
      vi.spyOn(agentService, "getAllAgents").mockReturnValue(of([{ id: "agent-1", name: agentName } as any]));
      vi.spyOn(agentService, "getHoveredMessageOperatorsObservable").mockReturnValue(hovered.asObservable());
      // an agent change re-runs the setup, which is what picks up the stubs above
      (agentService as any).agentChangeSubject.next();
    });

    it("labels the viewed, added and modified operators of the hovered message", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);
      const showLabel = vi.spyOn(jointUIService, "showAgentActionLabel");

      hovered.next({
        viewedOperatorIds: [mockScanPredicate.operatorID],
        addedOperatorIds: [mockSentimentPredicate.operatorID],
        modifiedOperatorIds: [mockResultPredicate.operatorID],
      });

      expect(showLabel).toHaveBeenCalledWith(component.paper, mockScanPredicate.operatorID, "viewed", agentName);
      expect(showLabel).toHaveBeenCalledWith(component.paper, mockSentimentPredicate.operatorID, "added", agentName);
      expect(showLabel).toHaveBeenCalledWith(component.paper, mockResultPredicate.operatorID, "modified", agentName);
      expect(showLabel).toHaveBeenCalledTimes(3);
    });

    it("clears the labels of the previous hover and skips ids that left the canvas", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      const labelVisibility = () =>
        component.paper
          .getModelById(mockScanPredicate.operatorID)
          .attr(`.${operatorAgentActionProgressClass}/visibility`);
      hovered.next({
        viewedOperatorIds: [mockScanPredicate.operatorID],
        addedOperatorIds: [],
        modifiedOperatorIds: [],
      });
      expect(labelVisibility()).toEqual("visible");
      const showLabel = vi.spyOn(jointUIService, "showAgentActionLabel");

      hovered.next({
        viewedOperatorIds: ["deleted-operator"],
        addedOperatorIds: ["deleted-operator"],
        modifiedOperatorIds: ["deleted-operator"],
      });

      expect(labelVisibility()).toEqual("hidden");
      expect(showLabel).not.toHaveBeenCalled();
    });
  });

  describe("operator chat popover", () => {
    beforeEach(() => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
    });

    it("toggles the popover shut when the same operator's chat button is clicked again", () => {
      clickChatButton(mockScanPredicate.operatorID);
      expect(component.chatPopoverOperator?.operatorId).toEqual(mockScanPredicate.operatorID);

      clickChatButton(mockScanPredicate.operatorID);

      expect(component.chatPopoverOperator).toBeNull();
    });

    it("leaves the popover alone when the chat event arrives from a cell that is not an operator", () => {
      workflowActionService.addCommentBox(commentBox);
      clickChatButton(mockScanPredicate.operatorID);
      const getOperator = vi.spyOn(workflowActionService.getTexeraGraph(), "getOperator");

      clickChatButton(commentBox.commentBoxID);

      // The popover surviving is not enough on its own: without the guard the handler reads the
      // cell as an operator and throws, which rxjs reports asynchronously and the popover keeps
      // its old value anyway. Pin the bail-out itself.
      expect(getOperator).not.toHaveBeenCalled();
      expect(component.chatPopoverOperator?.operatorId).toEqual(mockScanPredicate.operatorID);
    });

    it("closes the popover when the blank canvas is clicked", () => {
      clickChatButton(mockScanPredicate.operatorID);
      expect(component.chatPopoverOperator).not.toBeNull();

      clickBlankCanvas();

      expect(component.chatPopoverOperator).toBeNull();
    });

    it("keeps the popover anchored to its operator when the operator is dragged", () => {
      clickChatButton(mockScanPredicate.operatorID);
      const before = { ...component.chatPopoverOperator!.position };

      (<joint.dia.Element>component.paper.getModelById(mockScanPredicate.operatorID)).translate(60, 30);

      expect(component.chatPopoverOperator!.position).toEqual({ x: before.x + 60, y: before.y + 30 });
    });

    it("rescales the popover anchor when the canvas is zoomed", () => {
      clickChatButton(mockScanPredicate.operatorID);
      const before = { ...component.chatPopoverOperator!.position };

      workflowActionService.getJointGraphWrapper().setZoomProperty(0.5);

      // the anchor is a screen position, so it follows the zoom; the trailing 40px that clears
      // the operator's display name is added afterwards and is not scaled
      expect(component.chatPopoverOperator!.position).toEqual({
        x: before.x * 0.5,
        y: (before.y - 40) * 0.5 + 40,
      });
    });

    it("reports no anchor for an operator that is not on the paper", () => {
      expect((component as any).getOperatorChatPopoverPosition("not-on-the-paper")).toBeNull();
    });

    it("re-renders only while the popover is open when new agent summaries arrive", () => {
      const detectChanges = vi.spyOn((component as any).changeDetectorRef, "detectChanges");

      (agentService as any).operatorResultSummariesSubject.next(new Map());
      expect(detectChanges).not.toHaveBeenCalled();

      clickChatButton(mockScanPredicate.operatorID);
      detectChanges.mockClear();
      (agentService as any).operatorResultSummariesSubject.next(new Map());

      expect(detectChanges).toHaveBeenCalledTimes(1);
    });
  });
});

/**
 * Ambient operator recommender (apache/texera#5240).
 *
 * The feature is opt-in and `handleNextOperatorSuggestions` reads the flag once
 * during ngAfterViewInit, returning early when it is off — so the flag has to
 * be set before the first change detection, not inside the individual tests.
 */
describe("Ambient Operator Recommender", () => {
  let component: WorkflowEditorComponent;
  let fixture: ComponentFixture<WorkflowEditorComponent>;
  let workflowActionService: WorkflowActionService;
  let dragDropService: DragDropService;
  let getRecommendationsSpy: MockInstance;

  const mockRecommendations = [{ operatorType: "NlpSentiment", score: 0.9, reason: "Analyze the text" }];

  // The recommender listens to the drag-drop service's drop stream. Push the
  // dropped operator through directly rather than driving a real drag gesture,
  // which would need a live flying-operator paper.
  function emitOperatorDrop(operator: OperatorPredicate): void {
    (dragDropService as any).operatorDroppedSubject.next(operator);
  }

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: workflowEditorTestImports,
      providers: workflowEditorTestProviders,
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(WorkflowEditorComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    dragDropService = TestBed.inject(DragDropService);

    (TestBed.inject(GuiConfigService).env as any).operatorRecommendationEnabled = true;
    getRecommendationsSpy = vi
      .spyOn(TestBed.inject(OperatorRecommendationService), "getRecommendations")
      .mockReturnValue(of(mockRecommendations));

    // detect changes to run ngAfterViewInit and wire up the recommender
    fixture.detectChanges();
  });

  it("requests suggestions when the user drops an operator onto the canvas", () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    emitOperatorDrop(mockScanPredicate);

    expect(getRecommendationsSpy).toHaveBeenCalledTimes(1);
    expect(component.nextOperatorSuggestion?.operatorId).toEqual(mockScanPredicate.operatorID);
    expect(component.nextOperatorSuggestion?.recommendations).toEqual(mockRecommendations);
  });

  it("does not request suggestions for operators that arrive without a drop", () => {
    // Workflow load, undo/redo, paste and remote co-editor edits all reach the
    // graph through addOperator with no drop event, and must stay silent.
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockSentimentPredicate, mockPoint);

    expect(getRecommendationsSpy).not.toHaveBeenCalled();
    expect(component.nextOperatorSuggestion).toBeNull();
  });

  it("does not suggest when the drop already wired the output port", () => {
    // dragDropped adds links on both branches — dropping onto an existing edge, and
    // proximity auto-linking — so the operator can arrive with a successor.
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    workflowActionService.addLink(mockScanResultLink);

    emitOperatorDrop(mockScanPredicate);

    expect(getRecommendationsSpy).not.toHaveBeenCalled();
    expect(component.nextOperatorSuggestion).toBeNull();
  });

  it("does not materialize when the anchor port acquired a link while the chips were up", () => {
    const materializeSpy = vi.spyOn(TestBed.inject(OperatorRecommendationService), "materialize");
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    emitOperatorDrop(mockScanPredicate);
    expect(component.nextOperatorSuggestion).not.toBeNull();

    // The user hand-draws a link out of the anchor port. That gesture starts on
    // the port, so it never fires the blank:pointerdown that dismisses the chips.
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    workflowActionService.addLink(mockScanResultLink);

    component.materializeNextOperatorSuggestion(mockRecommendations[0]);

    expect(materializeSpy).not.toHaveBeenCalled();
    expect(component.nextOperatorSuggestion).toBeNull();
  });

  it("ignores a response that arrives after the user dismissed the suggestions", () => {
    const inFlight = new Subject<OperatorRecommendation[]>();
    getRecommendationsSpy.mockReturnValue(inFlight);
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    emitOperatorDrop(mockScanPredicate);

    // The user clicks blank canvas while the request is still in flight.
    (component.paper as any).trigger("blank:pointerdown");
    inFlight.next(mockRecommendations);

    expect(component.nextOperatorSuggestion).toBeNull();
  });

  it("lets the newest request win when an older response lands last", () => {
    const first = new Subject<OperatorRecommendation[]>();
    const second = new Subject<OperatorRecommendation[]>();
    getRecommendationsSpy.mockReturnValueOnce(first).mockReturnValueOnce(second);

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
    emitOperatorDrop(mockScanPredicate);
    emitOperatorDrop(mockSentimentPredicate);

    // The superseded request answers last; it must not show stale chips.
    first.next(mockRecommendations);
    expect(component.nextOperatorSuggestion).toBeNull();

    second.next(mockRecommendations);
    expect(component.nextOperatorSuggestion?.operatorId).toEqual(mockSentimentPredicate.operatorID);
  });

  it("dismisses the suggestions when the anchor operator is deleted", () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    emitOperatorDrop(mockScanPredicate);
    expect(component.nextOperatorSuggestion).not.toBeNull();

    workflowActionService.deleteOperator(mockScanPredicate.operatorID);

    expect(component.nextOperatorSuggestion).toBeNull();
  });

  it("chains a fresh round of suggestions onto the operator a clicked suggestion creates", () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    emitOperatorDrop(mockScanPredicate);
    getRecommendationsSpy.mockClear();

    component.materializeNextOperatorSuggestion(mockRecommendations[0]);

    // Materializing never emits a drop, so chaining only works if the click
    // path asks for suggestions itself.
    expect(getRecommendationsSpy).toHaveBeenCalledTimes(1);
    expect(component.nextOperatorSuggestion).not.toBeNull();
    expect(component.nextOperatorSuggestion?.operatorId).not.toEqual(mockScanPredicate.operatorID);
  });

  it("skips change detection when repositioning leaves the suggestions where they are", () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    emitOperatorDrop(mockScanPredicate);
    const detectChangesSpy = vi.spyOn((component as any).changeDetectorRef, "detectChanges");

    // Unchanged position: repositioning must be a no-op. Without this guard the
    // change:position listener re-renders the component on every drag frame.
    (component as any).repositionNextOperatorSuggestions();

    expect(detectChangesSpy).not.toHaveBeenCalled();

    // Moving the operator does move the chips with it.
    workflowActionService.getJointGraphWrapper().setElementPosition(mockScanPredicate.operatorID, 50, 30);
    detectChangesSpy.mockClear();
    (component as any).repositionNextOperatorSuggestions();

    expect(detectChangesSpy).toHaveBeenCalledTimes(1);
  });
});
