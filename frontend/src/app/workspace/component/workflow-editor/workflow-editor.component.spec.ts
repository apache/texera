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
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { ValidationWorkflowService } from "../../service/validation/validation-workflow.service";
import { WorkflowEditorComponent } from "./workflow-editor.component";
import { workflowEditorTestImports, workflowEditorTestProviders } from "./workflow-editor.test-utils";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { JointUIService, operatorAgentActionProgressClass } from "../../service/joint-ui/joint-ui.service";
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
import { OperatorState } from "../../types/execute-workflow.interface";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { OperatorLink, OperatorPredicate } from "../../types/workflow-common.interface";
import { tap } from "rxjs/operators";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { of, Subject } from "rxjs";
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
      const cachedCompleted = {
        [mockScanPredicate.operatorID]: {
          operatorState: OperatorState.Completed,
          aggregatedInputRowCount: 0,
          inputPortMetrics: {},
          aggregatedOutputRowCount: 0,
          outputPortMetrics: {},
        },
      };
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
