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
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "src/app/workspace/service/operator-metadata/stub-operator-metadata.service";

import { ContextMenuComponent } from "./context-menu.component";
import { HttpClientModule } from "@angular/common/http";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "src/app/workspace/service/workflow-result/workflow-result.service";
import { WorkflowResultExportService } from "src/app/workspace/service/workflow-result-export/workflow-result-export.service";
import { OperatorMenuService } from "src/app/workspace/service/operator-menu/operator-menu.service";
import { of } from "rxjs";
import { ReactiveFormsModule } from "@angular/forms";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { ValidationWorkflowService } from "src/app/workspace/service/validation/validation-workflow.service";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";
import { commonTestProviders } from "../../../../../common/testing/test-utils"; // Import NzModalModule and NzModalService
import type { Mocked } from "vitest";
describe("ContextMenuComponent", () => {
  let component: ContextMenuComponent;
  let fixture: ComponentFixture<ContextMenuComponent>;
  let workflowActionService: Mocked<WorkflowActionService>;
  let workflowResultService: Mocked<WorkflowResultService>;
  let workflowResultExportService: Mocked<WorkflowResultExportService>;
  let operatorMenuService: any; // We'll define this more precisely below
  let jointGraphWrapperSpy: any;
  let validationWorkflowService: Mocked<ValidationWorkflowService>;

  beforeEach(async () => {
    // Create spies for the services
    jointGraphWrapperSpy = {
      getCurrentHighlightedOperatorIDs: vi.fn(),
      getCurrentHighlightedCommentBoxIDs: vi.fn(),
      getCurrentHighlightedLinkIDs: vi.fn(),
    };

    jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue([]);
    jointGraphWrapperSpy.getCurrentHighlightedCommentBoxIDs.mockReturnValue([]);
    jointGraphWrapperSpy.getCurrentHighlightedLinkIDs.mockReturnValue([]);

    const texeraGraphSpy = { isOperatorDisabled: vi.fn(), hasLinkWithID: vi.fn(), bundleActions: vi.fn() };

    const workflowActionServiceSpy = {
      getJointGraphWrapper: vi.fn(),
      getWorkflowModificationEnabledStream: vi.fn(),
      deleteOperatorsAndLinks: vi.fn(),
      deleteCommentBox: vi.fn(),
      getWorkflowMetadata: vi.fn(),
      getTexeraGraph: vi.fn(),
      deleteLinkWithID: vi.fn(),
    };
    workflowActionServiceSpy.getJointGraphWrapper.mockReturnValue(jointGraphWrapperSpy);
    workflowActionServiceSpy.getWorkflowModificationEnabledStream.mockReturnValue(of(true));
    workflowActionServiceSpy.getTexeraGraph.mockReturnValue(texeraGraphSpy);
    workflowActionServiceSpy.deleteOperatorsAndLinks.mockReturnValue();
    workflowActionServiceSpy.deleteCommentBox.mockReturnValue();
    workflowActionServiceSpy.deleteLinkWithID.mockReturnValue();
    workflowActionServiceSpy.getWorkflowMetadata.mockReturnValue({ name: "Test Workflow" }); // Mock return value

    // Set up TexeraGraph spy return values
    texeraGraphSpy.hasLinkWithID.mockReturnValue(false);
    texeraGraphSpy.bundleActions.mockImplementation((callback: Function) => callback());

    const workflowResultServiceSpy = { getResultService: vi.fn(), hasAnyResult: vi.fn() };
    const workflowResultExportServiceSpy = { exportOperatorsResultAsFile: vi.fn() };

    // Create a mock for OperatorMenuService with necessary properties and methods
    operatorMenuService = {
      highlightedOperators$: of([] as readonly string[]),
      highlightedCommentBoxes$: of([] as readonly string[]),
      isDisableOperator: false,
      isDisableOperatorClickable: false,
      isToViewResult: false,
      isToViewResultClickable: false,
      isMarkForReuse: false,
      isReuseResultClickable: false,
      saveHighlightedElements: vi.fn(),
      performPasteOperation: vi.fn(),
      disableHighlightedOperators: vi.fn(),
      viewResultHighlightedOperators: vi.fn(),
      reuseResultHighlightedOperator: vi.fn(),
      executeUpToOperator: vi.fn(),
    };

    const validationWorkflowServiceSpy = { validateOperator: vi.fn() };

    await TestBed.configureTestingModule({
      declarations: [ContextMenuComponent],
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: WorkflowActionService, useValue: workflowActionServiceSpy },
        { provide: WorkflowResultService, useValue: workflowResultServiceSpy },
        { provide: WorkflowResultExportService, useValue: workflowResultExportServiceSpy },
        { provide: OperatorMenuService, useValue: operatorMenuService },
        { provide: ValidationWorkflowService, useValue: validationWorkflowServiceSpy },
        NzModalService, // Provide NzModalService
        ...commonTestProviders,
      ],
      imports: [
        HttpClientModule,
        ReactiveFormsModule,
        BrowserAnimationsModule,
        NzDropDownModule,
        NzModalModule, // Import NzModalModule
      ],
    }).compileComponents();

    workflowActionService = TestBed.inject(WorkflowActionService) as unknown as Mocked<WorkflowActionService>;
    workflowResultService = TestBed.inject(WorkflowResultService) as unknown as Mocked<WorkflowResultService>;
    workflowResultExportService = TestBed.inject(
      WorkflowResultExportService
    ) as unknown as Mocked<WorkflowResultExportService>;
    // operatorMenuService is already assigned
    validationWorkflowService = TestBed.inject(
      ValidationWorkflowService
    ) as unknown as Mocked<ValidationWorkflowService>;

    fixture = TestBed.createComponent(ContextMenuComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("isSelectedOperatorValid", () => {
    it("should return false when multiple operators are highlighted", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1", "op2"]);
      component.isWorkflowModifiable = true;

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
    });

    it("should return false when no operators are highlighted", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue([]);
      component.isWorkflowModifiable = true;

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
    });

    it("should return false when workflow is not modifiable", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1"]);
      component.isWorkflowModifiable = false;

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
    });

    it("should return true when single operator is highlighted, workflow is modifiable, and operator is valid", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1"]);
      component.isWorkflowModifiable = true;
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: true });

      expect(component.canExecuteOperator()).toBe(true);
      expect(validationWorkflowService.validateOperator).toHaveBeenCalledWith("op1");
    });

    it("should return false when single operator is highlighted but operator is invalid", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1"]);
      component.isWorkflowModifiable = true;
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: false, messages: {} });

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).toHaveBeenCalledWith("op1");
    });
  });

  describe("canExecuteOperator", () => {
    let texeraGraphSpy: any;

    beforeEach(() => {
      texeraGraphSpy = workflowActionService.getTexeraGraph() as any;
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1"]);
      component.isWorkflowModifiable = true;
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: true });
      texeraGraphSpy.isOperatorDisabled.mockReturnValue(false);
    });

    it("should return false when multiple operators are highlighted", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["op1", "op2"]);

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
      expect(texeraGraphSpy.isOperatorDisabled).not.toHaveBeenCalled();
    });

    it("should return false when no operators are highlighted", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue([]);

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
      expect(texeraGraphSpy.isOperatorDisabled).not.toHaveBeenCalled();
    });

    it("should return false when workflow is not modifiable", () => {
      component.isWorkflowModifiable = false;

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
      expect(texeraGraphSpy.isOperatorDisabled).not.toHaveBeenCalled();
    });

    it("should return true when all conditions are met (valid, enabled, modifiable)", () => {
      expect(component.canExecuteOperator()).toBe(true);
      expect(validationWorkflowService.validateOperator).toHaveBeenCalledWith("op1");
      expect(texeraGraphSpy.isOperatorDisabled).toHaveBeenCalledWith("op1");
    });

    it("should return false when operator is invalid and not check disabled status", () => {
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: false, messages: {} });

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).toHaveBeenCalledWith("op1");
      expect(texeraGraphSpy.isOperatorDisabled).not.toHaveBeenCalled();
    });

    it("should return false when operator is valid but disabled", () => {
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: true });
      texeraGraphSpy.isOperatorDisabled.mockReturnValue(true);

      expect(component.canExecuteOperator()).toBe(false);
      expect(validationWorkflowService.validateOperator).toHaveBeenCalledWith("op1");
      expect(texeraGraphSpy.isOperatorDisabled).toHaveBeenCalledWith("op1");
    });

    it("should check disabled status only for valid operators", () => {
      // First test with invalid operator
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: false, messages: {} });
      component.canExecuteOperator();
      expect(texeraGraphSpy.isOperatorDisabled).not.toHaveBeenCalled();

      // Then test with valid operator
      validationWorkflowService.validateOperator.mockReturnValue({ isValid: true });
      component.canExecuteOperator();
      expect(texeraGraphSpy.isOperatorDisabled).toHaveBeenCalledWith("op1");
    });
  });
});
