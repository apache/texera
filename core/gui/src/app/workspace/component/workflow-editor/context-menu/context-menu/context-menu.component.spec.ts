import { ComponentFixture, TestBed } from "@angular/core/testing";
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "src/app/workspace/service/operator-metadata/stub-operator-metadata.service";

import { ContextMenuComponent } from "./context-menu.component";
import { HttpClientModule } from "@angular/common/http";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "src/app/workspace/service/workflow-result/workflow-result.service";
import { WorkflowResultExportService } from "src/app/workspace/service/workflow-result-export/workflow-result-export.service";
import { OperatorMenuService } from "src/app/workspace/service/operator-menu/operator-menu.service";
import { of, BehaviorSubject } from "rxjs";
import { ReactiveFormsModule } from "@angular/forms";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { ValidationWorkflowService } from "src/app/workspace/service/validation/validation-workflow.service";

describe("ContextMenuComponent", () => {
  let component: ContextMenuComponent;
  let fixture: ComponentFixture<ContextMenuComponent>;
  let workflowActionService: jasmine.SpyObj<WorkflowActionService>;
  let workflowResultService: jasmine.SpyObj<WorkflowResultService>;
  let workflowResultExportService: jasmine.SpyObj<WorkflowResultExportService>;
  let operatorMenuService: any; // We'll define this more precisely below
  let jointGraphWrapperSpy: jasmine.SpyObj<any>;
  let validationWorkflowService: jasmine.SpyObj<ValidationWorkflowService>;

  beforeEach(async () => {
    // Create spies for the services
    jointGraphWrapperSpy = jasmine.createSpyObj("JointGraphWrapper", [
      "getCurrentHighlightedOperatorIDs",
      "getCurrentHighlightedCommentBoxIDs",
    ]);

    jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue([]);
    jointGraphWrapperSpy.getCurrentHighlightedCommentBoxIDs.and.returnValue([]);

    const workflowActionServiceSpy = jasmine.createSpyObj("WorkflowActionService", [
      "getJointGraphWrapper",
      "getWorkflowModificationEnabledStream",
      "deleteOperatorsAndLinks",
      "deleteCommentBox",
    ]);
    workflowActionServiceSpy.getJointGraphWrapper.and.returnValue(jointGraphWrapperSpy);
    workflowActionServiceSpy.getWorkflowModificationEnabledStream.and.returnValue(of(true));
    workflowActionServiceSpy.deleteOperatorsAndLinks.and.returnValue();
    workflowActionServiceSpy.deleteCommentBox.and.returnValue();

    const workflowResultServiceSpy = jasmine.createSpyObj("WorkflowResultService", [
      "getResultService",
      "hasAnyResult",
    ]);
    const workflowResultExportServiceSpy = jasmine.createSpyObj("WorkflowResultExportService", [
      "exportOperatorsResultAsFile",
    ]);

    // Create a mock for OperatorMenuService with necessary properties and methods
    operatorMenuService = {
      highlightedOperators: new BehaviorSubject<any[]>([]),
      highlightedCommentBoxes: new BehaviorSubject<any[]>([]),
      isDisableOperator: false,
      isDisableOperatorClickable: false,
      isToViewResult: false,
      isToViewResultClickable: false,
      isMarkForReuse: false,
      isReuseResultClickable: false,
      saveHighlightedElements: jasmine.createSpy("saveHighlightedElements"),
      performPasteOperation: jasmine.createSpy("performPasteOperation"),
      disableHighlightedOperators: jasmine.createSpy("disableHighlightedOperators"),
      viewResultHighlightedOperators: jasmine.createSpy("viewResultHighlightedOperators"),
      reuseResultHighlightedOperator: jasmine.createSpy("reuseResultHighlightedOperator"),
      executeUpToOperator: jasmine.createSpy("executeUpToOperator"),
    };

    const validationWorkflowServiceSpy = jasmine.createSpyObj("ValidationWorkflowService", ["validateOperator"]);

    await TestBed.configureTestingModule({
      declarations: [ContextMenuComponent],
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: WorkflowActionService, useValue: workflowActionServiceSpy },
        { provide: WorkflowResultService, useValue: workflowResultServiceSpy },
        { provide: WorkflowResultExportService, useValue: workflowResultExportServiceSpy },
        { provide: OperatorMenuService, useValue: operatorMenuService },
        { provide: ValidationWorkflowService, useValue: validationWorkflowServiceSpy },
      ],
      imports: [HttpClientModule, ReactiveFormsModule, BrowserAnimationsModule, NzDropDownModule],
    }).compileComponents();

    workflowActionService = TestBed.inject(WorkflowActionService) as jasmine.SpyObj<WorkflowActionService>;
    workflowResultService = TestBed.inject(WorkflowResultService) as jasmine.SpyObj<WorkflowResultService>;
    workflowResultExportService = TestBed.inject(
      WorkflowResultExportService
    ) as jasmine.SpyObj<WorkflowResultExportService>;
    // operatorMenuService is already assigned
    validationWorkflowService = TestBed.inject(ValidationWorkflowService) as jasmine.SpyObj<ValidationWorkflowService>;

    fixture = TestBed.createComponent(ContextMenuComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("should return \"download multiple results\" when multiple operators are highlighted", () => {
    jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue(["operator1", "operator2"]);

    const label = component.writeDownloadLabel();
    expect(label).toBe("download multiple results");
  });

  it("should return \"download result as HTML file\" when one operator is highlighted and result snapshot is available", () => {
    jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue(["operator1"]);

    const resultServiceSpy = jasmine.createSpyObj("ResultService", ["getCurrentResultSnapshot"]);
    workflowResultService.getResultService.and.returnValue(resultServiceSpy);
    resultServiceSpy.getCurrentResultSnapshot.and.returnValue({ some: "snapshot" });

    const label = component.writeDownloadLabel();
    expect(label).toBe("download result as HTML file");
  });

  it("should return \"download result as CSV file\" when one operator is highlighted and result exists but no snapshot", () => {
    jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue(["operator1"]);

    workflowResultService.getResultService.and.returnValue(undefined);
    workflowResultService.hasAnyResult.and.returnValue(true);

    const label = component.writeDownloadLabel();
    expect(label).toBe("download result as CSV file");
  });

  it("should return \"download result\" when one operator is highlighted and no result is available", () => {
    jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue(["operator1"]);

    workflowResultService.getResultService.and.returnValue(undefined);
    workflowResultService.hasAnyResult.and.returnValue(false);

    const label = component.writeDownloadLabel();
    expect(label).toBe("download result");
  });

  it("should return \"download result\" when no operators are highlighted", () => {
    jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue([]);

    const label = component.writeDownloadLabel();
    expect(label).toBe("download result");
  });

  describe("isSelectedOperatorValid", () => {
    it("should return false when multiple operators are highlighted", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue(["op1", "op2"]);
      component.isWorkflowModifiable = true;

      expect(component.isSelectedOperatorValid()).toBeFalse();
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
    });

    it("should return false when no operators are highlighted", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue([]);
      component.isWorkflowModifiable = true;

      expect(component.isSelectedOperatorValid()).toBeFalse();
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
    });

    it("should return false when workflow is not modifiable", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue(["op1"]);
      component.isWorkflowModifiable = false;

      expect(component.isSelectedOperatorValid()).toBeFalse();
      expect(validationWorkflowService.validateOperator).not.toHaveBeenCalled();
    });

    it("should return true when single operator is highlighted, workflow is modifiable, and operator is valid", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue(["op1"]);
      component.isWorkflowModifiable = true;
      validationWorkflowService.validateOperator.and.returnValue({ isValid: true });

      expect(component.isSelectedOperatorValid()).toBeTrue();
      expect(validationWorkflowService.validateOperator).toHaveBeenCalledWith("op1");
    });

    it("should return false when single operator is highlighted but operator is invalid", () => {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.and.returnValue(["op1"]);
      component.isWorkflowModifiable = true;
      validationWorkflowService.validateOperator.and.returnValue({ isValid: false, messages: {} });

      expect(component.isSelectedOperatorValid()).toBeFalse();
      expect(validationWorkflowService.validateOperator).toHaveBeenCalledWith("op1");
    });
  });
});
