import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { PropertyEditorComponent } from "./property-editor.component";
import { environment } from "../../../../environments/environment";
import {
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink,
  mockScanSentimentLink,
  mockSentimentPredicate,
  mockSentimentResultLink,
} from "../../service/workflow-graph/model/mock-workflow-data";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OperatorPropertyEditFrameComponent } from "./operator-property-edit-frame/operator-property-edit-frame.component";
import { BreakpointPropertyEditFrameComponent } from "./breakpoint-property-edit-frame/breakpoint-property-edit-frame.component";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { WorkflowVersionService } from "src/app/dashboard/user/service/workflow-version/workflow-version.service";
import { VersionsDisplayFrameComponent } from "./versions-display/versions-display-frame.component";

describe("PropertyEditorComponent", () => {
  let component: PropertyEditorComponent;
  let fixture: ComponentFixture<PropertyEditorComponent>;
  let workflowActionService: WorkflowActionService;
  let workflowVersionService: WorkflowVersionService;
  environment.schemaPropagationEnabled = true;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PropertyEditorComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    workflowVersionService = TestBed.inject(WorkflowVersionService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  /**
   * test if the property editor correctly receives the operator unhighlight stream
   *  and clears all the operator data, and hide the form.
   */
  it("should clear and hide the property editor panel correctly when no operator is highlighted", () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add and highlight an operator
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

    fixture.detectChanges();

    expect(component.frameComponentConfig?.component).toBe(OperatorPropertyEditFrameComponent);
    expect(component.frameComponentConfig?.componentInputs).toEqual({
      currentOperatorId: mockScanPredicate.operatorID,
    });

    // unhighlight the operator
    jointGraphWrapper.unhighlightOperators(mockScanPredicate.operatorID);
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);
    fixture.detectChanges();

    // check if the clearPropertyEditor called after the operator
    //  is unhighlighted has correctly updated the variables
    expect(component.frameComponentConfig).toBeUndefined();
  });

  it("should clear and hide the property editor panel correctly when multiple operators are highlighted", () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add and highlight two operators
    workflowActionService.addOperatorsAndLinks(
      [
        { op: mockScanPredicate, pos: mockPoint },
        { op: mockResultPredicate, pos: mockPoint },
      ],
      []
    );
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID, mockResultPredicate.operatorID);

    // assert that multiple operators are highlighted
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockResultPredicate.operatorID);
    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockScanPredicate.operatorID);
    fixture.detectChanges();

    // expect that the property editor is cleared
    expect(component.frameComponentConfig).toBeUndefined();
  });

  it("should switch the content of property editor to another operator from the former operator correctly", () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add two operators
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);

    // highlight the first operator
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);
    fixture.detectChanges();

    // check the variables
    expect(component.frameComponentConfig?.component).toBe(OperatorPropertyEditFrameComponent);
    expect(component.frameComponentConfig?.componentInputs).toEqual({
      currentOperatorId: mockScanPredicate.operatorID,
    });

    // unhighlight the operator
    jointGraphWrapper.unhighlightOperators(mockScanPredicate.operatorID);
    fixture.detectChanges();

    expect(component.frameComponentConfig).toBeUndefined();

    // highlight the second operator
    jointGraphWrapper.highlightOperators(mockResultPredicate.operatorID);
    fixture.detectChanges();

    expect(component.frameComponentConfig?.component).toBe(OperatorPropertyEditFrameComponent);
    expect(component.frameComponentConfig?.componentInputs).toEqual({
      currentOperatorId: mockResultPredicate.operatorID,
    });
  });

  it("should clear and hide the property editor panel correctly upon unhighlighting a link", () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    workflowActionService.addLink(mockScanResultLink);

    // highlight the link
    jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
    fixture.detectChanges();

    expect(component.frameComponentConfig?.component).toBe(BreakpointPropertyEditFrameComponent);
    expect(component.frameComponentConfig?.componentInputs).toEqual({
      currentLinkId: mockScanResultLink.linkID,
    });

    // unhighlight the highlighted link
    jointGraphWrapper.unhighlightLink(mockScanResultLink.linkID);
    fixture.detectChanges();

    expect(component.frameComponentConfig).toBeUndefined();
  });

  it("should switch the content of property editor to another link-breakpoint from the former link-breakpoint correctly", () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    workflowActionService.addLink(mockScanSentimentLink);
    workflowActionService.addLink(mockSentimentResultLink);

    // highlight the first link
    jointGraphWrapper.highlightLink(mockScanSentimentLink.linkID);

    fixture.detectChanges();
    expect(component.frameComponentConfig?.component).toBe(BreakpointPropertyEditFrameComponent);
    expect(component.frameComponentConfig?.componentInputs).toEqual({
      currentLinkId: mockScanSentimentLink.linkID,
    });

    // unhighlight the link
    jointGraphWrapper.unhighlightLink(mockScanSentimentLink.linkID);
    fixture.detectChanges();
    expect(component.frameComponentConfig).toBeUndefined();

    // highlight the second link
    jointGraphWrapper.highlightLink(mockSentimentResultLink.linkID);
    fixture.detectChanges();
    expect(component.frameComponentConfig?.component).toBe(BreakpointPropertyEditFrameComponent);
    expect(component.frameComponentConfig?.componentInputs).toEqual({
      currentLinkId: mockSentimentResultLink.linkID,
    });
  });

  it("should switch the content of property editor to version display component correctly if the versions display is clicked and switch back when done", () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add one operator
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    // highlight the first operator
    jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);
    fixture.detectChanges();

    //the operator shall be highlighted
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().length).toBe(1);

    // the operator property editor should be displayed
    expect(component.frameComponentConfig?.component).toBe(OperatorPropertyEditFrameComponent);
    expect(component.frameComponentConfig?.componentInputs).toEqual({
      currentOperatorId: mockScanPredicate.operatorID,
    });

    // click on versions display
    workflowVersionService.clickDisplayWorkflowVersions();
    fixture.detectChanges();

    // all the elements shall be unhighlighted
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().length).toBe(0);
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs().length).toBe(0);
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs().length).toBe(0);

    // the component should switch to versions display
    expect(component.frameComponentConfig?.component).toBe(VersionsDisplayFrameComponent);

    // add one more operator
    workflowActionService.addOperator(mockResultPredicate, mockPoint);

    // highlight the new operator
    jointGraphWrapper.highlightOperators(mockResultPredicate.operatorID);
    fixture.detectChanges();

    // the operator property editor should be displayed
    expect(component.frameComponentConfig?.component).toBe(OperatorPropertyEditFrameComponent);
    expect(component.frameComponentConfig?.componentInputs).toEqual({
      currentOperatorId: mockResultPredicate.operatorID,
    });
  });
});
