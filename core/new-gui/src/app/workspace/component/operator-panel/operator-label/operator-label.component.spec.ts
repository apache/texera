import { WorkflowUtilService } from "./../../../service/workflow-graph/util/workflow-util.service";
import { JointUIService } from "./../../../service/joint-ui/joint-ui.service";
import { DragDropService } from "./../../../service/drag-drop/drag-drop.service";
import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { OperatorLabelComponent } from "./operator-label.component";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";

import * as jQuery from "jquery";

import { CustomNgMaterialModule } from "../../../../common/custom-ng-material.module";
import { mockScanSourceSchema } from "../../../service/operator-metadata/mock-operator-metadata.data";
import { By } from "@angular/platform-browser";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../../../service/undo-redo/undo-redo.service";
import { RouterTestingModule } from "@angular/router/testing";
import { NgbModule } from "@ng-bootstrap/ng-bootstrap";

describe("OperatorLabelComponent", () => {
  const mockOperatorData = mockScanSourceSchema;
  let component: OperatorLabelComponent;
  let fixture: ComponentFixture<OperatorLabelComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [OperatorLabelComponent],
        imports: [CustomNgMaterialModule, RouterTestingModule.withRoutes([]), NgbModule],
        providers: [
          DragDropService,
          JointUIService,
          WorkflowUtilService,
          WorkflowActionService,
          UndoRedoService,
          {
            provide: OperatorMetadataService,
            useClass: StubOperatorMetadataService,
          },
        ],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(OperatorLabelComponent);
    component = fixture.componentInstance;

    // use one mock operator schema as input to construct the operator label
    component.operator = mockOperatorData;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("should generate an ID for the component DOM element", () => {
    expect(component.operatorLabelID).toContain("texera-operator-label-");
  });

  it("should display operator user friendly name on the UI", () => {
    const element = <HTMLElement>fixture.debugElement.query(By.css(".texera-operator-label-body")).nativeElement;
    expect(element.firstChild?.textContent?.trim()).toEqual(mockOperatorData.additionalMetadata.userFriendlyName);
  });

  it("should register itself as a draggable element", () => {
    const jqueryElement = jQuery(`#${component.operatorLabelID}`);
    expect(jqueryElement.data("uiDraggable")).toBeTruthy();
  });

  it("should call the mouseLeave function once the cursor leaves a operator label", () => {
    const spy = spyOn<any>(component, "mouseLeave");
    const operatorLabelElement = fixture.debugElement.query(By.css("#" + component.operatorLabelID));
    operatorLabelElement.triggerEventHandler("mouseleave", component);
    expect(spy).toHaveBeenCalled();
  });

  it("should call the mouseDown function once the cursor clicks the operator label", () => {
    const spy = spyOn<any>(component, "mouseDown");
    const operatorLabelElement = fixture.debugElement.query(By.css("#" + component.operatorLabelID));
    operatorLabelElement.triggerEventHandler("mousedown", component);
    expect(spy).toHaveBeenCalled();
  });

  it("should call the mouseUp function once the cursor un-clicks operator label", () => {
    const spy = spyOn<any>(component, "mouseUp");
    const operatorLabelElement = fixture.debugElement.query(By.css("#" + component.operatorLabelID));
    operatorLabelElement.triggerEventHandler("mouseup", component);
    expect(spy).toHaveBeenCalled();
  });

  // TODO: simulate drag and drop in tests, possibly using jQueryUI Simulate plugin
  //  https://github.com/j-ulrich/jquery-simulate-ext/blob/master/doc/drag-n-drop.md
});
