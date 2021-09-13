import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";

import { MatDialogModule } from "@angular/material/dialog";

import { NgbActiveModal, NgbModule } from "@ng-bootstrap/ng-bootstrap";
import { FormsModule } from "@angular/forms";

import { HttpClientModule } from "@angular/common/http";

import { NgbdModalDeleteWorkflowComponent } from "./ngbd-modal-delete-workflow.component";
import { Workflow, WorkflowContent } from "../../../../../common/type/workflow";
import { jsonCast } from "../../../../../common/util/storage";

describe("NgbdModalDeleteProjectComponent", () => {
  let component: NgbdModalDeleteWorkflowComponent;
  let fixture: ComponentFixture<NgbdModalDeleteWorkflowComponent>;

  let deleteComponent: NgbdModalDeleteWorkflowComponent;
  let deleteFixture: ComponentFixture<NgbdModalDeleteWorkflowComponent>;
  const targetWorkflow: Workflow = {
    name: "workflow 1",
    wid: 4,
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 1,
    lastModifiedTime: 2,
  };

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [NgbdModalDeleteWorkflowComponent],
        providers: [NgbActiveModal],
        imports: [MatDialogModule, NgbModule, FormsModule, HttpClientModule],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalDeleteWorkflowComponent);
    component = fixture.componentInstance;
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("deleteProjectComponent deleteSavedProject return a result of true", () => {
    deleteFixture = TestBed.createComponent(NgbdModalDeleteWorkflowComponent);
    deleteComponent = deleteFixture.componentInstance;
    deleteComponent.workflow = targetWorkflow;

    spyOn(deleteComponent.activeModal, "close");
    deleteComponent.deleteSavedWorkflow();
    expect(deleteComponent.activeModal.close).toHaveBeenCalledWith(true);
  });
});
