import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { HttpClient, HttpHandler } from "@angular/common/http";
import { WorkflowAccessService } from "../../../../service/workflow-access/workflow-access.service";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { NgbdModalWorkflowShareAccessComponent } from "./ngbd-modal-workflow-share-access.component";
import { StubWorkflowAccessService } from "../../../../service/workflow-access/stub-workflow-access.service";
import { Workflow, WorkflowContent } from "../../../../../common/type/workflow";
import { jsonCast } from "../../../../../common/util/storage";

describe("NgbdModalShareAccessComponent", () => {
  let component: NgbdModalWorkflowShareAccessComponent;
  let fixture: ComponentFixture<NgbdModalWorkflowShareAccessComponent>;
  let service: StubWorkflowAccessService;

  const workflow: Workflow = {
    wid: 28,
    name: "project 1",
    description: "dummy description.",
    content: jsonCast<WorkflowContent>(
      " {\"operators\":[],\"operatorPositions\":{},\"links\":[],\"groups\":[],\"breakpoints\":{}}"
    ),
    creationTime: 1,
    lastModifiedTime: 2,
  };

  beforeEach(waitForAsync(async () => {
    TestBed.configureTestingModule({
      imports: [ReactiveFormsModule, FormsModule],
      declarations: [NgbdModalWorkflowShareAccessComponent],
      providers: [
        NgbActiveModal,
        HttpClient,
        HttpHandler,
        {
          provide: WorkflowAccessService,
          useClass: StubWorkflowAccessService,
        },
      ],
    });
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalWorkflowShareAccessComponent);
    component = fixture.componentInstance;
    service = TestBed.get(WorkflowAccessService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("form invalid when empty", () => {
    expect(component.shareForm.valid).toBeFalsy();
  });

  it("can get all accesses", () => {
    const mySpy = spyOn(service, "retrieveGrantedWorkflowAccessList").and.callThrough();
    component.workflow = workflow;
    fixture.detectChanges();
    component.onClickGetAllSharedAccess(component.workflow);
    expect(mySpy).toHaveBeenCalled();
    expect(component.allUserWorkflowAccess.length === 0).toBeTruthy();
  });

  it("can share accesses", () => {
    const mySpy = spyOn(service, "grantUserWorkflowAccess").and.callThrough();
    component.workflow = workflow;
    fixture.detectChanges();
    component.grantWorkflowAccess(component.workflow, "Jim", "read");
    expect(mySpy).toHaveBeenCalled();
  });

  it("can remove accesses", () => {
    const mySpy = spyOn(service, "revokeWorkflowAccess").and.callThrough();
    component.onClickRemoveAccess(workflow, "Jim");
    expect(mySpy).toHaveBeenCalled();
  });

  it("submitting a form", () => {
    const mySpy = spyOn(component, "onClickShareWorkflow");
    expect(component.shareForm.valid).toBeFalsy();
    component.shareForm.controls["username"].setValue("testguy");
    component.shareForm.controls["accessLevel"].setValue("read");
    expect(component.shareForm.valid).toBeTruthy();
    component.onClickShareWorkflow(workflow);
    expect(mySpy).toHaveBeenCalled();
  });
});
