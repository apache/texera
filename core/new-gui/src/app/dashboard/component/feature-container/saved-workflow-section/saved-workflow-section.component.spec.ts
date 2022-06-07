import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { RouterTestingModule } from "@angular/router/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { SavedWorkflowSectionComponent } from "./saved-workflow-section.component";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { MatDividerModule } from "@angular/material/divider";
import { MatListModule } from "@angular/material/list";
import { MatCardModule } from "@angular/material/card";
import { MatDialogModule } from "@angular/material/dialog";
import { NgbActiveModal, NgbModal, NgbModalRef, NgbModule } from "@ng-bootstrap/ng-bootstrap";
import { NgbdModalWorkflowShareAccessComponent } from "./ngbd-modal-share-access/ngbd-modal-workflow-share-access.component";
import { Workflow, WorkflowContent } from "../../../../common/type/workflow";
import { jsonCast } from "../../../../common/util/storage";
import { HttpClient } from "@angular/common/http";
import { WorkflowAccessService } from "../../../service/workflow-access/workflow-access.service";
import { DashboardWorkflowEntry } from "../../../type/dashboard-workflow-entry";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";

describe("SavedWorkflowSectionComponent", () => {
  let component: SavedWorkflowSectionComponent;
  let fixture: ComponentFixture<SavedWorkflowSectionComponent>;
  let modalService: NgbModal;

  let mockWorkflowPersistService: WorkflowPersistService;
  let httpClient: HttpClient;
  let httpTestingController: HttpTestingController;

  const testWorkflow1: Workflow = {
    wid: 1,
    name: "workflow 1",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 1,
    lastModifiedTime: 2,
  };
  const testWorkflow2: Workflow = {
    wid: 2,
    name: "workflow 2",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 3,
    lastModifiedTime: 4,
  };
  const testWorkflow3: Workflow = {
    wid: 3,
    name: "workflow 3",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 3,
    lastModifiedTime: 3,
  };
  const testWorkflow4: Workflow = {
    wid: 4,
    name: "workflow 4",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 4,
    lastModifiedTime: 6,
  };
  const testWorkflow5: Workflow = {
    wid: 5,
    name: "workflow 5",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 3,
    lastModifiedTime: 8,
  };
  const testWorkflowEntries: DashboardWorkflowEntry[] = [
    {
      workflow: testWorkflow1,
      isOwner: true,
      ownerName: "Texera",
      accessLevel: "Write",
      projectIDs: [],
    },
    {
      workflow: testWorkflow2,
      isOwner: true,
      ownerName: "Texera",
      accessLevel: "Write",
      projectIDs: [],
    },
    {
      workflow: testWorkflow3,
      isOwner: true,
      ownerName: "Texera",
      accessLevel: "Write",
      projectIDs: [],
    },
    {
      workflow: testWorkflow4,
      isOwner: true,
      ownerName: "Texera",
      accessLevel: "Write",
      projectIDs: [],
    },
    {
      workflow: testWorkflow5,
      isOwner: true,
      ownerName: "Texera",
      accessLevel: "Write",
      projectIDs: [],
    },
  ];

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [SavedWorkflowSectionComponent, NgbdModalWorkflowShareAccessComponent],
        providers: [
          WorkflowPersistService,
          NgbActiveModal,
          HttpClient,
          NgbActiveModal,
          WorkflowAccessService,
          { provide: UserService, useClass: StubUserService },
        ],
        imports: [
          MatDividerModule,
          MatListModule,
          MatCardModule,
          MatDialogModule,
          NgbModule,
          FormsModule,
          RouterTestingModule,
          HttpClientTestingModule,
          ReactiveFormsModule,
          NzDropDownModule,
        ],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    httpClient = TestBed.get(HttpClient);
    httpTestingController = TestBed.get(HttpTestingController);
    fixture = TestBed.createComponent(SavedWorkflowSectionComponent);
    mockWorkflowPersistService = TestBed.inject(WorkflowPersistService);
    component = fixture.componentInstance;
    fixture.detectChanges();
    modalService = TestBed.get(NgbModal);
    spyOn(console, "log").and.callThrough();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
    expect(mockWorkflowPersistService).toBeTruthy();
  });

  it("alphaSortTest increaseOrder", () => {
    component.dashboardWorkflowEntries = [];
    component.dashboardWorkflowEntries = component.dashboardWorkflowEntries.concat(testWorkflowEntries);
    component.ascSort();
    const SortedCase = component.dashboardWorkflowEntries.map(item => item.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3", "workflow 4", "workflow 5"]);
  });

  it("alphaSortTest decreaseOrder", () => {
    component.dashboardWorkflowEntries = [];
    component.dashboardWorkflowEntries = component.dashboardWorkflowEntries.concat(testWorkflowEntries);
    component.dscSort();
    const SortedCase = component.dashboardWorkflowEntries.map(item => item.workflow.name);
    expect(SortedCase).toEqual(["workflow 5", "workflow 4", "workflow 3", "workflow 2", "workflow 1"]);
  });

  it("Modal Opened, then Closed", () => {
    const modalRef: NgbModalRef = modalService.open(NgbdModalWorkflowShareAccessComponent);
    spyOn(modalService, "open").and.returnValue(modalRef);
    component.onClickOpenShareAccess(testWorkflowEntries[0]);
    expect(modalService.open).toHaveBeenCalled();
    fixture.detectChanges();
    modalRef.dismiss();
  });

  it("createDateSortTest", () => {
    component.dashboardWorkflowEntries = [];
    component.dashboardWorkflowEntries = component.dashboardWorkflowEntries.concat(testWorkflowEntries);
    component.dateSort();
    const SortedCase = component.dashboardWorkflowEntries.map(item => item.workflow.creationTime);
    expect(SortedCase).toEqual([1, 3, 3, 3, 4]);
  });

  it("lastEditSortTest", () => {
    component.dashboardWorkflowEntries = [];
    component.dashboardWorkflowEntries = component.dashboardWorkflowEntries.concat(testWorkflowEntries);
    component.lastSort();
    const SortedCase = component.dashboardWorkflowEntries.map(item => item.workflow.lastModifiedTime);
    expect(SortedCase).toEqual([2, 3, 4, 6, 8]);
  });
});
