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
import Fuse from "fuse.js";

describe("SavedWorkflowSectionComponent", () => {
  let component: SavedWorkflowSectionComponent;
  let fixture: ComponentFixture<SavedWorkflowSectionComponent>;
  let modalService: NgbModal;

  let mockWorkflowPersistService: WorkflowPersistService;
  let httpClient: HttpClient;
  let httpTestingController: HttpTestingController;

  //All times in test Workflows are in PST because our local machine's timezone is PST
  //the Date class creates unix timestamp based on local timezone, therefore test workflow time needs to be in local timezone

  const testWorkflow1: Workflow = {
    wid: 1,
    name: "workflow 1",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 28800000, //28800000 is 1970-01-01 in PST
    lastModifiedTime: 28800000 + 2,
  };
  const testWorkflow2: Workflow = {
    wid: 2,
    name: "workflow 2",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 28800000 + (86400000 + 3), // 86400000 is the number of milliseconds in a day
    lastModifiedTime: 28800000 + (86400000 + 3),
  };
  const testWorkflow3: Workflow = {
    wid: 3,
    name: "workflow 3",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 28800000 + 86400000,
    lastModifiedTime: 28800000 + (86400000 + 3),
  };
  const testWorkflow4: Workflow = {
    wid: 4,
    name: "workflow 4",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 28800000 + 86400003 * 2,
    lastModifiedTime: 28800000 + 86400000 * 2 + 6,
  };
  const testWorkflow5: Workflow = {
    wid: 5,
    name: "workflow 5",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 28800000 + 86400000 * 2,
    lastModifiedTime: 28800000 + 86400000 * 2 + 8,
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
      ownerName: "Angular",
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
    const SortedCase = component.dashboardWorkflowEntries.map(item => item.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 3", "workflow 2", "workflow 5", "workflow 4"]);
  });

  it("lastEditSortTest", () => {
    component.dashboardWorkflowEntries = [];
    component.dashboardWorkflowEntries = component.dashboardWorkflowEntries.concat(testWorkflowEntries);
    component.lastSort();
    const SortedCase = component.dashboardWorkflowEntries.map(item => item.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3", "workflow 4", "workflow 5"]);
  });

  //OLD TEST CASES for searching based on input bar string

  // it("searchNoInput", () => {
  //   component.dashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
  //   component.workflowSearchValue = "";
  //   component.fuse.setCollection(component.allDashboardWorkflowEntries);
  //   component.searchWorkflow();
  //   const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
  //   expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3", "workflow 4", "workflow 5"]);
  // });

  // it("searchWorkflow", () => {
  //   component.dashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
  //   component.workflowSearchValue = "1";
  //   component.fuse.setCollection(component.allDashboardWorkflowEntries);
  //   component.searchWorkflow();
  //   const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
  //   expect(SortedCase).toEqual(["workflow 1"]);
  // });

  // it("searchCreationTime", () => {
  //   component.dashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
  //   component.workflowSearchValue = "ctime:1970-01-02";
  //   component.fuse.setCollection(component.allDashboardWorkflowEntries);
  //   component.searchWorkflow();
  //   const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
  //   expect(SortedCase).toEqual(["workflow 2", "workflow 3"]);
  // });

  // it("searchCreationTime (<)", () => {
  //   component.dashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
  //   component.workflowSearchValue = "ctime:<1970-01-02";
  //   component.fuse.setCollection(component.allDashboardWorkflowEntries);
  //   component.searchWorkflow();
  //   const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
  //   expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
  // });

  // it("searchCreationTime (>)", () => {
  //   component.dashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
  //   component.workflowSearchValue = "ctime:>1970-01-02";
  //   component.fuse.setCollection(component.allDashboardWorkflowEntries);
  //   component.searchWorkflow();
  //   const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
  //   expect(SortedCase).toEqual(["workflow 2", "workflow 3", "workflow 4", "workflow 5"]);
  // });

  // it("searchCreationTimeWrongFormat", () => {
  //   component.dashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
  //   component.dashboardWorkflowEntries = component.allDashboardWorkflowEntries; //for if workflows are already displayed, they will remain displayed
  //   component.workflowSearchValue = "ctime:191090";
  //   component.fuse.setCollection(component.allDashboardWorkflowEntries);
  //   component.searchWorkflow();
  //   const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
  //   expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3", "workflow 4", "workflow 5"]);
  // });

  // it("searchOwner", () => {
  //   component.dashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
  //   component.workflowSearchValue = "owner:Angular";
  //   component.fuse.setCollection(component.allDashboardWorkflowEntries);
  //   component.searchWorkflow();
  //   const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
  //   expect(SortedCase).toEqual(["workflow 4"]);
  // });

  // it("searchID", () => {
  //   component.dashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
  //   component.workflowSearchValue = "id:1";
  //   component.fuse.setCollection(component.allDashboardWorkflowEntries);
  //   component.searchWorkflow();
  //   const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
  //   expect(SortedCase).toEqual(["workflow 1"]);
  // });

  // it("searchCombo", () => {
  //   component.dashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = [];
  //   component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
  //   component.workflowSearchValue = "workflow ctime:1970-01-03 owner:Texera id:5";
  //   component.fuse.setCollection(component.allDashboardWorkflowEntries);
  //   component.searchWorkflow();
  //   const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
  //   expect(SortedCase).toEqual(["workflow 5"]);
  // });
});
