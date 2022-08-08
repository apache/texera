import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { RouterTestingModule } from "@angular/router/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { SavedWorkflowSectionComponent } from "./saved-workflow-section.component";
import {
  WORKFLOW_BASE_URL,
  WorkflowPersistService,
} from "../../../../common/service/workflow-persist/workflow-persist.service";
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
import { NzCardModule } from "ng-zorro-antd/card";
import { NzListModule } from "ng-zorro-antd/list";
import { NzCalendarModule } from "ng-zorro-antd/calendar";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzPopoverModule } from "ng-zorro-antd/popover";
import { NzDatePickerModule } from "ng-zorro-antd/date-picker";
import { en_US, NZ_I18N } from "ng-zorro-antd/i18n";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";
import { mockOperatorMetaData } from "src/app/workspace/service/operator-metadata/mock-operator-metadata.data";
import { AppSettings } from "src/app/common/app-setting";

describe("SavedWorkflowSectionComponent", () => {
  let component: SavedWorkflowSectionComponent;
  let fixture: ComponentFixture<SavedWorkflowSectionComponent>;
  let modalService: NgbModal;

  let mockWorkflowPersistService: WorkflowPersistService;
  let mockOperatorMetadataService: OperatorMetadataService;
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
      projectIDs: [1],
    },
    {
      workflow: testWorkflow2,
      isOwner: true,
      ownerName: "Texera",
      accessLevel: "Write",
      projectIDs: [1, 2],
    },
    {
      workflow: testWorkflow3,
      isOwner: true,
      ownerName: "Angular",
      accessLevel: "Write",
      projectIDs: [1],
    },
    {
      workflow: testWorkflow4,
      isOwner: true,
      ownerName: "Angular",
      accessLevel: "Write",
      projectIDs: [3],
    },
    {
      workflow: testWorkflow5,
      isOwner: true,
      ownerName: "UCI",
      accessLevel: "Write",
      projectIDs: [3],
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
          { provide: NZ_I18N, useValue: en_US },
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
          NzCardModule,
          NzListModule,
          NzCalendarModule,
          NzDatePickerModule,
          NzSelectModule,
          NzPopoverModule,
          NoopAnimationsModule,
        ],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    httpClient = TestBed.get(HttpClient);
    httpTestingController = TestBed.get(HttpTestingController);
    fixture = TestBed.createComponent(SavedWorkflowSectionComponent);
    mockWorkflowPersistService = TestBed.inject(WorkflowPersistService);
    mockOperatorMetadataService = TestBed.inject(OperatorMetadataService);
    mockOperatorMetadataService.getOperatorMetadata().subscribe(opdata => {
      opdata.groups.forEach(group => {
        component.operators.set(
          group.groupName,
          opdata.operators
            .filter(operator => operator.additionalMetadata.operatorGroupName === group.groupName)
            .map(operator => {
              return {
                userFriendlyName: operator.additionalMetadata.userFriendlyName,
                operatorType: operator.operatorType,
                operatorGroup: operator.additionalMetadata.operatorGroupName,
                checked: false,
              };
            })
        );
      });
    });
    component = fixture.componentInstance;
    fixture.detectChanges();
    modalService = TestBed.get(NgbModal);
    spyOn(console, "log").and.callThrough();
    component.selectedDate = null;
    component.owners = [
      { userName: "Texera", checked: false },
      { userName: "Angular", checked: false },
      { userName: "UCI", checked: false },
    ];
    component.wids = [
      { id: "1", checked: false },
      { id: "2", checked: false },
      { id: "3", checked: false },
      { id: "4", checked: false },
      { id: "5", checked: false },
    ];
    component.userProjectsDropdown = [
      { pid: 1, name: "Project1", checked: false },
      { pid: 2, name: "Project2", checked: false },
      { pid: 3, name: "Project3", checked: false },
    ];
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

  // TODO: add this test case back and figure out why it failed
  xit("Modal Opened, then Closed", () => {
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

  it("searchNoInput", () => {
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3", "workflow 4", "workflow 5"]);
    expect(component.masterFilterList).toEqual([]);
  });

  it("searchByWorkflowName", () => {
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = ["5"];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 5"]);
    expect(component.masterFilterList).toEqual(["5"]);
  });

  it("searchByOwners", () => {
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    component.owners[0].checked = true;
    component.updateSelectedOwners(); // calls searchWorkflow()
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2"]);
    expect(component.masterFilterList).toEqual(["owner: Texera"]);
  });

  it("searchByIDs", () => {
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    component.wids[0].checked = true;
    component.wids[1].checked = true;
    component.wids[2].checked = true;
    component.updateSelectedIDs(); // calls searchWorkflow()
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.masterFilterList).toEqual(["id: 1", "id: 2", "id: 3"]);
  });

  it("searchByProjects", () => {
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    component.userProjectsDropdown[0].checked = true;
    component.updateSelectedProjects(); // calls searchWorkflow()
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.masterFilterList).toEqual(["project: Project1"]);
  });

  it("searchByCreationTime", () => {
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    component.selectedDate = new Date(1970, 0, 3);
    component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 4", "workflow 5"]);
    expect(component.masterFilterList).toEqual(["ctime: 1970-01-03"]);
  });

  /*
   * To add operators to this test:
   *   1. Check if the operator's group is true
   *   2. Mark the selected operator "checked" as true
   *   3. Push the operator's operatorType to operatorSelectionList
   *   4. Update masterFilterList to have the correct tags
   *
   *   - Recommendation: print out the component.operators after the operatorDropdownRequest is made
   *
   *   - See searchByManyOperators test
   */
  it("searchByOperators", () => {
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    const operatorDropdownRequest = httpTestingController.match("api/resources/operator-metadata");
    operatorDropdownRequest[0].flush(mockOperatorMetaData);
    const operatorGroup = component.operators.get("Analysis");
    if (operatorGroup) {
      const operatorSelectionList = []; // list of operators for query
      operatorGroup[2].checked = true; // sentiment analysis
      component.updateSelectedOperators(); // calls searchWorkflow()
      operatorSelectionList.push(operatorGroup[2].operatorType);
      const req = httpTestingController.match(`api/workflow/search-by-operators?operator=${operatorSelectionList}`);
      req[0].flush(["3"]);
    }
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 3"]);
    expect(component.masterFilterList).toEqual(["operator: Sentiment Analysis"]); // userFriendlyName
  });

  it("searchByManyOperators", () => {
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    const operatorDropdownRequest = httpTestingController.match("api/resources/operator-metadata");
    operatorDropdownRequest[0].flush(mockOperatorMetaData);
    const operatorGroup = component.operators.get("Analysis");
    const operatorGroup2 = component.operators.get("View Results");
    if (operatorGroup && operatorGroup2) {
      console.log(component.operators);
      const operatorSelectionList = []; // list of operators for query
      operatorGroup[2].checked = true; // sentiment analysis
      operatorGroup2[0].checked = true;
      component.updateSelectedOperators(); // calls searchWorkflow()
      operatorSelectionList.push(operatorGroup[2].operatorType);
      operatorSelectionList.push(operatorGroup2[0].operatorType);
      const req = httpTestingController.match(`api/workflow/search-by-operators?operator=${operatorSelectionList}`);
      req[0].flush(["1", "2", "3"]);
    }
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.masterFilterList).toEqual(["operator: Sentiment Analysis", "operator: View Results"]); // userFriendlyName
  });

  it("searchByManyParameters", () => {
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);

    const operatorDropdownRequest = httpTestingController.match("api/resources/operator-metadata");
    operatorDropdownRequest[0].flush(mockOperatorMetaData);
    const operatorGroup = component.operators.get("Analysis");
    if (operatorGroup) {
      const operatorSelectionList = []; // list of operators for query
      operatorGroup[2].checked = true; // sentiment analysis
      component.updateSelectedOperators();
      operatorSelectionList.push(operatorGroup[2].operatorType);

      component.owners[0].checked = true; //Texera
      component.owners[1].checked = true; //Angular
      (component.wids[0].checked = true), (component.wids[1].checked = true), (component.wids[2].checked = true); //id 1,2,3
      component.userProjectsDropdown[0].checked = true; //Project 1
      component.selectedDate = new Date(1970, 0, 1);
      component.masterFilterList.push("1");
      //add/select new search parameter here

      component.updateSelectedProjects(), component.updateSelectedIDs(), component.updateSelectedOwners();
      // if adding parameters, add its respective update function here

      const req = httpTestingController.match(`api/workflow/search-by-operators?operator=${operatorSelectionList}`);
      req[0].flush(["1", "2", "3"]);
      //triggers backend call
    }
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1"]);
    expect(component.masterFilterList).toEqual([
      "1",
      "owner: Texera",
      "owner: Angular",
      "id: 1",
      "id: 2",
      "id: 3",
      "operator: Sentiment Analysis",
      "project: Project1",
      "ctime: 1970-01-01",
    ]);
  });

  it("Sends http request to backend to retrieve export json", () => {
    component.onClickDownloadWorkfllow(testWorkflowEntries[0]);
    httpTestingController.match(`${AppSettings.getApiEndpoint()}/${WORKFLOW_BASE_URL}/${testWorkflowEntries[0]}`);
    httpTestingController.expectOne("api/workflow/1");
  });
});
