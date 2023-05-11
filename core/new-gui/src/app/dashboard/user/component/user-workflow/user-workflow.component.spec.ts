import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { RouterTestingModule } from "@angular/router/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { UserWorkflowComponent } from "./user-workflow.component";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { StubWorkflowPersistService } from "../../../../common/service/workflow-persist/stub-workflow-persist.service";
import { MatDividerModule } from "@angular/material/divider";
import { MatListModule } from "@angular/material/list";
import { MatCardModule } from "@angular/material/card";
import { MatDialogModule } from "@angular/material/dialog";
import { NgbActiveModal, NgbModule } from "@ng-bootstrap/ng-bootstrap";
import { ShareAccessComponent } from "../share-access/share-access.component";
import { Workflow, WorkflowContent } from "../../../../common/type/workflow";
import { jsonCast } from "../../../../common/util/storage";
import { HttpClient } from "@angular/common/http";
import { ShareAccessService } from "../../service/share-access/share-access.service";
import { DashboardWorkflowEntry } from "../../type/dashboard-workflow-entry";
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
import { StubOperatorMetadataService } from "src/app/workspace/service/operator-metadata/stub-operator-metadata.service";
import { NzUploadModule } from "ng-zorro-antd/upload";
import { By } from "@angular/platform-browser";
import { ScrollingModule } from "@angular/cdk/scrolling";
import { NzAvatarModule } from "ng-zorro-antd/avatar";
import { NzToolTipModule } from "ng-zorro-antd/tooltip";
import { FileSaverService } from "../../service/user-file/file-saver.service";

describe("SavedWorkflowSectionComponent", () => {
  let component: UserWorkflowComponent;
  let fixture: ComponentFixture<UserWorkflowComponent>;

  let httpTestingController: HttpTestingController;
  //All times in test Workflows are in PST because our local machine's timezone is PST
  //the Date class creates unix timestamp based on local timezone, therefore test workflow time needs to be in local timezone

  const testWorkflow1: Workflow = {
    wid: 1,
    name: "workflow 1",
    description: "dummy description",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 28800000, //28800000 is 1970-01-01 in PST
    lastModifiedTime: 28800000 + 2,
  };
  const testWorkflow2: Workflow = {
    wid: 2,
    name: "workflow 2",
    description: "dummy description",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 28800000 + (86400000 + 3), // 86400000 is the number of milliseconds in a day
    lastModifiedTime: 28800000 + (86400000 + 3),
  };
  const testWorkflow3: Workflow = {
    wid: 3,
    name: "workflow 3",
    description: "dummy description",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 28800000 + 86400000,
    lastModifiedTime: 28800000 + (86400000 + 4),
  };
  const testWorkflow4: Workflow = {
    wid: 4,
    name: "workflow 4",
    description: "dummy description",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 28800000 + 86400003 * 2,
    lastModifiedTime: 28800000 + 86400000 * 2 + 6,
  };
  const testWorkflow5: Workflow = {
    wid: 5,
    name: "workflow 5",
    description: "dummy description",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 28800000 + 86400000 * 2,
    lastModifiedTime: 28800000 + 86400000 * 2 + 8,
  };

  const testDownloadWorkflow1: Workflow = {
    wid: 6,
    name: "workflow",
    description: "dummy description",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 28800000, //28800000 is 1970-01-01 in PST
    lastModifiedTime: 28800000 + 2,
  };
  const testDownloadWorkflow2: Workflow = {
    wid: 7,
    name: "workflow",
    description: "dummy description",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 28800000 + (86400000 + 3), // 86400000 is the number of milliseconds in a day
    lastModifiedTime: 28800000 + (86400000 + 3),
  };
  const testDownloadWorkflow3: Workflow = {
    wid: 8,
    name: "workflow",
    description: "dummy description",
    content: jsonCast<WorkflowContent>("{}"),
    creationTime: 28800000 + 86400000,
    lastModifiedTime: 28800000 + (86400000 + 4),
  };
  const testWorkflowFileNameConflictEntries: DashboardWorkflowEntry[] = [
    {
      workflow: testDownloadWorkflow1,
      isOwner: true,
      ownerName: "Texera",
      accessLevel: "Write",
      projectIDs: [1],
    },
    {
      workflow: testDownloadWorkflow2,
      isOwner: true,
      ownerName: "Texera",
      accessLevel: "Write",
      projectIDs: [1, 2],
    },
    {
      workflow: testDownloadWorkflow3,
      isOwner: true,
      ownerName: "Angular",
      accessLevel: "Write",
      projectIDs: [1],
    },
  ];

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

  const checked = <Event>(<any>{
    target: {
      checked: true,
    },
  });
  const unchecked = <Event>(<any>{
    target: {
      checked: false,
    },
  });

  const fileSaverServiceSpy = jasmine.createSpyObj<FileSaverService>(["saveAs"]);

  // must use waitForAsync for configureTestingModule in components with virtual scroll
  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [UserWorkflowComponent, ShareAccessComponent],
      providers: [
        { provide: WorkflowPersistService, useValue: new StubWorkflowPersistService(testWorkflowEntries) },
        NgbActiveModal,
        HttpClient,
        NgbActiveModal,
        ShareAccessService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: UserService, useClass: StubUserService },
        { provide: NZ_I18N, useValue: en_US },
        { provide: FileSaverService, useValue: fileSaverServiceSpy },
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
        NzAvatarModule,
        NzToolTipModule,
        NzUploadModule,
        ScrollingModule,
        NoopAnimationsModule,
      ],
    }).compileComponents();
  }));

  beforeEach(() => {
    httpTestingController = TestBed.get(HttpTestingController);
    fixture = TestBed.createComponent(UserWorkflowComponent);

    component = fixture.componentInstance;
    component.selectedMtime = [];
    component.selectedMtime = [];
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

    fixture.detectChanges();
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
  // xit("Modal Opened, then Closed", () => {
  //   const modalRef: NgbModalRef = modalService.open(NgbdModalWorkflowShareAccessComponent);
  //   spyOn(modalService, "open").and.returnValue(modalRef);
  //   component.onClickOpenShareAccess(testWorkflowEntries[0]);
  //   expect(modalService.open).toHaveBeenCalled();
  //   fixture.detectChanges();
  //   modalRef.dismiss();
  // });

  it("createDateSortTest", () => {
    component.dashboardWorkflowEntries = [];
    component.dashboardWorkflowEntries = component.dashboardWorkflowEntries.concat(testWorkflowEntries);
    component.dateSort();
    const SortedCase = component.dashboardWorkflowEntries.map(item => item.workflow.name);
    expect(SortedCase).toEqual(["workflow 4", "workflow 5", "workflow 2", "workflow 3", "workflow 1"]);
  });

  it("lastEditSortTest", () => {
    component.dashboardWorkflowEntries = [];
    component.dashboardWorkflowEntries = component.dashboardWorkflowEntries.concat(testWorkflowEntries);
    component.lastSort();
    const SortedCase = component.dashboardWorkflowEntries.map(item => item.workflow.name);
    expect(SortedCase).toEqual(["workflow 5", "workflow 4", "workflow 3", "workflow 2", "workflow 1"]);
  });

  it("searchNoInput", async () => {
    // When no search input is provided, it should show all workflows.
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3", "workflow 4", "workflow 5"]);
    expect(component.masterFilterList).toEqual([]);
  });

  it("searchByWorkflowName", async () => {
    // If the name "workflow 5" is entered as a single phrase, only workflow 5 should be returned, rather
    // than all containing the keyword "workflow".
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = ["workflow 5"];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 5"]);
    expect(component.masterFilterList).toEqual(["workflow 5"]);
  });

  it("searchByOwners", async () => {
    // If the owner filter is applied, only those workflow ownered by that user should be returned.
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    component.owners[0].checked = true;
    await component.updateSelectedOwners(); // calls searchWorkflow()
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2"]);
    expect(component.masterFilterList).toEqual(["owner: Texera"]);
  });

  it("searchByIDs", async () => {
    // If the ID filter is applied, only those workflows should be returned.
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    component.wids[0].checked = true;
    component.wids[1].checked = true;
    component.wids[2].checked = true;
    await component.updateSelectedIDs(); // calls searchWorkflow()
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.masterFilterList).toEqual(["id: 1", "id: 2", "id: 3"]);
  });

  it("searchByProjects", async () => {
    // If the project filter is applied, only those workflows belonging to those projects should be returned.
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    component.userProjectsDropdown[0].checked = true;
    await component.updateSelectedProjects(); // calls searchWorkflow()
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.masterFilterList).toEqual(["project: Project1"]);
  });

  it("searchByCreationTime", async () => {
    // If the creation time filter is applied, only those workflows matching the date range should be returned.
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    component.selectedCtime = [new Date(1970, 0, 3), new Date(1981, 2, 13)];
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 4", "workflow 5"]);
    expect(component.masterFilterList).toEqual(["ctime: 1970-01-03 ~ 1981-03-13"]);
  });

  it("searchByModifyTime", async () => {
    // If the modified time filter is applied, only those workflows matching the date range should be returned.
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    component.selectedMtime = [new Date(1970, 0, 3), new Date(1981, 2, 13)];
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 4", "workflow 5"]);
    expect(component.masterFilterList).toEqual(["mtime: 1970-01-03 ~ 1981-03-13"]);
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
  it("searchByOperators", async () => {
    // If a single operator filter is provided, only the workflows containing that operator should be returned.
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    const operatorGroup = component.operators.get("Analysis");
    if (operatorGroup) {
      operatorGroup[2].checked = true; // sentiment analysis
      await component.updateSelectedOperators();
    }
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 3"]);
    expect(component.masterFilterList).toEqual(["operator: Sentiment Analysis"]); // userFriendlyName
  });

  it("searchByManyOperators", async () => {
    // If a multiple operator filters are provided, workflows containing any of the provided operators should be returned.
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);
    const operatorGroup = component.operators.get("Analysis");
    const operatorGroup2 = component.operators.get("View Results");
    if (operatorGroup && operatorGroup2) {
      console.log(component.operators);
      operatorGroup[2].checked = true; // sentiment analysis
      operatorGroup2[0].checked = true;
      await component.updateSelectedOperators(); // calls searchWorkflow()
    }
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.masterFilterList).toEqual(["operator: Sentiment Analysis", "operator: View Results"]); // userFriendlyName
  });

  it("searchByManyParameters", async () => {
    // Apply the project, ID, owner, and operator filter all at once.
    component.dashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = [];
    component.allDashboardWorkflowEntries = component.allDashboardWorkflowEntries.concat(testWorkflowEntries);
    component.masterFilterList = [];
    component.fuse.setCollection(component.allDashboardWorkflowEntries);

    const operatorGroup = component.operators.get("Analysis");
    if (operatorGroup) {
      operatorGroup[3].checked = true; // Aggregation operator
      await component.updateSelectedOperators();

      component.owners[0].checked = true; //Texera
      component.owners[1].checked = true; //Angular
      component.wids[0].checked = true;
      component.wids[1].checked = true;
      component.wids[2].checked = true; //id 1,2,3
      component.userProjectsDropdown[0].checked = true; //Project 1
      component.selectedCtime = [new Date(1970, 0, 1), new Date(1973, 2, 11)];
      component.selectedMtime = [new Date(1970, 0, 1), new Date(1982, 3, 14)];
      component.masterFilterList.push("1");
      //add/select new search parameter here

      await component.updateSelectedProjects();
      await component.updateSelectedIDs();
      await component.updateSelectedOwners();
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
      "operator: Aggregation",
      "project: Project1",
      "ctime: 1970-01-01 ~ 1973-03-11",
      "mtime: 1970-01-01 ~ 1982-04-14",
    ]);
  });

  it("sends http request to backend to retrieve export json", () => {
    // Test the workflow download button.
    component.onClickDownloadWorkfllow(testWorkflowEntries[0]);
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledOnceWith(
      new Blob([JSON.stringify(testWorkflowEntries[0].workflow.content)], { type: "text/plain;charset=utf-8" }),
      "workflow 1.json"
    );
  });

  it("adds selected workflow to the downloadListWorkflow", () => {
    // Test workflow download with multiple workflows selected.
    component.dashboardWorkflowEntries = [];
    component.dashboardWorkflowEntries = component.dashboardWorkflowEntries.concat(testWorkflowEntries);
    component.onClickAddToDownload(testWorkflowEntries[0], checked);
    expect(component.downloadListWorkflow.has(<number>testWorkflowEntries[0].workflow.wid)).toEqual(true);
    component.onClickAddToDownload(testWorkflowEntries[3], checked);
    expect(component.downloadListWorkflow.has(<number>testWorkflowEntries[3].workflow.wid)).toEqual(true);
  });

  it("remove unchecked workflow from the downloadListWorkflow", () => {
    // Allow removal of items in the pending download list.
    component.dashboardWorkflowEntries = [];
    component.dashboardWorkflowEntries = component.dashboardWorkflowEntries.concat(testWorkflowEntries);
    component.onClickAddToDownload(testWorkflowEntries[0], checked);
    component.onClickAddToDownload(testWorkflowEntries[3], checked);
    component.onClickAddToDownload(testWorkflowEntries[0], unchecked);
    expect(component.downloadListWorkflow.has(<number>testWorkflowEntries[0].workflow.wid)).toEqual(false);
    component.onClickAddToDownload(testWorkflowEntries[3], unchecked);
    expect(component.downloadListWorkflow.has(<number>testWorkflowEntries[3].workflow.wid)).toEqual(false);
  });

  it("detects conflict filename and resolves it", () => {
    // If multiple workflows in a single batch download have name conflicts, rename them as workflow-1, workflow-2, etc.
    component.dashboardWorkflowEntries = [];
    component.dashboardWorkflowEntries = component.dashboardWorkflowEntries.concat(testWorkflowFileNameConflictEntries);
    component.onClickAddToDownload(testWorkflowFileNameConflictEntries[0], checked);
    component.onClickAddToDownload(testWorkflowFileNameConflictEntries[2], checked);
    let index = testWorkflowFileNameConflictEntries[0].workflow.wid as number;
    let index1 = testWorkflowFileNameConflictEntries[1].workflow.wid as number;
    let index2 = testWorkflowFileNameConflictEntries[2].workflow.wid as number;
    expect(component.downloadListWorkflow.get(index)).toEqual("workflow.json");
    expect(component.downloadListWorkflow.get(index2)).toEqual("workflow-1.json");
    component.onClickAddToDownload(testWorkflowFileNameConflictEntries[0], unchecked);
    component.onClickAddToDownload(testWorkflowFileNameConflictEntries[1], checked);
    expect(component.downloadListWorkflow.get(index1)).toEqual("workflow.json");
    expect(component.downloadListWorkflow.get(index2)).toEqual("workflow-1.json");
    component.onClickAddToDownload(testWorkflowFileNameConflictEntries[0], checked);
    expect(component.downloadListWorkflow.get(index1)).toEqual("workflow.json");
    expect(component.downloadListWorkflow.get(index2)).toEqual("workflow-1.json");
    expect(component.downloadListWorkflow.get(index)).toEqual("workflow-2.json");
  });

  it("adding a workflow description adds a description to the workflow", waitForAsync(() => {
    fixture.whenStable().then(() => {
      let addWorkflowDescriptionBtn1 = fixture.debugElement.query(By.css(".add-description-btn"));
      expect(addWorkflowDescriptionBtn1).toBeFalsy();
      // add some test workflows
      component.dashboardWorkflowEntries = testWorkflowEntries;
      fixture.detectChanges();
      let addWorkflowDescriptionBtn2 = fixture.debugElement.query(By.css(".add-description-btn"));
      // the button for adding workflow descriptions should appear now
      expect(addWorkflowDescriptionBtn2).toBeTruthy();
      addWorkflowDescriptionBtn2.triggerEventHandler("click", null);
      fixture.detectChanges();
      let editableDescriptionInput1 = fixture.debugElement.nativeElement.querySelector(
        ".workflow-editable-description"
      );
      expect(editableDescriptionInput1).toBeTruthy();

      spyOn(component, "confirmUpdateWorkflowCustomDescription");
      sendInput(editableDescriptionInput1, "dummy description added by focusing out the input element.").then(() => {
        fixture.detectChanges();
        editableDescriptionInput1.dispatchEvent(new Event("focusout"));
        fixture.detectChanges();
        expect(component.confirmUpdateWorkflowCustomDescription).toHaveBeenCalledTimes(1);
      });
    });
  }));

  it("Editing a workflow description edits a description to the workflow", waitForAsync(() => {
    fixture.whenStable().then(() => {
      let workflowDescriptionLabel1 = fixture.debugElement.query(By.css(".workflow-description"));
      expect(workflowDescriptionLabel1).toBeFalsy();
      // add some test workflows
      component.dashboardWorkflowEntries = testWorkflowEntries;
      fixture.detectChanges();
      let workflowDescriptionLabel2 = fixture.debugElement.query(By.css(".workflow-description"));
      // the workflow description label should appear now
      expect(workflowDescriptionLabel2).toBeTruthy();
      workflowDescriptionLabel2.triggerEventHandler("click", null);
      fixture.detectChanges();
      let editableDescriptionInput1 = fixture.debugElement.nativeElement.querySelector(
        ".workflow-editable-description"
      );
      expect(editableDescriptionInput1).toBeTruthy();

      spyOn(component, "confirmUpdateWorkflowCustomDescription");

      sendInput(editableDescriptionInput1, "dummy description added by focusing out the input element.").then(() => {
        fixture.detectChanges();
        editableDescriptionInput1.dispatchEvent(new Event("focusout"));
        fixture.detectChanges();
        expect(component.confirmUpdateWorkflowCustomDescription).toHaveBeenCalledTimes(1);
      });
    });
  }));

  function sendInput(editableDescriptionInput: HTMLInputElement, text: string) {
    // Helper function to change the workflow description textbox.
    editableDescriptionInput.value = text;
    editableDescriptionInput.dispatchEvent(new Event("input"));
    fixture.detectChanges();
    return fixture.whenStable();
  }
});
