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
import { ScrollingModule } from "@angular/cdk/scrolling";
import { NzAvatarModule } from "ng-zorro-antd/avatar";
import { NzToolTipModule } from "ng-zorro-antd/tooltip";
import { FileSaverService } from "../../service/user-file/file-saver.service";
import { testWorkflowEntries, testWorkflowFileNameConflictEntries } from "./user-workflow-test-fixtures";

describe("SavedWorkflowSectionComponent", () => {
  let component: UserWorkflowComponent;
  let fixture: ComponentFixture<UserWorkflowComponent>;

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
    fixture = TestBed.createComponent(UserWorkflowComponent);
    component = fixture.componentInstance;
    component.allDashboardWorkflowEntries = [...testWorkflowEntries];
    component.dashboardWorkflowEntries = [...testWorkflowEntries];
    component.masterFilterList = [];
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
    component.ascSort();
    const SortedCase = component.dashboardWorkflowEntries.map(item => item.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3", "workflow 4", "workflow 5"]);
  });

  it("alphaSortTest decreaseOrder", () => {
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
    component.dateSort();
    const SortedCase = component.dashboardWorkflowEntries.map(item => item.workflow.name);
    expect(SortedCase).toEqual(["workflow 4", "workflow 5", "workflow 2", "workflow 3", "workflow 1"]);
  });

  it("lastEditSortTest", () => {
    component.lastSort();
    const SortedCase = component.dashboardWorkflowEntries.map(item => item.workflow.name);
    expect(SortedCase).toEqual(["workflow 5", "workflow 4", "workflow 3", "workflow 2", "workflow 1"]);
  });

  it("searchNoInput", async () => {
    // When no search input is provided, it should show all workflows.
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3", "workflow 4", "workflow 5"]);
    expect(component.masterFilterList).toEqual([]);
  });

  it("searchByWorkflowName", async () => {
    // If the name "workflow 5" is entered as a single phrase, only workflow 5 should be returned, rather
    // than all containing the keyword "workflow".
    component.masterFilterList = ["workflow 5"];
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 5"]);
    expect(component.masterFilterList).toEqual(["workflow 5"]);
  });

  it("searchByOwners", async () => {
    // If the owner filter is applied, only those workflow ownered by that user should be returned.
    component.owners[0].checked = true;
    await component.updateSelectedOwners(); // calls searchWorkflow()
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2"]);
    expect(component.masterFilterList).toEqual(["owner: Texera"]);
  });

  it("searchByIDs", async () => {
    // If the ID filter is applied, only those workflows should be returned.
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
    component.userProjectsDropdown[0].checked = true;
    await component.updateSelectedProjects(); // calls searchWorkflow()
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.masterFilterList).toEqual(["project: Project1"]);
  });

  it("searchByCreationTime", async () => {
    // If the creation time filter is applied, only those workflows matching the date range should be returned.
    component.selectedCtime = [new Date(1970, 0, 3), new Date(1981, 2, 13)];
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 4", "workflow 5"]);
    expect(component.masterFilterList).toEqual(["ctime: 1970-01-03 ~ 1981-03-13"]);
  });

  it("searchByModifyTime", async () => {
    // If the modified time filter is applied, only those workflows matching the date range should be returned.
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
    const operatorGroup = component.operators.get("Analysis");
    if (operatorGroup) {
      operatorGroup[2].checked = true; // sentiment analysis
      await component.updateSelectedOperators();
    }
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.masterFilterList).toEqual(["operator: Sentiment Analysis"]); // userFriendlyName
  });

  it("searchByManyOperators", async () => {
    // If a multiple operator filters are provided, workflows containing any of the provided operators should be returned.
    const operatorGroup = component.operators.get("Analysis");
    const operatorGroup2 = component.operators.get("View Results");
    if (operatorGroup && operatorGroup2) {
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

  it("downloads checked files", async () => {
    // If multiple workflows in a single batch download have name conflicts, rename them as workflow-1, workflow-2, etc.
    component.dashboardWorkflowEntries = component.dashboardWorkflowEntries.concat(testWorkflowFileNameConflictEntries);
    testWorkflowFileNameConflictEntries[0].checked = true;
    testWorkflowFileNameConflictEntries[2].checked = true;
    await component.onClickOpenDownloadZip();
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledTimes(1);
  });
});
