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
import { FiltersComponent } from "../filters/filters.component";
import { delay } from "rxjs";
import { UserWorkflowListItemComponent } from "./user-workflow-list-item/user-workflow-list-item.component";
import { UserProjectService } from "../../service/user-project/user-project.service";
import { StubUserProjectService } from "../../service/user-project/stub-user-project.service";

describe("SavedWorkflowSectionComponent", () => {
  let component: UserWorkflowComponent;
  let fixture: ComponentFixture<UserWorkflowComponent>;

  const fileSaverServiceSpy = jasmine.createSpyObj<FileSaverService>(["saveAs"]);

  // must use waitForAsync for configureTestingModule in components with virtual scroll
  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [UserWorkflowComponent, ShareAccessComponent, FiltersComponent, UserWorkflowListItemComponent],
      providers: [
        { provide: WorkflowPersistService, useValue: new StubWorkflowPersistService(testWorkflowEntries) },
        { provide: UserProjectService, useValue: new StubUserProjectService() },
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
    component.filters = TestBed.createComponent(FiltersComponent).componentInstance;
    component.allDashboardWorkflowEntries = [...testWorkflowEntries];
    component.dashboardWorkflowEntries = [...testWorkflowEntries];
    component.filters.masterFilterList = [];
    component.filters.selectedMtime = [];
    component.filters.selectedMtime = [];
    fixture.detectChanges();
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

  it("searchNoInput", async () => {
    // When no search input is provided, it should show all workflows.
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3", "workflow 4", "workflow 5"]);
    expect(component.filters.masterFilterList).toEqual([]);
  });

  it("searchByWorkflowName", async () => {
    // If the name "workflow 5" is entered as a single phrase, only workflow 5 should be returned, rather
    // than all containing the keyword "workflow".
    component.filters.masterFilterList = ["workflow 5"];
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 5"]);
    expect(component.filters.masterFilterList).toEqual(["workflow 5"]);
  });

  it("searchByOwners", async () => {
    // If the owner filter is applied, only those workflow ownered by that user should be returned.
    component.filters.owners[0].checked = true;
    component.filters.updateSelectedOwners();
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2"]);
    expect(component.filters.masterFilterList).toEqual(["owner: Texera"]);
  });

  it("searchByIDs", async () => {
    // If the ID filter is applied, only those workflows should be returned.
    component.filters.wids[0].checked = true;
    component.filters.wids[1].checked = true;
    component.filters.wids[2].checked = true;
    component.filters.updateSelectedIDs();
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.filters.masterFilterList).toEqual(["id: 1", "id: 2", "id: 3"]);
  });

  it("searchByProjects", async () => {
    // If the project filter is applied, only those workflows belonging to those projects should be returned.
    component.filters.userProjectsDropdown[0].checked = true;
    component.filters.updateSelectedProjects();
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.filters.masterFilterList).toEqual(["project: Project1"]);
  });

  it("searchByCreationTime", async () => {
    // If the creation time filter is applied, only those workflows matching the date range should be returned.
    component.filters.selectedCtime = [new Date(1970, 0, 3), new Date(1981, 2, 13)];
    component.filters.buildMasterFilterList();
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 4", "workflow 5"]);
    expect(component.filters.masterFilterList).toEqual(["ctime: 1970-01-03 ~ 1981-03-13"]);
  });

  it("searchByModifyTime", async () => {
    // If the modified time filter is applied, only those workflows matching the date range should be returned.
    component.filters.selectedMtime = [new Date(1970, 0, 3), new Date(1981, 2, 13)];
    component.filters.buildMasterFilterList();
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 4", "workflow 5"]);
    expect(component.filters.masterFilterList).toEqual(["mtime: 1970-01-03 ~ 1981-03-13"]);
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
    const operatorGroup = component.filters.operators.get("Analysis");
    if (operatorGroup) {
      operatorGroup[2].checked = true; // sentiment analysis
      component.filters.updateSelectedOperators();
    }
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.filters.masterFilterList).toEqual(["operator: Sentiment Analysis"]); // userFriendlyName
  });

  it("searchByManyOperators", async () => {
    // If a multiple operator filters are provided, workflows containing any of the provided operators should be returned.
    const operatorGroup = component.filters.operators.get("Analysis");
    const operatorGroup2 = component.filters.operators.get("View Results");
    if (operatorGroup && operatorGroup2) {
      operatorGroup[2].checked = true; // sentiment analysis
      operatorGroup2[0].checked = true;
      component.filters.updateSelectedOperators();
    }
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.filters.masterFilterList).toEqual(["operator: Sentiment Analysis", "operator: View Results"]); // userFriendlyName
  });

  it("searchByManyParameters", async () => {
    // Apply the project, ID, owner, and operator filter all at once.
    const operatorGroup = component.filters.operators.get("Analysis");
    if (operatorGroup) {
      operatorGroup[3].checked = true; // Aggregation operator
      component.filters.updateSelectedOperators();

      component.filters.owners[0].checked = true; //Texera
      component.filters.owners[1].checked = true; //Angular
      component.filters.wids[0].checked = true;
      component.filters.wids[1].checked = true;
      component.filters.wids[2].checked = true; //id 1,2,3
      component.filters.userProjectsDropdown[0].checked = true; //Project 1
      component.filters.selectedCtime = [new Date(1970, 0, 1), new Date(1973, 2, 11)];
      component.filters.selectedMtime = [new Date(1970, 0, 1), new Date(1982, 3, 14)];
      component.filters.masterFilterList.push("1");
      //add/select new search parameter here

      component.filters.updateSelectedProjects();
      component.filters.updateSelectedIDs();
      component.filters.updateSelectedOwners();
    }
    await component.searchWorkflow();
    const SortedCase = component.dashboardWorkflowEntries.map(workflow => workflow.workflow.name);
    expect(SortedCase).toEqual(["workflow 1"]);
    expect(component.filters.masterFilterList).toEqual([
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
