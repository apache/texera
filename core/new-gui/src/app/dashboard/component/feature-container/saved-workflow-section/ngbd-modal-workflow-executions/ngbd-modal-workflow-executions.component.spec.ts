import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { MatDialogModule } from "@angular/material/dialog";

import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { FormsModule } from "@angular/forms";

import { WorkflowExecutionsService } from "../../../../service/workflow-executions/workflow-executions.service";
import { HttpClientModule } from "@angular/common/http";

import { NgbdModalWorkflowExecutionsComponent } from "./ngbd-modal-workflow-executions.component";
import { WorkflowExecutionsEntry } from "../../../../type/workflow-executions-entry";
import { Workflow, WorkflowContent } from "../../../../../common/type/workflow";
import { jsonCast } from "../../../../../common/util/storage";
import { NotificationService } from "../../../../../common/service/notification/notification.service";

describe("NgbModalWorkflowExecutionsComponent", () => {
  let component: NgbdModalWorkflowExecutionsComponent;
  let fixture: ComponentFixture<NgbdModalWorkflowExecutionsComponent>;
  let modalService: NgbActiveModal;

  const workflow: Workflow = {
    wid: 1,
    name: "workflow 1",
    description: "dummy description",
    content: jsonCast<WorkflowContent>(
      " {\"operators\":[],\"operatorPositions\":{},\"links\":[],\"groups\":[],\"breakpoints\":{}}"
    ),
    creationTime: 1557787975000,
    lastModifiedTime: 1705673070000,
  };

  // for filter & sorting tests
  // eId,vId,status,result,bookmarked values are meaningless in this test
  const testWorkflowExecution1: WorkflowExecutionsEntry = {
    eId: 1,
    vId: 1,
    sId: 1,
    userName: "texera",
    name: "execution1",
    startingTime: 1657777975000, // 07/13/2022 22:52:55 GMT-7
    completionTime: 1657778000000, // 7/13/2022, 22:53:20 GMT-7
    status: 3,
    result: "",
    bookmarked: false,
  };

  const testWorkflowExecution2: WorkflowExecutionsEntry = {
    eId: 2,
    vId: 2,
    sId: 2,
    userName: "Peter",
    name: "twitter1",
    startingTime: 1657787975000, // 7/14/2022, 1:39:35 GMT-7
    completionTime: 1658787975000, // 7/25/2022, 15:26:15 GMT-7
    status: 3,
    result: "",
    bookmarked: false,
  };

  const testWorkflowExecution3: WorkflowExecutionsEntry = {
    eId: 3,
    vId: 3,
    sId: 3,
    userName: "Amy",
    name: "healthcare",
    startingTime: 1557787975000, // 5/13/2019, 15:52:55 GMT-7
    completionTime: 1557987975000, // 5/15/2019, 23:26:15 GMT-7
    status: 3,
    result: "",
    bookmarked: true,
  };

  const testWorkflowExecution4: WorkflowExecutionsEntry = {
    eId: 4,
    vId: 4,
    sId: 4,
    userName: "sarahchen",
    name: "123",
    startingTime: 1617797970000, // 4/7/2021, 5:19:30 GMT-7
    completionTime: 1618797970000, // 4/18/2021, 19:06:10 GMT-7
    status: 1,
    result: "",
    bookmarked: false,
  };

  const testWorkflowExecution5: WorkflowExecutionsEntry = {
    eId: 5,
    vId: 5,
    sId: 5,
    userName: "edison",
    name: "covid",
    startingTime: 1623957560000, // 6/17/2021, 12:19:20 GMT-7
    completionTime: 1624058390000, // 6/18/2021, 16:19:50 GMT-7
    status: 2,
    result: "",
    bookmarked: true,
  };

  const testWorkflowExecution6: WorkflowExecutionsEntry = {
    eId: 6,
    vId: 6,
    sId: 6,
    userName: "johnny270",
    name: "cancer",
    startingTime: 1695673070000, // 9/25/2023, 13:17:50 GMT-7
    completionTime: 1705673070000, // 1/19/2024, 6:04:30 GMT-7
    status: 5,
    result: "",
    bookmarked: false,
  };

  const testWorkflowExecution7: WorkflowExecutionsEntry = {
    eId: 7,
    vId: 7,
    sId: 7,
    userName: "texera",
    name: "Untitled Execution",
    startingTime: 1665673070000, // 10/13/2022, 7:57:50 GMT-7
    completionTime: 1669673070000, // 11/28/2022, 14:04:30 GMT-7
    status: 4,
    result: "",
    bookmarked: false,
  };

  const testExecutionEntries: WorkflowExecutionsEntry[] = [
    testWorkflowExecution1,
    testWorkflowExecution2,
    testWorkflowExecution3,
    testWorkflowExecution4,
    testWorkflowExecution5,
    testWorkflowExecution6,
    testWorkflowExecution7,
  ];

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [NgbdModalWorkflowExecutionsComponent],
      providers: [NgbActiveModal, WorkflowExecutionsService],
      imports: [MatDialogModule, FormsModule, HttpClientModule],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalWorkflowExecutionsComponent);
    component = fixture.componentInstance;
    modalService = TestBed.get(NgbActiveModal);
    fixture.detectChanges();
  });

  it("executionNameFilterTest NoInput", () => {
    component.workflow = workflow;
    component.allExecutionEntries = [];
    component.allExecutionEntries = component.allExecutionEntries.concat(testExecutionEntries);
    component.paginatedExecutionEntries = component.allExecutionEntries;
    component.fuse.setCollection(component.allExecutionEntries);
    component.workflowExecutionsDisplayedList = component.allExecutionEntries;
    component.executionSearchValue = "";
    component.searchExecution();
    const filteredCase = component.workflowExecutionsDisplayedList.map(item => item.eId);
    expect(filteredCase).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  it("executionNameFilterTest correctName", () => {
    component.workflow = workflow;
    component.allExecutionEntries = [];
    component.allExecutionEntries = component.allExecutionEntries.concat(testExecutionEntries);
    component.paginatedExecutionEntries = component.allExecutionEntries;
    component.fuse.setCollection(component.allExecutionEntries);
    component.workflowExecutionsDisplayedList = component.allExecutionEntries;
    component.executionSearchValue = "cancer";
    component.searchExecution();
    const filteredCase = component.workflowExecutionsDisplayedList.map(item => item.eId);
    expect(filteredCase).toEqual([6]);
  });

  it("userNameFilterTest", () => {
    component.workflow = workflow;
    component.allExecutionEntries = [];
    component.allExecutionEntries = component.allExecutionEntries.concat(testExecutionEntries);
    component.paginatedExecutionEntries = component.allExecutionEntries;
    component.fuse.setCollection(component.allExecutionEntries);
    component.workflowExecutionsDisplayedList = component.allExecutionEntries;
    component.executionSearchValue = "user:Amy";
    component.searchExecution();
    const filteredCase = component.workflowExecutionsDisplayedList.map(item => item.eId);
    expect(filteredCase).toEqual([3]);
  });

  it("statusFilterTest", () => {
    component.workflow = workflow;
    component.allExecutionEntries = [];
    component.allExecutionEntries = component.allExecutionEntries.concat(testExecutionEntries);
    component.paginatedExecutionEntries = component.allExecutionEntries;
    component.fuse.setCollection(component.allExecutionEntries);
    component.workflowExecutionsDisplayedList = component.allExecutionEntries;
    component.executionSearchValue = "status:Completed";
    component.searchExecution();
    const filteredCase = component.workflowExecutionsDisplayedList.map(item => item.eId);
    expect(filteredCase).toEqual([1, 2, 3]);
  });

  it("filterComboTest", () => {
    component.workflow = workflow;
    component.allExecutionEntries = [];
    component.allExecutionEntries = component.allExecutionEntries.concat(testExecutionEntries);
    component.paginatedExecutionEntries = component.allExecutionEntries;
    component.fuse.setCollection(component.allExecutionEntries);
    component.workflowExecutionsDisplayedList = component.allExecutionEntries;
    component.executionSearchValue = "execution1 user:texera";
    component.searchExecution();
    const filteredCase = component.workflowExecutionsDisplayedList.map(item => item.eId);
    expect(filteredCase).toEqual([1, 7]);
  });

  it("executionNameSortTest increasingOrder", () => {
    component.workflow = workflow;
    component.workflowExecutionsDisplayedList = [];
    component.workflowExecutionsDisplayedList = component.workflowExecutionsDisplayedList.concat(testExecutionEntries);
    component.ascSort("Name");
    const SortedCase = component.workflowExecutionsDisplayedList.map(item => item.eId);
    /* Order: 123/Exe4, cancer/Exe6, covid/Exe5, execution1/Exe1, healthcare/Exe3, twitter/Exe2, Untitled Execution/Exe7*/
    expect(SortedCase).toEqual([4, 6, 5, 1, 3, 2, 7]);
  });

  it("executionNameSortTest decreasingOrder", () => {
    component.workflow = workflow;
    component.workflowExecutionsDisplayedList = [];
    component.workflowExecutionsDisplayedList = component.workflowExecutionsDisplayedList.concat(testExecutionEntries);
    component.dscSort("Name");
    const SortedCase = component.workflowExecutionsDisplayedList.map(item => item.eId);
    /* Order: Untitled Execution/Exe7, twitter/Exe2, healthcare/Exe3, execution1/Exe1, covid/Exe5, cancer/Exe6, 123/Exe4*/
    expect(SortedCase).toEqual([7, 2, 3, 1, 5, 6, 4]);
  });

  it("userNameSortTest increasingOrder", () => {
    component.workflow = workflow;
    component.workflowExecutionsDisplayedList = [];
    component.workflowExecutionsDisplayedList = component.workflowExecutionsDisplayedList.concat(testExecutionEntries);
    component.ascSort("Username");
    const SortedCase = component.workflowExecutionsDisplayedList.map(item => item.eId);
    /* Order: Amy/Exe3, edison/Exe5, johnny270/Exe6, Peter/Exe2, sarahchen/Exe4, texera/Exe1, texera/Exe7*/
    expect(SortedCase).toEqual([3, 5, 6, 2, 4, 1, 7]);
  });

  it("userNameSortTest decreasingOrder", () => {
    component.workflow = workflow;
    component.workflowExecutionsDisplayedList = [];
    component.workflowExecutionsDisplayedList = component.workflowExecutionsDisplayedList.concat(testExecutionEntries);
    component.dscSort("Username");
    const SortedCase = component.workflowExecutionsDisplayedList.map(item => item.eId);
    /* Order: texera/Exe1, texera/Exe7, sarahchen/Exe4, Peter/Exe2, johnny270/Exe6, edison/Exe5, Amy/Exe3*/
    expect(SortedCase).toEqual([1, 7, 4, 2, 6, 5, 3]);
  });

  it("startingTimeSortTest increasingOrder", () => {
    component.workflow = workflow;
    component.workflowExecutionsDisplayedList = [];
    component.workflowExecutionsDisplayedList = component.workflowExecutionsDisplayedList.concat(testExecutionEntries);
    component.ascSort("Starting Time");
    const SortedCase = component.workflowExecutionsDisplayedList.map(item => item.eId);
    /* Order: Exe3, Exe4, Exe5, Exe1, Exe2, Exe7, Exe6*/
    expect(SortedCase).toEqual([3, 4, 5, 1, 2, 7, 6]);
  });

  it("startingTimeSortTest decreasingOrder", () => {
    component.workflow = workflow;
    component.workflowExecutionsDisplayedList = [];
    component.workflowExecutionsDisplayedList = component.workflowExecutionsDisplayedList.concat(testExecutionEntries);
    component.dscSort("Starting Time");
    const SortedCase = component.workflowExecutionsDisplayedList.map(item => item.eId);
    /* Order: Exe6, Exe7, Exe2, Exe1, Exe5, Exe4, Exe3*/
    expect(SortedCase).toEqual([6, 7, 2, 1, 5, 4, 3]);
  });

  it("updatingTimeSortTest increasingOrder", () => {
    component.workflow = workflow;
    component.workflowExecutionsDisplayedList = [];
    component.workflowExecutionsDisplayedList = component.workflowExecutionsDisplayedList.concat(testExecutionEntries);
    component.ascSort("Last Status Updated Time");
    const SortedCase = component.workflowExecutionsDisplayedList.map(item => item.eId);
    /* Order: Exe3, Exe4, Exe5, Exe1, Exe2, Exe7, Exe6*/
    expect(SortedCase).toEqual([3, 4, 5, 1, 2, 7, 6]);
  });

  it("updatingTimeSortTest decreasingOrder", () => {
    component.workflow = workflow;
    component.workflowExecutionsDisplayedList = [];
    component.workflowExecutionsDisplayedList = component.workflowExecutionsDisplayedList.concat(testExecutionEntries);
    component.dscSort("Last Status Updated Time");
    const SortedCase = component.workflowExecutionsDisplayedList.map(item => item.eId);
    /* Order: Exe6, Exe7, Exe2, Exe1, Exe5, Exe4, Exe3*/
    expect(SortedCase).toEqual([6, 7, 2, 1, 5, 4, 3]);
  });
});
