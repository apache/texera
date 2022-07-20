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
    content: jsonCast<WorkflowContent>(
      " {\"operators\":[],\"operatorPositions\":{},\"links\":[],\"groups\":[],\"breakpoints\":{}}"
    ),
    creationTime: 1557787975000,
    lastModifiedTime: 1705673070000,
  };

  // for filter tests
  // eId,vId,status,result,bookmarked values are meaningless in this test
  const testWorkflowExecution1: WorkflowExecutionsEntry = {
    eId: 1,
    vId: 1,
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

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [NgbdModalWorkflowExecutionsComponent],
        providers: [NgbActiveModal, WorkflowExecutionsService],
        imports: [MatDialogModule, FormsModule, HttpClientModule],
      }).compileComponents();
    })
  );

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
    component.fuse.setCollection(component.allExecutionEntries);
    component.workflowExecutionsList = component.allExecutionEntries;
    component.executionSearchValue = "";
    component.searchExecution();
    const SortedCase = component.workflowExecutionsList.map(item => item.eId);
    expect(SortedCase).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  it("executionNameFilterTest correctName", () => {
    component.workflow = workflow;
    component.allExecutionEntries = [];
    component.allExecutionEntries = component.allExecutionEntries.concat(testExecutionEntries);
    component.fuse.setCollection(component.allExecutionEntries);
    component.workflowExecutionsList = component.allExecutionEntries;
    component.executionSearchValue = "cancer";
    component.searchExecution();
    const SortedCase = component.workflowExecutionsList.map(item => item.eId);
    expect(SortedCase).toEqual([6]);
  });

  it("userNameFilterTest", () => {
    component.workflow = workflow;
    component.allExecutionEntries = [];
    component.allExecutionEntries = component.allExecutionEntries.concat(testExecutionEntries);
    component.fuse.setCollection(component.allExecutionEntries);
    component.workflowExecutionsList = component.allExecutionEntries;
    component.executionSearchValue = "user:Amy";
    component.searchExecution();
    const SortedCase = component.workflowExecutionsList.map(item => item.eId);
    expect(SortedCase).toEqual([3]);
  });

  it("startingTimeFilterTest ':'", () => {
    component.workflow = workflow;
    component.allExecutionEntries = [];
    component.allExecutionEntries = component.allExecutionEntries.concat(testExecutionEntries);
    component.fuse.setCollection(component.allExecutionEntries);
    component.workflowExecutionsList = component.allExecutionEntries;
    component.executionSearchValue = "sTime:07/13/2022";
    component.searchExecution();
    const SortedCase = component.workflowExecutionsList.map(item => item.eId);
    expect(SortedCase).toEqual([1]);
  });

  it("startingTimeFilterTest '<'", () => {
    component.workflow = workflow;
    component.allExecutionEntries = [];
    component.allExecutionEntries = component.allExecutionEntries.concat(testExecutionEntries);
    component.fuse.setCollection(component.allExecutionEntries);
    component.workflowExecutionsList = component.allExecutionEntries;
    component.executionSearchValue = "sTime:<07/13/2022";
    component.searchExecution();
    const SortedCase = component.workflowExecutionsList.map(item => item.eId);
    expect(SortedCase).toEqual([1, 3, 4, 5]);
  });

  it("startingTimeFilterTest '>'", () => {
    component.workflow = workflow;
    component.allExecutionEntries = [];
    component.allExecutionEntries = component.allExecutionEntries.concat(testExecutionEntries);
    component.fuse.setCollection(component.allExecutionEntries);
    component.workflowExecutionsList = component.allExecutionEntries;
    component.executionSearchValue = "sTime:>07/13/2022";
    component.searchExecution();
    const SortedCase = component.workflowExecutionsList.map(item => item.eId);
    expect(SortedCase).toEqual([1, 2, 6, 7]);
  });

  it("updatingTimeFilterTest ':'", () => {
    component.workflow = workflow;
    component.allExecutionEntries = [];
    component.allExecutionEntries = component.allExecutionEntries.concat(testExecutionEntries);
    component.fuse.setCollection(component.allExecutionEntries);
    component.workflowExecutionsList = component.allExecutionEntries;
    component.executionSearchValue = "uTime:07/13/2022";
    component.searchExecution();
    const SortedCase = component.workflowExecutionsList.map(item => item.eId);
    expect(SortedCase).toEqual([1]);
  });

  it("updatingTimeFilterTest '<'", () => {
    component.workflow = workflow;
    component.allExecutionEntries = [];
    component.allExecutionEntries = component.allExecutionEntries.concat(testExecutionEntries);
    component.fuse.setCollection(component.allExecutionEntries);
    component.workflowExecutionsList = component.allExecutionEntries;
    component.executionSearchValue = "uTime:<07/13/2022";
    component.searchExecution();
    const SortedCase = component.workflowExecutionsList.map(item => item.eId);
    expect(SortedCase).toEqual([1, 3, 4, 5]);
  });

  it("updatingTimeFilterTest '>'", () => {
    component.workflow = workflow;
    component.allExecutionEntries = [];
    component.allExecutionEntries = component.allExecutionEntries.concat(testExecutionEntries);
    component.fuse.setCollection(component.allExecutionEntries);
    component.workflowExecutionsList = component.allExecutionEntries;
    component.executionSearchValue = "uTime:>07/13/2022";
    component.searchExecution();
    const SortedCase = component.workflowExecutionsList.map(item => item.eId);
    expect(SortedCase).toEqual([1, 2, 6, 7]);
  });
});
