import { async, ComponentFixture, TestBed } from "@angular/core/testing";

import { ResultPanelComponent } from "./result-panel.component";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";

import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { By } from "@angular/platform-browser";

import { ResultPanelToggleService } from "../../service/result-panel-toggle/result-panel-toggle.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { DynamicModule } from "ng-dynamic-component";
import { mockPoint, mockResultPredicate } from "../../service/workflow-graph/model/mock-workflow-data";

describe("ResultPanelComponent", () => {
  let component: ResultPanelComponent;
  let fixture: ComponentFixture<ResultPanelComponent>;
  let executeWorkflowService: ExecuteWorkflowService;
  let nzModalService: NzModalService;
  let workflowActionService: WorkflowActionService;
  let resultPanelToggleService: ResultPanelToggleService;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ResultPanelComponent],
      imports: [DynamicModule, HttpClientTestingModule, NzModalModule],
      providers: [
        WorkflowActionService,
        ExecuteWorkflowService,
        ResultPanelToggleService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ResultPanelComponent);
    component = fixture.componentInstance;
    executeWorkflowService = TestBed.inject(ExecuteWorkflowService);
    resultPanelToggleService = TestBed.inject(ResultPanelToggleService);
    nzModalService = TestBed.inject(NzModalService);
    workflowActionService = TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  it("should create", () => expect(component).toBeTruthy());

  // it('should change the content of result panel correctly when selected operator is a sink operator with result', marbles((m) => {

  //   const endMarbleString = '-e-|';
  //   const endMarblevalues = {
  //     e: mockExecutionResult
  //   };

  //   const httpClient: HttpClient = TestBed.inject(HttpClient);
  //   spyOn(httpClient, 'post').and.returnValue(
  //     Observable.of(mockExecutionResult)
  //   );

  //   spyOn(executeWorkflowService, 'getExecuteEndedStream').and.returnValue(
  //     m.hot(endMarbleString, endMarblevalues)
  //   );

  //   workflowActionService.addOperator(mockResultOperator, mockResultPoint);
  //   workflowActionService.injectJointGraphWrapper().highlightOperator(mockResultData[0].operatorID);

  //   const testComponent = new ResultPanelComponent(executeWorkflowService, ngbModel, resultPanelToggleService, workflowActionService);

  //   executeWorkflowService.executeWorkflow();

  //   executeWorkflowService.injectExecuteEndedStream().subscribe({
  //     complete: () => {
  //       const mockColumns = Object.keys(mockResultData[0].table[0]);
  //       expect(testComponent.currentDisplayColumns).toEqual(mockColumns);
  //       expect(testComponent.currentColumns).toBeTruthy();
  //       expect(testComponent.currentDataSource).toBeTruthy();
  //     }
  //   });

  // }));

  // it(`should create error message and update the Component's properties when the execution result size is 0`, marbles((m) => {
  //   const endMarbleString = '-e-|';
  //   const endMarbleValues = {
  //     e: mockExecutionEmptyResult
  //   };

  //   spyOn(executeWorkflowService, 'getExecuteEndedStream').and.returnValue(
  //     m.hot(endMarbleString, endMarbleValues)
  //   );

  //   const testComponent = new ResultPanelComponent(executeWorkflowService, ngbModel, resultPanelToggleService, workflowActionService);
  //   executeWorkflowService.injectExecuteEndedStream().subscribe({
  //     complete: () => {
  //       expect(testComponent.message).toEqual(`execution doesn't have any results`);
  //       expect(testComponent.currentDataSource).toBeFalsy();
  //       expect(testComponent.currentColumns).toBeFalsy();
  //       expect(testComponent.currentDisplayColumns).toBeFalsy();
  //       expect(testComponent.showMessage).toBeTruthy();
  //     }
  //   });
  // }));

  // it('should respond to error and print error messages', marbles((m) => {
  //   const endMarbleString = '-e-|';
  //   const endMarbleValues = {
  //     e: mockExecutionErrorResult
  //   };

  //   spyOn(executeWorkflowService, 'getExecuteEndedStream').and.returnValue(
  //     m.hot(endMarbleString, endMarbleValues)
  //   );

  //   const testComponent = new ResultPanelComponent(executeWorkflowService, ngbModel, resultPanelToggleService, workflowActionService);

  //   executeWorkflowService.injectExecuteEndedStream().subscribe({
  //     complete: () => {
  //       expect(testComponent.showMessage).toBeTruthy();
  //       expect(testComponent.message.length).toBeGreaterThan(0);
  //     }
  //   });

  // }));

  // it('should update the result panel when new execution result arrives and a sink operator is selected', marbles((m) => {
  //   const endMarbleString = '-a-b-|';
  //   const endMarblevalues = {
  //     a: mockExecutionErrorResult,
  //     b: mockExecutionResult
  //   };
  //   const httpClient: HttpClient = TestBed.inject(HttpClient);
  //   spyOn(httpClient, 'post').and.returnValue(
  //     Observable.of(mockExecutionResult)
  //   );

  //   spyOn(executeWorkflowService, 'getExecuteEndedStream').and.returnValue(
  //     m.hot(endMarbleString, endMarblevalues)
  //   );

  //   workflowActionService.addOperator(mockResultOperator, mockResultPoint);
  //   workflowActionService.injectJointGraphWrapper().highlightOperator(mockResultData[0].operatorID);

  //   const testComponent = new ResultPanelComponent(executeWorkflowService, ngbModel, resultPanelToggleService, workflowActionService);

  //   executeWorkflowService.executeWorkflow();

  //   executeWorkflowService.injectExecuteEndedStream().subscribe({
  //     complete: () => {
  //       const mockColumns = Object.keys(mockResultData[0].table[0]);
  //       expect(testComponent.currentDisplayColumns).toEqual(mockColumns);
  //       expect(testComponent.currentColumns).toBeTruthy();
  //       expect(testComponent.currentDataSource).toBeTruthy();
  //     }
  //   });
  // }));

  // it('should generate the result table correctly on the user interface', () => {

  //   const httpClient: HttpClient = TestBed.inject(HttpClient);
  //   spyOn(httpClient, 'post').and.returnValue(
  //     Observable.of(mockExecutionResult)
  //   );

  //   executeWorkflowService.injectExecuteEndedStream().subscribe();

  //   executeWorkflowService.executeWorkflow();

  //   fixture.detectChanges();

  //   const resultTable = fixture.debugElement.query(By.css('.result-table'));
  //   expect(resultTable).toBeTruthy();
  // });

  it("should show nothing by default", () => {
    expect(component.frameComponentConfigs.size).toBe(0);
  });

  it("should show the result panel if a workflow finishes execution", () => {
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    executeWorkflowService["updateExecutionState"]({
      state: ExecutionState.Running,
    });
    executeWorkflowService["updateExecutionState"]({
      state: ExecutionState.Completed,
    });
    fixture.detectChanges();
    const resultPanelDiv = fixture.debugElement.query(By.css(".texera-workspace-result-panel-body"));
    const resultPanelHtmlElement: HTMLElement = resultPanelDiv.nativeElement;
    expect(resultPanelHtmlElement.hasAttribute("hidden")).toBeFalsy();
  });

  it("should show the result panel if the current status of the result panel is hidden and when the toggle is triggered", () => {
    const resultPanelDiv = fixture.debugElement.query(By.css(".texera-workspace-result-panel-body"));
    const resultPanelHtmlElement: HTMLElement = resultPanelDiv.nativeElement;

    expect(resultPanelHtmlElement.hasAttribute("hidden")).toBeTruthy();

    resultPanelToggleService.toggleResultPanel();
    fixture.detectChanges();

    expect(resultPanelHtmlElement.hasAttribute("hidden")).toBeFalsy();
  });

  it(`should hide the result panel if the current status of the result panel is already
      shown when the toggle is triggered`, () => {
    const resultPanelDiv = fixture.debugElement.query(By.css(".texera-workspace-result-panel-body"));
    const resultPanelHtmlElement: HTMLElement = resultPanelDiv.nativeElement;

    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    executeWorkflowService["updateExecutionState"]({
      state: ExecutionState.Running,
    });
    executeWorkflowService["updateExecutionState"]({
      state: ExecutionState.Completed,
    });
    fixture.detectChanges();
    expect(resultPanelHtmlElement.hasAttribute("hidden")).toBeFalsy();

    resultPanelToggleService.toggleResultPanel();
    fixture.detectChanges();

    expect(resultPanelHtmlElement.hasAttribute("hidden")).toBeTruthy();
  });
});
