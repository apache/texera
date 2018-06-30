import { ExecutionResult } from '../../types/execute-workflow.interface';
import { TestBed, inject } from '@angular/core/testing';

import { ExecuteWorkflowService } from './execute-workflow.service';

import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { JointUIService } from '../joint-ui/joint-ui.service';
import { Observable } from 'rxjs/Observable';

import { mockExecutionResult } from './mock-result-data';
import { mockWorkflowPlan, mockLogicalPlan } from './mock-workflow-plan';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { marbles } from 'rxjs-marbles';
import { WorkflowGraph } from '../workflow-graph/model/workflow-graph';
import { LogicalPlan } from '../../types/execute-workflow.interface';


class StubHttpClient {

  constructor() {}

  public post(): Observable<string> { return Observable.of('a'); }

}

/* tslint:disable:no-non-null-assertion */

describe('ExecuteWorkflowService', () => {

  let service: ExecuteWorkflowService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        ExecuteWorkflowService,
        WorkflowActionService,
        JointUIService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: HttpClient, useClass: StubHttpClient},
      ]
    });

    service = TestBed.get(ExecuteWorkflowService);
  });

  it('should be created', inject([ExecuteWorkflowService], (injectedService: ExecuteWorkflowService) => {
    expect(injectedService).toBeTruthy();
  }));

  it('should generate a logical plan request based on the workflow graph that is passed to the function', () => {
    const workflowGraph: WorkflowGraph = mockWorkflowPlan;
    const newLogicalPlan: LogicalPlan = ExecuteWorkflowService.getLogicalPlanRequest(workflowGraph);
    expect(newLogicalPlan).toEqual(mockLogicalPlan);
  });

  it('should notify execution start event stream when an execution begins', marbles((m) => {
    const executionStartStream = service.getExecuteStartedStream()
      .map(() => 'a');

    m.hot('-a-').do(() => service.executeWorkflow()).subscribe();

    const expectedStream = m.hot('-a-');

    m.expect(executionStartStream).toBeObservable(expectedStream);

  }));

  it('should notify execution end event stream when a correct result is passed from backend', marbles((m) => {
    const executionEndStream = service.getExecuteEndedStream()
      .map(() => 'a');

    // execute workflow at this time
    m.hot('-a-').do(() => service.executeWorkflow()).subscribe();

    const expectedStream = m.hot('-a-');

    m.expect(executionEndStream).toBeObservable(expectedStream);

  }));

  it('should call post function when executing workflow', () => {
    const httpClient: HttpClient = TestBed.get(HttpClient);
    const postMethodSpy = spyOn(httpClient, 'post').and.returnValue(
      Observable.of(mockExecutionResult)
    );

    service.executeWorkflow();

    expect(postMethodSpy.calls.count()).toEqual(1);

  });

  it('should stimulate backend error for invalid workflow graph and generate correct error messages', () => {
    const mockErrorMessage = 'mock backend error message';

    const httpClient: HttpClient = TestBed.get(HttpClient);
    spyOn(httpClient, 'post').and.returnValue(
      Observable.throw({
        status: 400,
        error: {
          code: 1,
          message: mockErrorMessage
        }
      })
    );

    let executionResult: ExecutionResult | undefined;
    service.getExecuteEndedStream().subscribe(value => executionResult = value);

    service.executeWorkflow();

    expect(executionResult!.code).toEqual(1);
    if (! ExecuteWorkflowService.isExecutionSuccessful(executionResult)) {
      expect(executionResult!.message).toEqual(mockErrorMessage);
    }

  });

  it('should stimulate a backend server system error and generate correct error messages', () => {

    const mockErrorMessage = 'mock server error message';

    const httpClient: HttpClient = TestBed.get(HttpClient);
    spyOn(httpClient, 'post').and.returnValue(
      Observable.throw({
        status: 500,
        error: {
          code: 1,
          message: mockErrorMessage
        }
      })
    );

    let executionResult: ExecutionResult | undefined;
    service.getExecuteEndedStream().subscribe(value => executionResult = value);

    service.executeWorkflow();

    expect(executionResult!.code).toEqual(1);
    if (!ExecuteWorkflowService.isExecutionSuccessful(executionResult)) {
      expect(executionResult!.message).toEqual(`Texera server error: ${mockErrorMessage}`);
    }

  });

  it('should stimulate frontend internet error and generate correct error messages', () => {

    const mockErrorMessage = 'mock interent error message';

    const httpClient: HttpClient = TestBed.get(HttpClient);

    const progressEvent: ProgressEvent = new ProgressEvent(mockErrorMessage, undefined);

    spyOn(httpClient, 'post').and.returnValue(
      Observable.throw(new HttpErrorResponse(
        {
          error: progressEvent,
          headers: undefined,
          status: 0,
          statusText: '',
          url: ''}
        )
      )
    );

    let executionResult: ExecutionResult | undefined;
    service.getExecuteEndedStream().subscribe(value => executionResult = value);

    service.executeWorkflow();

    expect(executionResult!.code).toEqual(1);

    if (!ExecuteWorkflowService.isExecutionSuccessful(executionResult)) {
      expect(executionResult!.message).toEqual(`Could not reach Texera server`);
    }


  });



});
