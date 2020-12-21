import { ExecutionResult, ExecutionState } from '../../types/execute-workflow.interface';
import { TestBed, inject, fakeAsync, tick, flush } from '@angular/core/testing';

import { ExecuteWorkflowService, FORM_DEBOUNCE_TIME_MS } from './execute-workflow.service';

import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { UndoRedoService } from './../../service/undo-redo/undo-redo.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { JointUIService } from '../joint-ui/joint-ui.service';
import { Observable } from 'rxjs/Observable';

import { mockExecutionResult, mockResultData } from './mock-result-data';
import { mockWorkflowPlan_scan_result, mockLogicalPlan_scan_result } from './mock-workflow-plan';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { marbles } from 'rxjs-marbles';
import { WorkflowGraph } from '../workflow-graph/model/workflow-graph';
import { LogicalPlan } from '../../types/execute-workflow.interface';
import { environment } from '../../../../environments/environment';
import { mockScanResultLink } from '../workflow-graph/model/mock-workflow-data';
import { WorkflowUtilService } from '../workflow-graph/util/workflow-util.service';

class StubHttpClient {

  constructor() { }

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
        WorkflowUtilService,
        UndoRedoService,
        JointUIService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: HttpClient, useClass: StubHttpClient },
      ]
    });

    service = TestBed.inject(ExecuteWorkflowService);
    environment.pauseResumeEnabled = true;
  });

  it('should be created', inject([ExecuteWorkflowService], (injectedService: ExecuteWorkflowService) => {
    expect(injectedService).toBeTruthy();
  }));

  it('should generate a logical plan request based on the workflow graph that is passed to the function', () => {
    const workflowGraph: WorkflowGraph = mockWorkflowPlan_scan_result;
    const newLogicalPlan: LogicalPlan = ExecuteWorkflowService.getLogicalPlanRequest(workflowGraph);
    expect(newLogicalPlan).toEqual(mockLogicalPlan_scan_result);
  });

  // it('should notify execution start event stream when an execution begins', marbles((m) => {
  //   const executionStartStream = service.getExecuteStartedStream()
  //     .map(() => 'a');

  //   m.hot('-a-').do(() => service.executeWorkflow()).subscribe();

  //   const expectedStream = m.hot('-a-');

  //   m.expect(executionStartStream).toBeObservable(expectedStream);

  // }));

  // it('should notify execution end event stream when a correct result is passed from backend', marbles((m) => {
  //   const executionEndStream = service.getExecuteEndedStream()
  //     .map(() => 'a');

  //   // execute workflow at this time
  //   m.hot('-a-').do(() => service.executeWorkflow()).subscribe();

  //   const expectedStream = m.hot('-a-');

  //   m.expect(executionEndStream).toBeObservable(expectedStream);

  // }));

  it('should msg backend when executing workflow', fakeAsync(() => {

    if (environment.amberEngineEnabled) {
      const wsSendSpy = spyOn((service as any).workflowWebsocketService, 'send');

      service.executeWorkflow();
      tick(FORM_DEBOUNCE_TIME_MS + 1);
      flush();
      expect(wsSendSpy).toHaveBeenCalledTimes(1);
    } else {
      // old engine test
      const httpClient: HttpClient = TestBed.inject(HttpClient);
      const postMethodSpy = spyOn(httpClient, 'post').and.returnValue(
        Observable.of(mockExecutionResult)
      );

      service.executeWorkflow();
      expect(postMethodSpy.calls.count()).toEqual(1);
    }

  }));

  // it('should stimulate backend error for invalid workflow graph and generate correct error messages', () => {
  //   const mockErrorMessage = 'mock backend error message';

  //   const httpClient: HttpClient = TestBed.inject(HttpClient);
  //   spyOn(httpClient, 'post').and.returnValue(
  //     Observable.throw({
  //       status: 400,
  //       error: {
  //         code: 1,
  //         message: mockErrorMessage
  //       }
  //     })
  //   );

  //   let executionResult: ExecutionResult | undefined;
  //   service.getExecuteEndedStream().subscribe(value => executionResult = value);

  //   service.executeWorkflow();

  //   expect(executionResult!.code).toEqual(1);
  //   if (! ExecuteWorkflowService.isExecutionSuccessful(executionResult)) {
  //     expect(executionResult!.message).toEqual(mockErrorMessage);
  //   }

  // });

  // it('should stimulate a backend server system error and generate correct error messages', () => {

  //   const mockErrorMessage = 'mock server error message';

  //   const httpClient: HttpClient = TestBed.inject(HttpClient);
  //   spyOn(httpClient, 'post').and.returnValue(
  //     Observable.throw({
  //       status: 500,
  //       error: {
  //         code: 1,
  //         message: mockErrorMessage
  //       }
  //     })
  //   );

  //   let executionResult: ExecutionResult | undefined;
  //   service.getExecuteEndedStream().subscribe(value => executionResult = value);

  //   service.executeWorkflow();

  //   expect(executionResult!.code).toEqual(1);
  //   if (!ExecuteWorkflowService.isExecutionSuccessful(executionResult)) {
  //     expect(executionResult!.message).toEqual(`Texera server error: ${mockErrorMessage}`);
  //   }

  // });

  // it('should stimulate frontend internet error and generate correct error messages', () => {

  //   const mockErrorMessage = 'mock interent error message';

  //   const httpClient: HttpClient = TestBed.inject(HttpClient);

  //   const progressEvent: ProgressEvent = new ProgressEvent(mockErrorMessage, undefined);

  //   spyOn(httpClient, 'post').and.returnValue(
  //     Observable.throw(new HttpErrorResponse(
  //       {
  //         error: progressEvent,
  //         headers: undefined,
  //         status: 0,
  //         statusText: '',
  //         url: ''}
  //       )
  //     )
  //   );

  //   let executionResult: ExecutionResult | undefined;
  //   service.getExecutionStateStream().subscribe(event => {
  //     if (event.current.state === ExecutionState.Completed) {
  //     }
  //   });

  //   service.executeWorkflow();

  //   expect(executionResult!.code).toEqual(1);

  //   if (!ExecuteWorkflowService.isExecutionSuccessful(executionResult)) {
  //     expect(executionResult!.message).toEqual(`Could not reach Texera server`);
  //   }


  // });


  it('it should raise an error when pauseWorkflow() is called without an execution state', () => {
    (service as any).currentState = { state: ExecutionState.Uninitialized };
    expect(function () {
      service.pauseWorkflow();
    }).toThrowError(new RegExp('cannot pause workflow, current execution state is ' + (service as any).currentState.state));
  });


  it('it should raise an error when resumeWorkflow() is called without an execution state', () => {
    (service as any).currentState = { state: ExecutionState.Uninitialized };
    expect(function () {
      service.resumeWorkflow();
    }).toThrowError(new RegExp('cannot resume workflow, current execution state is '  + (service as any).currentState.state));
  });


  // it('should notify pause and resume stream when a result is returned from backend after calling pauseWorkflow()', marbles((m) => {
  //   (service as any).workflowExecutionID = 'DEFAULT_WORKFLOW_ID';

  //   m.hot('-e-').do(() => service.pauseWorkflow()).subscribe();
  //   const resultStream = service.getExecutionPauseResumeStream().map(value => 'e');

  //   const expectedStream = m.hot('-e-');
  //   m.expect(resultStream).toBeObservable(expectedStream);
  // }));


  // it('should notify pause and resume stream when a result is returned from backend after calling resumeWorkflow()', marbles((m) => {
  //   (service as any).workflowExecutionID = 'DEFAULT_WORKFLOW_ID';

  //   m.hot('-e-').do(() => service.resumeWorkflow()).subscribe();
  //   const resultStream = service.getExecutionPauseResumeStream().map(value => 'e');

  //   const expectedStream = m.hot('-e-');
  //   m.expect(resultStream).toBeObservable(expectedStream);
  // }));

});
