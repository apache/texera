import { TestBed, inject } from '@angular/core/testing';

import { ExecuteWorkflowService } from './execute-workflow.service';

import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { JointUIService } from '../joint-ui/joint-ui.service';
import { Observable } from 'rxjs/Observable';

import { MOCK_RESULT_DATA } from './mock-result-data';
import { MOCK_WORKFLOW_PLAN, MOCK_LOGICAL_PLAN } from './mock-workflow-plan';
import { HttpClient } from '@angular/common/http';
import { marbles } from 'rxjs-marbles';
import { WorkflowGraph } from '../workflow-graph/model/workflow-graph';
import { LogicalPlan } from '../../types/workflow-execute.interface';


class StubHttpClient {
  constructor() { }

  // fake an async http response with a very small delay
  public post(url: string, body: string, headers: object): Observable<any> {
    return Observable.of(MOCK_RESULT_DATA).delay(1);
  }

}

let service: ExecuteWorkflowService;

describe('ExecuteWorkflowService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        ExecuteWorkflowService,
        WorkflowActionService,
        JointUIService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: HttpClient, useClass: StubHttpClient}
      ]
    });

    service = TestBed.get(ExecuteWorkflowService);
  });

  it('should be created', inject([ExecuteWorkflowService], (injectedService: ExecuteWorkflowService) => {
    expect(injectedService).toBeTruthy();
  }));

  it('should generate a logical plan request based on the workflow graph that is passed to the function', marbles((m)=>{
    const workflowGraph: WorkflowGraph = MOCK_WORKFLOW_PLAN;
    const newLogicalPlan: LogicalPlan = service.getLogicalPlanRequest(workflowGraph);
    expect(MOCK_LOGICAL_PLAN).toEqual(newLogicalPlan);
  }));

  it('should execute the workflow plan and get the correct result in execute stream observable', marbles((m)=>{
    service.getExecuteEndedStream().subscribe(
      executionResult => {
        expect(executionResult).toEqual(MOCK_RESULT_DATA)
      }
    );

    service.executeWorkflow();

  }));
});
