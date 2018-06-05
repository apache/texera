import { ExecuteWorkflowService } from './execute-workflow.service';
import { Injectable } from '@angular/core';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import './../../../common/rxjs-operators';
import { AppSettings } from './../../../common/app-setting';

import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { WorkflowGraph, WorkflowGraphReadonly } from './../workflow-graph/model/workflow-graph';
import { LogicalLink, LogicalPlan, LogicalOperator, ExecutionResult } from './../../types/workflow-execute.interface';

import { MOCK_EXECUTION_RESULT } from './mock-result-data';
import { MOCK_WORKFLOW_PLAN } from './mock-workflow-plan';

export const EXECUTE_WORKFLOW_ENDPOINT = 'queryplan/execute';


@Injectable()
export class StubExecuteWorkflowService extends ExecuteWorkflowService {

  public executeWorkflow(): void {
    const workflowPlan = MOCK_WORKFLOW_PLAN;

    const body = ExecuteWorkflowService.getLogicalPlanRequest(workflowPlan);

    this.executeStartedStream.next('execution started');
    Observable.of(MOCK_EXECUTION_RESULT)
      .subscribe(
        response => this.handleExecuteResult(response)
      );
  }

}
