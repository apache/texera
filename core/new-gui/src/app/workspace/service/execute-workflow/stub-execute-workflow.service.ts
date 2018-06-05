import { ExecuteWorkflowService } from './execute-workflow.service';
import { Injectable } from '@angular/core';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import './../../../common/rxjs-operators';
import { AppSettings } from './../../../common/app-setting';

import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { WorkflowGraph, WorkflowGraphReadonly } from './../workflow-graph/model/workflow-graph';
import { LogicalLink, LogicalPlan, LogicalOperator, ExecutionResult } from './../../types/workflow-execute.interface';

import { mockExecutionResult } from './mock-result-data';
import { mockWorkflowPlan } from './mock-workflow-plan';

export const EXECUTE_WORKFLOW_ENDPOINT = 'queryplan/execute';

/**
 * This class is mainly use for testing purposes.
 * It inherits all the functionality from the ExecuteWorkflowService class.
 *
 * To avoid sending an actual http request during testing,
 *  we overload the executeWorkflow() function to send
 *  the mock execution result instead.
 */
@Injectable()
export class StubExecuteWorkflowService extends ExecuteWorkflowService {

  public executeWorkflow(): void {
    const workflowPlan = mockWorkflowPlan;

    const body = ExecuteWorkflowService.getLogicalPlanRequest(workflowPlan);

    this.executeStartedStream.next('execution started');
    Observable.of(mockExecutionResult)
      .subscribe(
        response => this.handleExecuteResult(response)
      );
  }

}
