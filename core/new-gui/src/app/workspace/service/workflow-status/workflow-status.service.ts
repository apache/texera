import { environment } from './../../../../environments/environment';
import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { OperatorStatistics, ExecutionState, OperatorState } from '../../types/execute-workflow.interface';
import { WorkflowWebsocketService } from '../workflow-websocket/workflow-websocket.service';
import { ExecuteWorkflowService } from '../execute-workflow/execute-workflow.service';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';


@Injectable({
  providedIn: 'root'
})
export class WorkflowStatusService {
  // status is responsible for passing websocket responses to other components
  private status = new Subject<Record<string, OperatorStatistics>>();

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {
    if (!environment.executionStatusEnabled) {
      return;
    }
    this.workflowWebsocketService.websocketEvent().subscribe(event => {
      if (event.type !== 'WorkflowStatusUpdateEvent') {
        return;
      }
      this.status.next(event.operatorStatistics);
    });
    this.executeWorkflowService.getExecutionStateStream().subscribe(event => {
      if (event.current.state === ExecutionState.WaitingToRun) {
        const initialStatistics: Record<string, OperatorStatistics> = {};
        this.workflowActionService.getTexeraGraph().getAllOperators().forEach(op => {
          initialStatistics[op.operatorID] = {
            operatorState: OperatorState.Initializing,
            aggregatedInputRowCount: 0,
            aggregatedOutputRowCount: 0
          };
        });
        this.status.next(initialStatistics);
      }
    });
  }

  public getStatusUpdateStream(): Observable<Record<string, OperatorStatistics>> {
    return this.status;
  }

}
