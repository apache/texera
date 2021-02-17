import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { environment } from '../../../../environments/environment';
import { ExecutionState, OperatorState, OperatorStatistics, ResultObject } from '../../types/execute-workflow.interface';
import { ExecuteWorkflowService } from '../execute-workflow/execute-workflow.service';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { WorkflowWebsocketService } from '../workflow-websocket/workflow-websocket.service';

@Injectable({
  providedIn: 'root'
})
export class WorkflowStatusService {
  // status is responsible for passing websocket responses to other components
  private statusSubject = new Subject<Record<string, OperatorStatistics>>();
  private currentStatus: Record<string, OperatorStatistics> = {};

  private resultSubject = new Subject<Record<string, ResultObject>>();
  private currentResult: Record<string, ResultObject> = {};

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {
    if (!environment.executionStatusEnabled) {
      return;
    }
    this.getStatusUpdateStream().subscribe(event => this.currentStatus = event);
    this.getResultUpdateStream().subscribe(event => this.currentResult = event);

    this.workflowWebsocketService.websocketEvent().subscribe(event => {
      if (event.type !== 'WorkflowStatusUpdateEvent') {
        return;
      }
      this.statusSubject.next(event.operatorStatistics);
    });

    this.workflowWebsocketService.websocketEvent().subscribe(event => {
      if (event.type !== 'WorkflowCompletedEvent') {
        return;
      }
      const results: Record<string, ResultObject> = {};
      event.result.forEach(r => {
        results[r.operatorID] = r;
      });
      if (Object.keys(results).length !== 0) {
        this.resultSubject.next(results);
      }
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
        this.statusSubject.next(initialStatistics);
      }
    });
  }

  public getStatusUpdateStream(): Observable<Record<string, OperatorStatistics>> {
    return this.statusSubject.asObservable();
  }

  public getCurrentStatus(): Record<string, OperatorStatistics> {
    return this.currentStatus;
  }

  public getResultUpdateStream(): Observable<Record<string, ResultObject>> {
    return this.resultSubject.asObservable();
  }

  public getCurrentResult(): Record<string, ResultObject> {
    return this.currentResult;
  }

}
