import { environment } from './../../../../environments/environment';
import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { OperatorStatistics } from '../../types/execute-workflow.interface';
import { WorkflowWebsocketService } from '../workflow-websocket/workflow-websocket.service';

const Engine_URL = 'ws://localhost:7070/api/websocket';

@Injectable()
export class WorkflowStatusService {
  // status is responsible for passing websocket responses to other components
  private status = new Subject<Record<string, OperatorStatistics>>();

  constructor(
    private workflowWebsocketService: WorkflowWebsocketService
  ) {
    if (! environment.executionStatusEnabled) {
      return;
    }
    this.workflowWebsocketService.websocketEvent().subscribe(event => {
      if (event.type !== 'WorkflowStatusUpdateEvent') {
        return;
      }
      this.status.next(event.operatorStatistics);
    });
  }

  public getStatusUpdateStream(): Observable<Record<string, OperatorStatistics>> {
    return this.status;
  }

}
