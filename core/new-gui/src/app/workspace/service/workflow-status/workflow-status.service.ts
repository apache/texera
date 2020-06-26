import { environment } from './../../../../environments/environment';
import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { SuccessProcessStatus } from '../../types/execute-workflow.interface';
import { webSocket } from 'rxjs/webSocket';

const Engine_URL = 'ws://localhost:7070/api/websocket';

@Injectable()
export class WorkflowStatusService {
  // connectionChannel is dedicated to communication with backend via websocket
  private connectionChannel: Subject<string> = new Subject<string>();
  // status is responsible for passing websocket responses to other components
  private status: Subject<SuccessProcessStatus> = new Subject<SuccessProcessStatus>();

  constructor() {
    if (! environment.executionStatusEnabled) {
      return;
    }
    this.connectionChannel = this.connect(Engine_URL);
    // within this.connectionChannel.subscribe function
    // the scope will no longer be websocketService
    // so this.status will be an error
    // solution: give "this" a differenct name
    const current = this;
    this.connectionChannel.subscribe({
      next(response) {
        const status = JSON.parse(JSON.stringify(response)) as SuccessProcessStatus;
        current.status.next(status);
      },
      error(err) { throw new Error(err); },
      complete() {console.log('websocket finished and disconected'); }
    });
  }

  // send workflowId via websocket to backend
  // the backend will response with real-time
  // updates on the status of the engine
  public checkStatus(workflowId: string) {
    this.connectionChannel.next(workflowId);
  }

  //
  public getStatusInformationStream(): Observable<SuccessProcessStatus> {
    return this.status;
  }

  private connect(URL: string) {
    return webSocket<string>(URL);
  }
}
