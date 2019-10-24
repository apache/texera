import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { WebsocketService } from '../websocket/websocket.service';
import { ProcessStatus, SuccessProcessStatus } from '../../types/execute-workflow.interface';

const Engine_URL = 'ws://localhost:7070/api/websocket';

@Injectable()
export class WorkflowStatusService {
  // connectionChannel is dedicated to communication with backend via websocket
  private connectionChannel: Subject<string> = new Subject<string>();
  // status is responsible for communication to other components
  private status: Subject<SuccessProcessStatus> = new Subject<SuccessProcessStatus>();

  constructor(wsService: WebsocketService) {
    console.log('creating websocket to ', Engine_URL);

    this.connectionChannel = <Subject<string>>wsService.connect(Engine_URL);
    const current = this;
    this.connectionChannel.subscribe({
      next(response) {
        console.log('received status from backend: ');
        console.log(response);
        const json = JSON.parse((response as any).data) as SuccessProcessStatus;
        console.log(json);
        // current.status.next(json);
      },
      error(err) {console.log('websocket error occured: ' + err); },
      complete() {console.log('websocket finished and disconected'); }
    });

    // this.connectionChannel = <Subject<string>>wsService.connect(Engine_URL).map(
    //   (response: string): string => {
    //     console.log('received status from backend: ');
    //     console.log(response);

    //     const json = JSON.parse((response as any).data)['Result'] as SuccessProcessStatus;
    //     console.log(json.code);
    //     console.log((json as SuccessProcessStatus).OperatorStatus);
    //     console.log((json as SuccessProcessStatus).OperatorStatistics);

    //     // this.status.next(json);

    //     return response;
    //   }
    // );

    // this.connectionChannel.subscribe({
    //   next(response) {
    //     console.log('received status from backend: ');
    //     const json = JSON.parse((response as any).data)['Result'] as SuccessProcessStatus;
    //     return json;
    //   },
    //   error(err) {console.log('websocket error occured: ' + err); },
    //   complete() {console.log('websocket finished and disconected'); }
    // });
  }

  // send a request via websocket to receive
  // real-time updates on the status of the engine
  public checkStatus(workflowId: string) {
    this.connectionChannel.next(workflowId);
  }

  // usage is shown below, need to do (status as any)
  // to access the fields of the JSON object
  public getStatusInformationStream(): Observable<SuccessProcessStatus> {
    return this.status;
  }
}
