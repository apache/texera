import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { WebsocketService } from '../websocket/websocket.service';

const Engine_URL = 'ws://localhost:8080/api/websocket';

@Injectable()
export class WorkflowStatusService {
  // connectionChannel is dedicated to communication with backend via websocket
  private connectionChannel: Subject<string>;
  // status is responsible for communication to other components
  private status: Subject<JSON> = new Subject<JSON>();


  constructor(wsService: WebsocketService) {
    console.log('creating websocket to ', Engine_URL);
    this.connectionChannel = <Subject<string>>wsService.connect(Engine_URL).map(
      (response: string): string => {
        console.log('received status from backend: ');
        const json = JSON.parse((response as any).data);
        this.status.next(json);
        return response;
      }
    );
    this.connectionChannel.subscribe();
  }

  // send a request via websocket to receive
  // real-time updates on the status of the engine
  public checkStatus(workflowId: string) {
    this.connectionChannel.next(workflowId);
  }

  // usage is shown below, need to do (status as any)
  // to access the fields of the JSON object
  public getStatusInformationStream(): Observable<JSON> {
    return this.status;
  }
  // workflowStatusService.getStatusInformationStream()
  //     .subscribe(status => {
  //       console.log((status as any)['OperatorState']);
  //       console.log((status as any)['ProcessedCount']);
  //     });
}
