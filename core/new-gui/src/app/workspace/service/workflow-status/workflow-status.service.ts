import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { WebsocketService } from '../websocket/websocket.service';

const Engine_URL = 'ws://localhost:8080/api/websocket';
// const Engine_URL = 'ws://echo.websocket.org/';

@Injectable()
export class WorkflowStatusService {
  public status: Subject<string>;
  public operatorStates: any;

  constructor(wsService: WebsocketService) {
    console.log('creating websocket to ', Engine_URL);
    this.status = <Subject<string>>wsService.connect(Engine_URL).map(
      (response: any): any => {
        const json = JSON.parse(response.data);
        console.log('this status is : ', json);
        this.operatorStates = json;
        // this.OperatorsInfo = json['operatorsInfo'];
        // this.operatorsInfoSubject.next(this.OperatorsInfo);
        return response;
      }
    );
  }

  public getStatusStream(): Observable<string> {
    return this.status.asObservable();
  }

  public getOperatorStates(): any {
    return this.operatorStates;
  }
}
