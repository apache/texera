import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { WebsocketService } from '../websocket/websocket.service';

const Engine_URL = 'ws://localhost:8080/api/websocket';
// const Engine_URL = 'ws://echo.websocket.org/';

@Injectable()
export class WorkflowStatusService {
  public status: Subject<string>;

  private operatorsInfoSubject: Subject<JSON> = new Subject<JSON>();
  private OperatorsInfo: JSON | undefined;

  constructor(wsService: WebsocketService) {
    console.log('creating websocket to ', Engine_URL);
    this.status = <Subject<string>>wsService.connect(Engine_URL).map(
      (response: any): any => {
        const json = JSON.parse(response.data);
        console.log('this status is : ', json);
        this.OperatorsInfo = json['operatorsInfo'];
        this.operatorsInfoSubject.next(this.OperatorsInfo);
        return response;
      }
    );
  }

  public getOperatorInfo(): JSON | undefined {
    return this.OperatorsInfo;
  }

  public getOperatorsInfoSubjectStream(): Observable<JSON> {
    return this.operatorsInfoSubject.asObservable();
  }

  public getStatusStream(): Observable<string> {
    return this.status.asObservable();
  }
}
