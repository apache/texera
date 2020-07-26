import { Injectable } from '@angular/core';
import {
  TexeraWebsocketRequest, TexeraWebsocketEvent, TexeraWebsocketRequestTypeMap, TexeraWebsocketRequestTypes
} from '../../types/workflow-ws.interface';
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class WorkflowWebsocketService {

  public static readonly TEXERA_WEBSOCKET_ENDPOINT = 'ws://localhost:8080/wsapi/workflow-websocket';

  private readonly websocket: WebSocketSubject<TexeraWebsocketEvent | TexeraWebsocketRequest>;

  constructor() {
    console.log('web socket service constructor!!!');
    this.websocket = webSocket(WorkflowWebsocketService.TEXERA_WEBSOCKET_ENDPOINT);
    this.websocket.subscribe(data => {
      console.log(data);
      if (data.type === 'HelloWorldResponse') {
        console.log('hello world works: ' + data.message);
      }
    });
    this.send('HelloWorldRequest', {message: 'Texera on Amber'});
  }

  public websocketEvent(): Observable<TexeraWebsocketEvent> {
    return this.websocket.asObservable() as Observable<TexeraWebsocketEvent>;
  }

  public send<T extends TexeraWebsocketRequestTypes>(type: T, payload: TexeraWebsocketRequestTypeMap[T]): void {
    const request = {
      type,
      ...payload
    } as any as TexeraWebsocketRequest ;
    this.websocket.next(request);
  }


}
