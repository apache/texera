import { Injectable } from '@angular/core';
import * as Rx from 'rxjs';

@Injectable()
export class WebsocketService {
  private subject: Rx.Subject<string> | undefined;

  constructor() {}

  // establish a websocket connection to the given url
  public connect(url: string): Rx.Subject<string> {
    if (!this.subject) {
      console.log('Trying to connect to ', url);
      this.subject = this.create(url);
    }
    return this.subject;
  }

  // send out websocket connection request
  // bind the websocket object with a subject
  // return this subject to be used by other components/services
  private create(url: string): Rx.Subject<string> {
    const ws = new WebSocket(url);

    const observable = Rx.Observable.create((obs: Rx.Observer<string>) => {
      ws.onmessage = obs.next.bind(obs);
      ws.onerror = obs.error.bind(obs);
      ws.onclose = obs.complete.bind(obs);
      return ws.close.bind(ws);
    });

    const observer = {
      next: (data: Object) => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify(data));
        }
      }
    };
    return Rx.Subject.create(observer, observable);
  }
}
