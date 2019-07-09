import { Injectable } from '@angular/core';
import * as Rx from 'rxjs';

@Injectable()
export class WebsocketService {
  private subject = new Rx.Subject<string>();

  constructor() {}

  public connect(url: string): Rx.Subject<string> {
    // if (!this.subject) {
    console.log('Trying to connect to ', url);
    this.subject = this.create(url);
    console.log('Successfully connected: ' + url);
    // }
    return this.subject;
  }

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
