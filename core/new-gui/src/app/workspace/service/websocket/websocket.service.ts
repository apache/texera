import { Injectable } from '@angular/core';
import * as Rx from 'rxjs';
import { webSocket } from 'rxjs/webSocket';

@Injectable()
export class WebsocketService {
  private subject: Rx.Subject<string> | undefined;
  constructor() {}

  // establish a websocket connection to the given url
  public connect(url: string): Rx.Subject<string> {
    // const ws = webSocket<string>('ws://');
    if (!this.subject) {
      this.subject = webSocket(url);
    }
    return this.subject;
  }
}
