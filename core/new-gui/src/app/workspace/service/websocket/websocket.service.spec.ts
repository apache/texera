// import { TestBed, inject } from '@angular/core/testing';

// import { WebsocketService } from './websocket.service';

// describe('WebsocketService', () => {
//   let wsService: WebsocketService;
//   beforeEach(() => {
//     TestBed.configureTestingModule({
//       providers: [WebsocketService]
//     });

//     wsService = TestBed.get(WebsocketService);
//   });

//   fit('should be created', inject([WebsocketService], (service: WebsocketService) => {
//     expect(service).toBeTruthy();
//   }));

//   fit('should create a websocket connection', () => {
//     const test_url = 'wss://echo.websocket.org';
//     expect(wsService.connect(test_url)).toBeTruthy();
//   });
// });

import * as Rx from 'rxjs';
import { TestBed, inject} from '@angular/core/testing';
import 'rxjs/add/operator/zip';
import { WebsocketService } from './websocket.service';
import { Subject } from 'rxjs';

class MockWebSocket {
  onmessage: ((s: string) => void) | undefined;
  onerror: ((s: string) => void) | undefined;
  onclose: (() => void) | undefined;

  readyState: number | undefined;

  send = jasmine.createSpy('send');
  close = jasmine.createSpy('close');

  constructor (link: string) {
    const a = new Subject();
    const socketMock = {
      url: link,
      readyState: WebSocket.CONNECTING,
      send: jasmine.createSpy('send'),
      close: jasmine.createSpy('close').and.callFake(function () {
        socketMock.readyState = WebSocket.CLOSING;
      }),
      onmessage: () => a.next,
      onerror: () => a.next,
      onclose: () => a.next
    };
    return socketMock;
  }
}

class MockWebSocketService {
  // send out websocket connection request
  // bind the websocket object with a subject
  // return this subject to be used by other components/services
  public create(ws: MockWebSocket): Rx.Subject<string> {
    // const ws = new MockWebSocket(url);
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

describe('WebSocketService', () => {
  const url = 'ws://localhost:8888';
  let wsService: MockWebSocketService;
  let wsSubject: Subject<string>;
  let socketMock: MockWebSocket;

  beforeEach(() => {
    // WebSocketStub['OPEN'] = WebSocket.OPEN;
    // WebSocketStub['CLOSED'] = WebSocket.CLOSED;

    TestBed.configureTestingModule({
      providers: [
        WebsocketService,
        MockWebSocketService
      ]
    });

    wsService = TestBed.get(MockWebSocketService);
    socketMock = new MockWebSocket(url);
    wsSubject = wsService.create(socketMock);
  });

  it('should be created', inject([WebsocketService], (service: WebsocketService) => {
    expect(service).toBeTruthy();
  }));

  describe('subject.next()', () => {
    it('should send the message on the websocket.send', () => {
      const message = 'hello';

      wsSubject.next(message);

      expect(socketMock.send.calls.count()).toEqual(0);

      socketMock.readyState = WebSocket.OPEN;
      wsSubject.next(message);

      expect(socketMock.send.calls.count()).toEqual(1);
      expect(socketMock.send.calls.argsFor(0).values().next().value).toContain(message);
    });
  });

  describe('web socket onmessage()', () => {
    it('should call subscribe next callback', (done: DoneFn) => {
      const message = 'hello';

      wsSubject.subscribe(
        (s: any) => {
          expect(s).toEqual('hello');
          done();
        },
        () => {},
        () => {}
      );
      if (socketMock.onmessage) {
        socketMock.onmessage(message);
      }
    });
  });

  describe('web socket on onerror()', () => {
    it('should call subscribe error callback', (done: DoneFn) => {
      const err = 'hello';

      wsSubject.subscribe(
        () => {},
        (s: any) => {
          expect(s).toEqual(err);
          done();
        }
      );
      if (socketMock.onerror) {
        socketMock.onerror(err);
      }
    });
  });

  describe('web socket onclose()', () => {
    it('should call subscribe complete callback', (done: DoneFn) => {
      let a = 0;
      wsSubject.subscribe(
        () => {},
        () => {},
        () => {
          a = 1;
          done();
        }
      );
      if (socketMock.onclose) {
        socketMock.onclose();
      }
      expect(a).toBe(1);
    });
  });

  describe('web socket close()', () => {
    it('should call websocket close on unsubscribe', () => {
      expect(socketMock.close.calls.count()).toEqual(0);

      wsSubject.subscribe(
        () => {},
        () => {},
        () => {}
      ).unsubscribe();

      expect(socketMock.close.calls.count()).toEqual(1);
    });
  });
});
