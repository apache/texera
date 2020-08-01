import { TestBed, inject } from '@angular/core/testing';

import { WorkflowStatusService } from './workflow-status.service';
import * as Rx from 'rxjs';
import { ProcessStatus } from '../../types/execute-workflow.interface';
import { mockStatus1 } from './mock-workflow-status';
import * as RxJSWebSocket from 'rxjs/webSocket';
import { environment } from './../../../../environments/environment';

// TODO: this test case related to websocket is not stable, find out why and fix it
xdescribe('WorkflowStatusService', () => {
  let workflowStatusService: WorkflowStatusService;
  let mockBackend: Rx.Subject<string>;
  let backendTester: Rx.Subject<string>;
  beforeEach(() => {
    // this function creates a mock websocket connection
    // to a fake backend that we can monitor on
    function mockConnect(url: string) {
      mockBackend = new Rx.Subject<string>();
      backendTester = new Rx.Subject<string>();
      const observable = Rx.Observable.create((obs: Rx.Observer<string>) => {
        mockBackend.next = obs.next.bind(obs);
        mockBackend.error = obs.error.bind(obs);
        mockBackend.complete = obs.complete.bind(obs);
        return mockBackend.unsubscribe.bind(mockBackend);
      });
      const observer = {
        next: (data: Object) => {
          backendTester.next(JSON.stringify(data));
        }
      };
      return Rx.Subject.create(observer, observable);
    }
    TestBed.configureTestingModule({
      providers: [
        WorkflowStatusService,
      ]
    });

    // since webSocket is a function in rxjs/webSocket, it is very hard to spy on
    // I found the following way of replacing it with mockBackend online.
    const funcSpy = jasmine.createSpy('webSocket').and.returnValue(mockConnect('abc'));
    spyOnProperty(RxJSWebSocket, 'webSocket', 'get').and.returnValue(funcSpy);
    workflowStatusService = TestBed.get(WorkflowStatusService);
  });

  beforeAll(() => {
    environment.executionStatusEnabled = true;
  });

  afterAll(() => {
    environment.executionStatusEnabled = false;
  });

  it('should be created', inject([WorkflowStatusService], (service: WorkflowStatusService) => {
    expect(service).toBeTruthy();
  }));

  it('should send workflow ID to websocket', (done: DoneFn) => {
    const mockId = '123456';
    backendTester.subscribe(
      (s: string) => {
        expect(s.toString()).toEqual(JSON.stringify(mockId));
        done();
      },
      () => {},
      () => {}
    );
    workflowStatusService.checkStatus(mockId);
  });

  it('should return a observable of operator status', () => {
    const test = workflowStatusService.getStatusInformationStream();
    expect(test).toBeDefined();
  });

  it('should receive responses from the backend and emits processStatus', (done: DoneFn) => {
    const stream = workflowStatusService.getStatusInformationStream();

    stream.subscribe(
      (status: ProcessStatus) => {
        expect(status.toString()).toEqual(expectedResponse);
        done();
      },
      () => {},
      () => {}
    );
    const expectedResponse = JSON.stringify(mockStatus1);
    mockBackend.next(expectedResponse);
  });
});
