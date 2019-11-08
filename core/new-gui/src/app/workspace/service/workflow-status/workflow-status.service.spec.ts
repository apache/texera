import { TestBed, inject } from '@angular/core/testing';

import { WorkflowStatusService } from './workflow-status.service';
import { WebsocketService } from '../websocket/websocket.service';
import * as Rx from 'rxjs';
import { SuccessProcessStatus, OperatorStates } from '../../types/execute-workflow.interface';

describe('WorkflowStatusService', () => {
  let workflowStatusService: WorkflowStatusService;
  let mockBackend: Rx.Subject<string>;
  let backendTester: Rx.Subject<string>;
  beforeEach(() => {
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
        {
          provide: WebsocketService,
          useValue: {connect: mockConnect}
        }
      ]
    });
    workflowStatusService = TestBed.get(WorkflowStatusService);
  });

  it('should be created', inject([WorkflowStatusService], (service: WorkflowStatusService) => {
    expect(service).toBeTruthy();
  }));

  describe('WorkflowStatusService.checkStatus()', () => {
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
  });

  describe('WorkflowStatusService.getStatusInformationStream()', () => {
    it('should return a observable', () => {
      let test;
      expect(test).toBeUndefined();
      test = workflowStatusService.getStatusInformationStream();
      expect(test).toBeDefined();
    });

    // unable to access data field of the JSON object
    it('should emit responses from the backend', (done: DoneFn) => {
      const stream = workflowStatusService.getStatusInformationStream();
      const mockStatus = {
        code: 0,
        message: 'mock message',
        data: 'mock data'
      };
      stream.subscribe(
        (status: SuccessProcessStatus) => {
          expect(true).toBeFalsy();
          // expect(status).toBe(mockStatus);
          done();
        },
        () => {},
        () => {}
      );

      console.log('data is', mockStatus.data);
      // mockBackend.next(JSON.stringify(mockStatus));
    });
  });
});
