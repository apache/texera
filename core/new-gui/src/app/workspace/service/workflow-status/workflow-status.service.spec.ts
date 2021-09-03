import { WorkflowStatusService } from "./workflow-status.service";

// TODO: this test case related to websocket is not stable, find out why and fix it
xdescribe("WorkflowStatusService", () => {
  // let workflowStatusService: WorkflowStatusService;
  // let mockBackend: Rx.Subject<string>;
  // let backendTester: Rx.Subject<string>;
  // beforeEach(() => {
  //   // this function creates a mock websocket connection
  //   // to a fake backend that we can monitor on
  //   function mockConnect(url: string) {
  //     mockBackend = new Rx.Subject<string>();
  //     backendTester = new Rx.Subject<string>();
  //     const observable = Rx.Observable.create((obs: Rx.Observer<string>) => {
  //       mockBackend.next = obs.next.bind(obs);
  //       mockBackend.error = obs.error.bind(obs);
  //       mockBackend.complete = obs.complete.bind(obs);
  //       return mockBackend.unsubscribe.bind(mockBackend);
  //     });
  //     const observer = {
  //       next: (data: Object) => {
  //         backendTester.next(JSON.stringify(data));
  //       }
  //     };
  //     return Rx.Subject.create(observer, observable);
  //   }
  //   TestBed.configureTestingModule({
  //     providers: [
  //       WorkflowStatusService,
  //     ]
  //   });
  //   // since webSocket is a function in rxjs/webSocket, it is very hard to spy on
  //   // I found the following way of replacing it with mockBackend online.
  //   const funcSpy = jasmine.createSpy('webSocket').and.returnValue(mockConnect('abc'));
  //   spyOnProperty(RxJSWebSocket, 'webSocket', 'get').and.returnValue(funcSpy);
  //   workflowStatusService = TestBed.get(WorkflowStatusService);
  // });
  // beforeAll(() => {
  //   environment.executionStatusEnabled = true;
  // });
  // afterAll(() => {
  //   environment.executionStatusEnabled = false;
  // });
  // it('should be created', inject([WorkflowStatusService], (service: WorkflowStatusService) => {
  //   expect(service).toBeTruthy();
  // }));
});
