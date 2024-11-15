import { TestBed } from "@angular/core/testing";
import { WorkflowWebsocketService } from "./workflow-websocket.service";
import { HttpClientModule } from "@angular/common/http";

describe("WorkflowWebsocketService", () => {
  let service: WorkflowWebsocketService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientModule],
      providers: [WorkflowWebsocketService],
    });
    service = TestBed.inject(WorkflowWebsocketService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
