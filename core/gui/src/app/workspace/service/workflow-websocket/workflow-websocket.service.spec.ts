import { TestBed } from "@angular/core/testing";
import { WorkflowWebsocketService } from "./workflow-websocket.service";

describe("WorkflowWebsocketService", () => {
  let service: WorkflowWebsocketService;

  beforeEach(() => {
    service = TestBed.inject(WorkflowWebsocketService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
