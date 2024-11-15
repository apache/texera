import { TestBed } from "@angular/core/testing";

import { WorkflowConsoleService } from "./workflow-console.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { HttpClientModule } from "@angular/common/http";

describe("WorkflowConsoleService", () => {
  let service: WorkflowConsoleService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [
        HttpClientModule, // Import the HttpClientModule to provide HttpClient
      ],
      providers: [
        WorkflowConsoleService, // Provide the service you're testing
        WorkflowWebsocketService, // Provide any dependent services
      ],
    });

    service = TestBed.inject(WorkflowConsoleService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
