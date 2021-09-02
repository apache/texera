import { TestBed } from "@angular/core/testing";

import { WorkflowConsoleService } from "./workflow-console.service";

describe("WorkflowConsoleService", () => {
  let service: WorkflowConsoleService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(WorkflowConsoleService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
