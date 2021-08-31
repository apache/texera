import { TestBed } from "@angular/core/testing";

import { WorkflowResultService } from "./workflow-result.service";

describe("WorkflowResultService", () => {
  let service: WorkflowResultService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(WorkflowResultService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
