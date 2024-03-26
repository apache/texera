import { TestBed } from "@angular/core/testing";

import { WorkflowResultExportService } from "./workflow-result-export.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";

describe("WorkflowResultExportService", () => {
  let service: WorkflowResultExportService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
    });
    service = TestBed.inject(WorkflowResultExportService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
