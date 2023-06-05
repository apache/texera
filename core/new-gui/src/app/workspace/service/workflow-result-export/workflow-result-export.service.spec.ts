import { TestBed } from "@angular/core/testing";

import { WorkflowResultExportService } from "./workflow-result-export.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { RouterTestingModule } from "@angular/router/testing";

describe("WorkflowResultExportService", () => {
  let service: WorkflowResultExportService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [RouterTestingModule, HttpClientTestingModule],
    });
    service = TestBed.inject(WorkflowResultExportService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
