import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { WorkflowAccessService } from "./workflow-access.service";

describe("WorkflowAccessService", () => {
  let service: WorkflowAccessService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowAccessService],
      imports: [HttpClientTestingModule],
    });
    service = TestBed.get(WorkflowAccessService);
    httpMock = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
