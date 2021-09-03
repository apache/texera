import { TestBed } from "@angular/core/testing";

import { OperatorCacheStatusService } from "./operator-cache-status.service";

describe("OperatorCacheStatusService", () => {
  let service: OperatorCacheStatusService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(OperatorCacheStatusService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
