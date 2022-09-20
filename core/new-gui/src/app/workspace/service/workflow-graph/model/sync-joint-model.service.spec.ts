import { TestBed } from "@angular/core/testing";

import { SyncJointModelService } from "./sync-joint-model.service";

describe("SyncJointModelServiceService", () => {
  let service: SyncJointModelService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(SyncJointModelService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
