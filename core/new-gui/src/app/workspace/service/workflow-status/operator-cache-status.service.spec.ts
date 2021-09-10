import { TestBed } from "@angular/core/testing";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";

import { OperatorCacheStatusService } from "./operator-cache-status.service";

xdescribe("OperatorCacheStatusService", () => {
  let service: OperatorCacheStatusService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
    });
    service = TestBed.inject(OperatorCacheStatusService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
