import { TestBed } from "@angular/core/testing";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";

import { OperatorReuseCacheStatusService } from "./operator-reuse-cache-status.service";
import { HttpClientModule } from "@angular/common/http";

describe("OperatorCacheStatusService", () => {
  let service: OperatorReuseCacheStatusService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
      imports: [HttpClientModule],
    });
    service = TestBed.inject(OperatorReuseCacheStatusService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
