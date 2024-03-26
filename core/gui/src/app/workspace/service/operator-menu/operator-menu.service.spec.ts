import { TestBed } from "@angular/core/testing";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";

import { OperatorMenuService } from "./operator-menu.service";

describe("OperatorMenuService", () => {
  let service: OperatorMenuService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [{ provide: OperatorMetadataService, useClass: StubOperatorMetadataService }],
    });
    service = TestBed.inject(OperatorMenuService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
