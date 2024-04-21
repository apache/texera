import { TestBed } from "@angular/core/testing";

import { PanelResizeService } from "./panel-resize.service";

describe("PanelResizeService", () => {
  let service: PanelResizeService;

  beforeEach(() => {
    service = TestBed.inject(PanelResizeService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
