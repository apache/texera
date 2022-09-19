import {TestBed} from "@angular/core/testing";

import {CoeditorPresenceService} from "./coeditor-presence.service";

describe("CoeditorPresenceService", () => {
  let service: CoeditorPresenceService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(CoeditorPresenceService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
