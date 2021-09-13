import { inject, TestBed } from "@angular/core/testing";
import { ResultPanelToggleService } from "./result-panel-toggle.service";
import { marbles } from "rxjs-marbles";
import { map, tap } from "rxjs/operators";

describe("ResultPanelToggleService", () => {
  let resultPanelToggleService: ResultPanelToggleService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [ResultPanelToggleService],
    });

    resultPanelToggleService = TestBed.inject(ResultPanelToggleService);
  });

  it("should be created", inject([ResultPanelToggleService], (injectedservice: ResultPanelToggleService) => {
    expect(injectedservice).toBeTruthy();
  }));

  it(
    `should receive 'true' from toggleDisplayChangeStream when toggleResultPanel
    is called when the current result panel status is hidden`,
    marbles(m => {
      (resultPanelToggleService as any).currentResultPanelStatus = false;

      resultPanelToggleService.getToggleChangeStream().subscribe(newToggleStatus => {
        expect(newToggleStatus).toBeTruthy();
      });

      const expectedStream = "-a-";

      const toggleStream = resultPanelToggleService.getToggleChangeStream().pipe(map(value => "a"));
      m.hot("-a-")
        .pipe(tap(event => resultPanelToggleService.toggleResultPanel()))
        .subscribe();
      m.expect(toggleStream).toBeObservable(expectedStream);
    })
  );

  it(
    `should receive 'false' from toggleDisplayChangeStream when toggleResultPanel
    is called when the current result panel status is open`,
    marbles(m => {
      (resultPanelToggleService as any).currentResultPanelStatus = true;

      resultPanelToggleService.getToggleChangeStream().subscribe(newToggleStatus => {
        expect(newToggleStatus).toBeFalsy();
      });

      const expectedStream = "-a-";

      const toggleStream = resultPanelToggleService.getToggleChangeStream().pipe(map(value => "a"));
      m.hot("-a-")
        .pipe(tap(event => resultPanelToggleService.toggleResultPanel()))
        .subscribe();
      m.expect(toggleStream).toBeObservable(expectedStream);
    })
  );
});
