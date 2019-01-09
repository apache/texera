import { TestBed, inject } from '@angular/core/testing';
import { ResultPanelToggleService } from './result-panel-toggle.service';
import { marbles } from 'rxjs-marbles';

describe('ResultPanelToggleService', () => {
  let resultPanelToggleService: ResultPanelToggleService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        ResultPanelToggleService,
      ]
    });

    resultPanelToggleService = TestBed.get(ResultPanelToggleService);

  });

  it('should be created', inject([ResultPanelToggleService], (injectedservice: ResultPanelToggleService) => {
    expect(injectedservice).toBeTruthy();
  }));

  it(`should receive 'true' from toggleDisplayChangeStream when toggleResultPanel
    is called when the current result panel status is hidden`, marbles((m) => {

    resultPanelToggleService.getToggleChangeStream().subscribe(
      newToggleStatus => {
        expect(newToggleStatus).toBeTruthy();
      }
    );

    const expectedStream = '-a-';
    const hiddenStatus = false;

    const toggleStream = resultPanelToggleService.getToggleChangeStream().map(value => 'a');
    m.hot('-a-').do(event => resultPanelToggleService.toggleResultPanel(hiddenStatus)).subscribe();
    m.expect(toggleStream).toBeObservable(expectedStream);

  }));

  it(`should receive 'false' from toggleDisplayChangeStream when toggleResultPanel
    is called when the current result panel status is open`, marbles((m) => {

    resultPanelToggleService.getToggleChangeStream().subscribe(
      newToggleStatus => {
        expect(newToggleStatus).toBeFalsy();
      }
    );

    const expectedStream = '-a-';
    const openStatus = true;

    const toggleStream = resultPanelToggleService.getToggleChangeStream().map(value => 'a');
    m.hot('-a-').do(event => resultPanelToggleService.toggleResultPanel(openStatus)).subscribe();
    m.expect(toggleStream).toBeObservable(expectedStream);

  }));

});
