import { ObserversModule } from '@angular/cdk/observers';
import { TestBed, inject } from '@angular/core/testing';
import { Observable } from 'rxjs/Observable';
import { ResultPanelToggleService } from './result-panel-toggle.service';

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
     is called when the current result panel status is hidden`, () => {

    const openResultPanelSpy = spyOn(resultPanelToggleService, 'openResultPanel');

    resultPanelToggleService.getToggleChangeStream().subscribe(
      newToggleStatus => {
        expect(newToggleStatus).toBeTruthy();
      }
    );

    const hiddenStatus = false;
    resultPanelToggleService.toggleResultPanel(hiddenStatus);
    expect(openResultPanelSpy).toHaveBeenCalled();

  });


  it(`should receive 'false' from toggleDisplayChangeStream when toggleResultPanel
     is called when the current result panel status is open`, () => {

    const closeResultPanelSpy = spyOn(resultPanelToggleService, 'closeResultPanel');

    resultPanelToggleService.getToggleChangeStream().subscribe(
      newToggleStatus => {
        expect(newToggleStatus).toBeFalsy();
      }
    );

    const openStatus = true;
    resultPanelToggleService.toggleResultPanel(openStatus);
    expect(closeResultPanelSpy).toHaveBeenCalled();

  });



});
