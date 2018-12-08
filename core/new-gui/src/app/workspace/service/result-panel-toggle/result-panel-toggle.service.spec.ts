import { ObserversModule } from '@angular/cdk/observers';
import { TestBed, inject } from '@angular/core/testing';
import { Observable } from 'rxjs/Observable';
import { ResultPanelToggleService } from './result-panel-toggle.service';

describe('ResultPanelToggleService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        ResultPanelToggleService,
      ]
    });
  });


  it('should be created', inject([ResultPanelToggleService], (injectedservice: ResultPanelToggleService) => {
    expect(injectedservice).toBeTruthy();
  }));

});
