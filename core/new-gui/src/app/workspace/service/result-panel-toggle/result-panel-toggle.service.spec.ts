import { TestBed, inject } from '@angular/core/testing';

import { ResultPanelToggleService } from './result-panel-toggle.service';

describe('ResultPanelToggleService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [ResultPanelToggleService]
    });
  });

  it('should be created', inject([ResultPanelToggleService], (service: ResultPanelToggleService) => {
    expect(service).toBeTruthy();
  }));
});
