import { TestBed, inject } from '@angular/core/testing';

import { StubSavedProjectService } from './stub-saved-project.service';

describe('StubSavedProjectService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [StubSavedProjectService]
    });
  });

  it('should be created', inject([StubSavedProjectService], (service: StubSavedProjectService) => {
    expect(service).toBeTruthy();
  }));
});
