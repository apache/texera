import { TestBed, inject } from '@angular/core/testing';

import { StubSavedProjectService } from './stub-saved-project.service';

import { HttpModule } from '@angular/http';

describe('StubSavedProjectService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [StubSavedProjectService],
      imports: [HttpModule]
    });
  });

  it('should be created', inject([StubSavedProjectService], (service: StubSavedProjectService) => {
    expect(service).toBeTruthy();
  }));
});
