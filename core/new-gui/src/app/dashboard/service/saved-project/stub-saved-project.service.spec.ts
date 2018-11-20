import { TestBed, inject } from '@angular/core/testing';

import { StubSavedProjectService } from './stub-saved-project.service';

import { HttpClient } from '@angular/common/http';

class StubHttpClient {
  constructor() { }
}

describe('StubSavedProjectService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        StubSavedProjectService,
        { provide: HttpClient, useClass: StubHttpClient }
      ]
    });
  });

  it('should be created', inject([StubSavedProjectService], (service: StubSavedProjectService) => {
    expect(service).toBeTruthy();
  }));
});
