import { TestBed, inject } from '@angular/core/testing';

import { SavedProjectService } from './saved-project.service';

import { HttpClient } from '@angular/common/http';

class StubHttpClient {
  constructor() { }
}

describe('SavedProjectService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        SavedProjectService,
        { provide: HttpClient, useClass: StubHttpClient }
      ]
    });
  });

  it('should be created', inject([SavedProjectService], (service: SavedProjectService) => {
    expect(service).toBeTruthy();
  }));
});
