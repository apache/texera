import { TestBed, inject } from '@angular/core/testing';

import { SavedProjectService } from './saved-project.service';

describe('SavedProjectService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [SavedProjectService]
    });
  });

  it('should be created', inject([SavedProjectService], (service: SavedProjectService) => {
    expect(service).toBeTruthy();
  }));
});
