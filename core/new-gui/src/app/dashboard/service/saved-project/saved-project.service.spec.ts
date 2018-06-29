import { TestBed, inject } from '@angular/core/testing';

import { SavedProjectService } from './saved-project.service';

import { HttpModule } from '@angular/http';

describe('SavedProjectService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [SavedProjectService],
      imports: [HttpModule]
    });
  });

  it('should be created', inject([SavedProjectService], (service: SavedProjectService) => {
    expect(service).toBeTruthy();
  }));
});
