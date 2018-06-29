import { TestBed, inject } from '@angular/core/testing';

import { UserDictionaryService } from './user-dictionary.service';

import { HttpModule } from '@angular/http';

describe('UserDictionaryService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UserDictionaryService],
      imports: [HttpModule]
    });
  });

  it('should be created', inject([UserDictionaryService], (service: UserDictionaryService) => {
    expect(service).toBeTruthy();
  }));
});
