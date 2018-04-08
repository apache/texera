import { TestBed, inject } from '@angular/core/testing';

import { UserDictionaryService } from './user-dictionary.service';

describe('UserDictionaryService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UserDictionaryService]
    });
  });

  it('should be created', inject([UserDictionaryService], (service: UserDictionaryService) => {
    expect(service).toBeTruthy();
  }));
});
