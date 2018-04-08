import { TestBed, inject } from '@angular/core/testing';

import { StubUserDictionaryService } from './stub-user-dictionary.service';

describe('StubUserDictionaryService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [StubUserDictionaryService]
    });
  });

  it('should be created', inject([StubUserDictionaryService], (service: StubUserDictionaryService) => {
    expect(service).toBeTruthy();
  }));
});
