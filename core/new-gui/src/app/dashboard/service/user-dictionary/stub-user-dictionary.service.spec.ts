import { TestBed, inject } from '@angular/core/testing';

import { StubUserDictionaryService } from './stub-user-dictionary.service';

import { HttpModule } from '@angular/http';

describe('StubUserDictionaryService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [StubUserDictionaryService],
      imports: [HttpModule]
    });
  });

  it('should be created', inject([StubUserDictionaryService], (service: StubUserDictionaryService) => {
    expect(service).toBeTruthy();
  }));
});
