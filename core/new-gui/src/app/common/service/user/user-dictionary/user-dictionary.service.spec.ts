import { TestBed, inject } from '@angular/core/testing';

import { UserDictionaryService } from './user-dictionary.service';

import { Observable } from 'rxjs/Observable';
import { HttpClient } from '@angular/common/http';
import { marbles} from 'rxjs-marbles';
import { UserAccountService } from '../user-account/user-account.service';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

describe('UserDictionaryService', () => {

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        UserAccountService,
        UserDictionaryService,
      ],
      imports: [
        HttpClientTestingModule
      ]
    });
  });

  afterEach(inject([HttpTestingController], (httpMock: HttpTestingController) => {
    httpMock.verify();
  }));

  it('should be created', inject([UserDictionaryService], (InjectableService: UserDictionaryService) => {
    expect(InjectableService).toBeTruthy();
  }));

  it('should contain no files by default', inject([UserDictionaryService, UserAccountService, HttpTestingController],
    (service: UserDictionaryService) => {
    expect(service.getDictionaryArrayLength()).toBe(0);
  }));
});
