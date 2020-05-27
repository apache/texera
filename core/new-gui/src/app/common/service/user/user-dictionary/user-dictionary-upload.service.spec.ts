import { TestBed, inject } from '@angular/core/testing';

import { UserDictionaryUploadService } from './user-dictionary-upload.service';
import { UserAccountService } from '../user-account/user-account.service';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { UserDictionaryService } from './user-dictionary.service';
import { StubUserAccountService } from '../user-account/stub-user-account.service';
import { StubUserDictionaryService } from './stub-user-dictionary.service';

const arrayOfBlob: Blob[] = Array<Blob>(); // just for test,needed for creating File object.
const testDictionaryNameA = 'testTextDictionary';
const testDictionaryNameC = 'testJPGFile';
const testTextDictionaryA: File = new File( arrayOfBlob, testDictionaryNameA, {type: 'text/plain'});
const testTextDictionaryB: File = new File( arrayOfBlob, testDictionaryNameA, {type: 'text/plain'});
const testPictureDictionaryC: File = new File( arrayOfBlob, testDictionaryNameC, {type: 'jpg'});

describe('UserDictionaryUploadService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        UserDictionaryUploadService,
        { provide: UserAccountService, useClass: StubUserAccountService },
        { provide: UserDictionaryService, useClass: StubUserDictionaryService },
      ],
      imports: [
        HttpClientTestingModule
      ]
    });
  });

  afterEach(inject([HttpTestingController], (httpMock: HttpTestingController) => {
    httpMock.verify();
  }));

  it('should be created', inject([UserDictionaryUploadService, UserAccountService, UserDictionaryService, HttpTestingController],
    (service: UserDictionaryUploadService) => {
    expect(service).toBeTruthy();
  }));

  it('should contain no dictionary by default', inject([UserDictionaryUploadService, UserAccountService, UserDictionaryService, HttpTestingController],
    (service: UserDictionaryUploadService, userAccountService: UserAccountService) => {
      userAccountService.loginUser('').subscribe();
      expect(service.getDictionaryArrayLength()).toBe(0);
      expect(() => service.getDictionaryUploadItem(0)).toThrowError();
      expect(service.manualDictionary).toEqual({
        name : '',
        content: '',
        separator: '',
        description: ''
      });
  }));

  it('should contain no dictionary by default', inject([UserDictionaryUploadService, UserAccountService, UserDictionaryService, HttpTestingController],
    (service: UserDictionaryUploadService, userAccountService: UserAccountService) => {
      userAccountService.loginUser('').subscribe();
      expect(service.getDictionaryArrayLength()).toBe(0);
      expect(() => service.getDictionaryUploadItem(0)).toThrowError();
      expect(service.manualDictionary).toEqual({
        name : '',
        content: '',
        separator: '',
        description: ''
      });
  }));

  it('should insert file successfully', inject([UserDictionaryUploadService, UserAccountService, UserDictionaryService, HttpTestingController],
    (service: UserDictionaryUploadService, userAccountService: UserAccountService) => {
      userAccountService.loginUser('').subscribe();
      expect(service.getDictionaryArrayLength()).toBe(0);
      service.insertNewDictionary(testTextDictionaryA);
      expect(service.getDictionaryArrayLength()).toEqual(1);
      expect(service.getDictionaryUploadItem(0).file).toEqual(testTextDictionaryA);
      expect(service.getDictionaryUploadItem(0).name).toEqual(testDictionaryNameA);
      expect(service.getDictionaryUploadItem(0).description).toEqual('');
      expect(service.getDictionaryArray()[0]).toEqual(service.getDictionaryUploadItem(0));
  }));

  it('should insert multiple file successfully', inject([UserDictionaryUploadService, UserAccountService, UserDictionaryService, HttpTestingController],
    (service: UserDictionaryUploadService, userAccountService: UserAccountService) => {
      userAccountService.loginUser('').subscribe();
      expect(service.getDictionaryArrayLength()).toBe(0);
      service.insertNewDictionary(testTextDictionaryA);
      service.insertNewDictionary(testTextDictionaryB);
      service.insertNewDictionary(testPictureDictionaryC);
      expect(service.getDictionaryArrayLength()).toEqual(3);
      expect(service.getDictionaryUploadItem(0).file).toEqual(testTextDictionaryA);
      expect(service.getDictionaryUploadItem(1).file).toEqual(testTextDictionaryB);
      expect(service.getDictionaryUploadItem(2).file).toEqual(testPictureDictionaryC);
      expect(() => service.getDictionaryUploadItem(3)).toThrowError();
  }));

  it('should valid for single text file', inject([UserDictionaryUploadService, UserAccountService, UserDictionaryService, HttpTestingController],
    (service: UserDictionaryUploadService, userAccountService: UserAccountService) => {
      userAccountService.loginUser('').subscribe();
      expect(service.getDictionaryArrayLength()).toBe(0);
      service.insertNewDictionary(testTextDictionaryA);
      expect(service.getDictionaryArrayLength()).toEqual(1);
      expect(service.isItemValid(service.getDictionaryUploadItem(0))).toBeTruthy();
      expect(service.isAllItemsValid()).toBeTruthy();
  }));

  it('should invalid for non text file', inject([UserDictionaryUploadService, UserAccountService, UserDictionaryService, HttpTestingController],
    (service: UserDictionaryUploadService, userAccountService: UserAccountService) => {
      userAccountService.loginUser('').subscribe();
      expect(service.getDictionaryArrayLength()).toBe(0);
      service.insertNewDictionary(testPictureDictionaryC);
      expect(service.isItemValid(service.getDictionaryUploadItem(0))).toBeFalsy();
      expect(service.isAllItemsValid()).toBeFalsy();
  }));

  it('should invalid for duplicate file name', inject([UserDictionaryUploadService, UserAccountService, UserDictionaryService, HttpTestingController],
    (service: UserDictionaryUploadService, userAccountService: UserAccountService) => {
      userAccountService.loginUser('').subscribe();
      expect(service.getDictionaryArrayLength()).toBe(0);
      service.insertNewDictionary(testTextDictionaryA);
      expect(service.isItemValid(service.getDictionaryUploadItem(0))).toBeTruthy();
      expect(service.isItemNameUnique(service.getDictionaryUploadItem(0))).toBeTruthy();
      expect(service.isAllItemsValid()).toBeTruthy();
      service.insertNewDictionary(testTextDictionaryB);
      expect(service.isItemNameUnique(service.getDictionaryUploadItem(0))).toBeFalsy();
      expect(service.isItemValid(service.getDictionaryUploadItem(0))).toBeFalsy();
      expect(service.isAllItemsValid()).toBeFalsy();
  }));

  it('should delete file successfully', inject([UserDictionaryUploadService, UserAccountService, UserDictionaryService, HttpTestingController],
    (service: UserDictionaryUploadService, userAccountService: UserAccountService) => {
      userAccountService.loginUser('').subscribe();
      expect(service.getDictionaryArrayLength()).toBe(0);
      service.insertNewDictionary(testTextDictionaryA);
      service.insertNewDictionary(testTextDictionaryB);
      service.insertNewDictionary(testPictureDictionaryC);
      expect(service.getDictionaryArrayLength()).toBe(3);
      service.deleteDictionary(service.getDictionaryUploadItem(1));
      expect(service.getDictionaryArrayLength()).toBe(2);
      expect(service.getDictionaryUploadItem(0).file).toEqual(testTextDictionaryA);
      expect(service.getDictionaryUploadItem(1).file).toEqual(testPictureDictionaryC);
  }));
});
