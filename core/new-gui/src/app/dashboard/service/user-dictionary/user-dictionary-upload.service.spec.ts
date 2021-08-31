// import { TestBed } from '@angular/core/testing';
// import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
// import { AppSettings } from '../../../app-setting';

// import {
//   UserDictionaryUploadService, USER_DICTIONARY_UPLOAD_URL, USER_MANUAL_DICTIONARY_UPLOAD_URL, USER_DICTIONARY_VALIDATE_URL
// } from './user-dictionary-upload.service';
// import { UserService } from '../user.service';
// import { UserDictionaryService } from './user-dictionary.service';
// import { StubUserService } from '../stub-user.service';
// import { ManualDictionaryUploadItem } from 'src/app/common/type/user-dictionary';

// const arrayOfBlob: Blob[] = Array<Blob>(); // just for test,needed for creating File object.
// const testDictionaryName = 'testTextDictionary';
// const testTextDictionary: File = new File(arrayOfBlob, testDictionaryName, { type: 'text/plain' });

// const manual_dict_name = 'testDict';
// const content = 'this,is,a,test,dictionary';
// const separator = ',';
// const description = 'this is a test dictionary';

// describe('UserDictionaryUploadService', () => {

//   let service: UserDictionaryUploadService;
//   let httpMock: HttpTestingController;

//   beforeEach(() => {
//     TestBed.configureTestingModule({
//       providers: [
//         { provide: UserService, useClass: StubUserService },
//         UserDictionaryService,
//         UserDictionaryUploadService
//       ],
//       imports: [
//         HttpClientTestingModule
//       ]
//     });
//     service = TestBed.get(UserDictionaryUploadService);
//     httpMock = TestBed.get(HttpTestingController);
//   });

//   afterEach(() => {
//     httpMock.verify();
//   });

//   it('should be created', () => {
//     expect(service).toBeTruthy();
//   });

//   it('should contain no dictionary by default', () => {
//     expect(service.getDictionariesToBeUploaded().length).toBe(0);
//   });

//   it('should insert dictionary successfully', () => {
//     service.addDictionaryToUploadArray(testTextDictionary);
//     expect(service.getDictionariesToBeUploaded().length).toBe(1);
//     expect(service.getDictionariesToBeUploaded()[0].file).toEqual(testTextDictionary);
//     expect(service.getDictionariesToBeUploaded()[0].name).toEqual(testTextDictionary.name);
//     expect(service.getDictionariesToBeUploaded()[0].isUploadingFlag).toBeFalsy();
//   });

//   it('should delete dictionary successfully', () => {
//     service.addDictionaryToUploadArray(testTextDictionary);
//     expect(service.getDictionariesToBeUploaded().length).toBe(1);
//     const testTextDictionaryUploadItem = service.getDictionariesToBeUploaded()[0];
//     service.removeFileFromUploadArray(testTextDictionaryUploadItem);
//     expect(service.getDictionariesToBeUploaded().length).toBe(0);
//   });

//   it('should upload dictionary successfully', () => {
//     service.addDictionaryToUploadArray(testTextDictionary);
//     expect(service.getDictionariesToBeUploaded().length).toBe(1);

//     const userDictionaryService = TestBed.get(UserDictionaryService);
//     const spy = spyOn(userDictionaryService, 'refreshDictionaries');

//     service.uploadAllDictionaries();

//     const req1 = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${USER_DICTIONARY_VALIDATE_URL}`);
//     expect(req1.request.method).toEqual('POST');
//     req1.flush({ code: 0, message: '' });

//     const req2 = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${USER_DICTIONARY_UPLOAD_URL}`);
//     expect(req2.request.method).toEqual('POST');
//     req2.flush({ code: 0, message: '' });

//     expect(spy).toHaveBeenCalled();
//   });

//   it('should upload manual dictionary successfully', () => {
//     const userDictionaryService = TestBed.get(UserDictionaryService);
//     const spy = spyOn(userDictionaryService, 'refreshDictionaries');

//     const manualDictionary: ManualDictionaryUploadItem = service.getManualDictionary();
//     manualDictionary.name = manual_dict_name;
//     manualDictionary.content = content;
//     manualDictionary.separator = separator;
//     manualDictionary.description = description;

//     service.uploadManualDictionary();

//     const req1 = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${USER_MANUAL_DICTIONARY_UPLOAD_URL}`);
//     expect(req1.request.method).toEqual('POST');
//     req1.flush({ code: 0, message: '' });

//     expect(spy).toHaveBeenCalled();
//   });

// });
