// import { TestBed, inject } from '@angular/core/testing';
// import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

// import { UserDictionaryService, USER_DICTIONARY_LIST_URL } from './user-dictionary.service';
// import { UserService } from '../user.service';
// // import { StubUserService, MOCK_USER } from '../stub-user.service';
// import { AppSettings } from 'src/app/common/app-setting';
// import { UserDictionary } from 'src/app/common/type/user-dictionary';

// const id = 1;
// const name = 'testFile';
// const items = ['this', 'is', 'a', 'test', 'file'];
// const description = 'this is a test file';
// const testDictionary: UserDictionary = {
//   id: id,
//   name: name,
//   items: items,
//   description: description
// };

// describe('UserDictionaryService', () => {

//   let httpMock: HttpTestingController;
//   let service: UserDictionaryService;

//   beforeEach(() => {
//     TestBed.configureTestingModule({
//       providers: [
//         // { provide: UserService, useClass: StubUserService },
//         UserDictionaryService
//       ],
//       imports: [
//         HttpClientTestingModule
//       ]
//     });
//     httpMock = TestBed.get(HttpTestingController);
//     service = TestBed.get(UserDictionaryService);
//   });

//   afterEach(() => {
//     httpMock.verify();
//   });

//   it('should be created', () => {
//     expect(service).toBeTruthy();
//   });

//   it('should contain no files by default', () => {
//     expect(service.getUserDictionaries()).toBeFalsy();
//   });

//   it('should refresh file after user login', () => {
//     const spy = spyOn(service, 'refreshDictionaries').and.callThrough();

//     // const stubUserService: StubUserService = TestBed.get(UserService);
//     // stubUserService.userChangedEvent.next(MOCK_USER);

//     expect(spy).toHaveBeenCalled();

//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${USER_DICTIONARY_LIST_URL}`);
//     expect(req.request.method).toEqual('GET');
//     req.flush([testDictionary]);
//   });

// });
