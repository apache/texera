// import { TestBed } from '@angular/core/testing';

// import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
// import { UserFile } from '../../../type/user-file';

// import { UserFileService, USER_FILE_LIST_URL } from './user-file.service';
// import { UserService } from '../user.service';
// import { StubUserService, MOCK_USER } from '../stub-user.service';
// import { AppSettings } from 'src/app/common/app-setting';

// const id = 1;
// const name = 'testFile';
// const path = 'test/path';
// const description = 'this is a test file';
// const size = 1024;
// const testFile: UserFile = {
//   id: id,
//   name: name,
//   path: path,
//   size: size,
//   description: description
// };

// describe('UserFileService', () => {
//   let httpMock: HttpTestingController;
//   let service: UserFileService;

//   beforeEach(() => {
//     TestBed.configureTestingModule({
//       providers: [
//         UserFileService,
//         { provide: UserService, useClass: StubUserService }
//       ],
//       imports: [
//         HttpClientTestingModule
//       ]
//     });
//     httpMock = TestBed.get(HttpTestingController);
//     service = TestBed.get(UserFileService);
//   });

//   afterEach(() => {
//     httpMock.verify();
//   });

//   it('should be created', () => {
//     expect(service).toBeTruthy();
//   });

//   it('should contain no files by default', () => {
//     expect(service.getUserFiles()).toBeFalsy();
//   });


//   it('should refresh file after user login', () => {
//     const spy = spyOn(service, 'refreshFiles').and.callThrough();

//     const stubUserService: StubUserService = TestBed.get(UserService);
//     stubUserService.userChangedEvent.next(MOCK_USER);

//     expect(spy).toHaveBeenCalled();

//     const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${USER_FILE_LIST_URL}`);
//     expect(req.request.method).toEqual('GET');
//     req.flush([testFile]);
//   });

// });
