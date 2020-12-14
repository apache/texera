// import { TestBed } from '@angular/core/testing';

// import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

// import { UserService } from '../user.service';
// import { UserFileUploadService, USER_FILE_UPLOAD_URL, USER_FILE_VALIDATE_URL } from './user-file-upload.service';
// import { UserFileService } from './user-file.service';
// import { StubUserService } from '../stub-user.service';
// import { AppSettings } from '../../../app-setting';

// const arrayOfBlob: Blob[] = Array<Blob>(); // just for test,needed for creating File object.
// const testFileName = 'testTextFile';
// const testFile: File = new File(arrayOfBlob, testFileName, { type: 'text/plain' });

// describe('UserFileUploadService', () => {

//   let service: UserFileUploadService;
//   let httpMock: HttpTestingController;

//   beforeEach(() => {
//     TestBed.configureTestingModule({
//       providers: [
//         { provide: UserService, useClass: StubUserService },
//         UserFileService,
//         UserFileUploadService
//       ],
//       imports: [
//         HttpClientTestingModule
//       ]
//     });
//     service = TestBed.get(UserFileUploadService);
//     httpMock = TestBed.get(HttpTestingController);
//   });

//   afterEach(() => {
//     httpMock.verify();
//   });

//   it('should be created', () => {
//     expect(service).toBeTruthy();
//   });

//   it('should contain no file by default', () => {
//     expect(service.getFilesToBeUploaded().length).toBe(0);
//   });

//   it('should insert file successfully', () => {
//     service.addFileToUploadArray(testFile);
//     expect(service.getFilesToBeUploaded().length).toBe(1);
//     expect(service.getFilesToBeUploaded()[0].file).toEqual(testFile);
//     expect(service.getFilesToBeUploaded()[0].name).toEqual(testFileName);
//     expect(service.getFilesToBeUploaded()[0].isUploadingFlag).toBeFalsy();
//   });

//   it('should delete file successfully', () => {
//     service.addFileToUploadArray(testFile);
//     expect(service.getFilesToBeUploaded().length).toBe(1);
//     const testFileUploadItem = service.getFilesToBeUploaded()[0];
//     service.removeFileFromUploadArray(testFileUploadItem);
//     expect(service.getFilesToBeUploaded().length).toBe(0);
//   });

//   it('should upload file successfully', () => {
//     service.addFileToUploadArray(testFile);
//     expect(service.getFilesToBeUploaded().length).toBe(1);

//     const userFileService = TestBed.get(UserFileService);
//     const spy = spyOn(userFileService, 'refreshFiles');

//     service.uploadAllFiles();

//     const req1 = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${USER_FILE_VALIDATE_URL}`);
//     expect(req1.request.method).toEqual('POST');
//     req1.flush({ code: 0, message: '' });

//     const req2 = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/${USER_FILE_UPLOAD_URL}`);
//     expect(req2.request.method).toEqual('POST');
//     req2.flush({ code: 0, message: '' });

//     expect(spy).toHaveBeenCalled();
//   });

// });
