import { TestBed, inject } from '@angular/core/testing';

import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

import { UserService } from '../user.service';
import { StubUserService, STUB_USER_NAME } from '../stub-user.service';
import { UserFileUploadService, USER_FILE_UPLOAD_URL } from './user-file-upload.service';
import { UserFileService } from './user-file.service';

const arrayOfBlob: Blob[] = Array<Blob>(); // just for test,needed for creating File object.
const testFileName = 'testTextFile';
const testFile: File = new File( arrayOfBlob, testFileName, {type: 'text/plain'});

describe('UserFileUploadService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        { provide: UserService, useClass: StubUserService },
        UserFileService,
        UserFileUploadService
      ],
      imports: [
        HttpClientTestingModule
      ]
    });
  });

  // afterEach(inject([HttpTestingController], (httpMock: HttpTestingController) => {
  //   httpMock.verify();
  // }));

  it('should be created', inject([UserFileUploadService, UserFileService, HttpTestingController], (service: UserFileUploadService) => {
    expect(service).toBeTruthy();
  }));

  it('should contain no file by default', inject([UserFileUploadService, UserService, UserFileService, HttpTestingController],
    (service: UserFileUploadService, userService: UserService) => {
    expect(service.getFileArray().length).toBe(0);
    expect(() => service.getFileUploadItem(0)).toThrowError();
  }));

  it('should insert file successfully', inject([UserFileUploadService, UserService, UserFileService, HttpTestingController],
    (service: UserFileUploadService, userService: UserService) => {
    service.insertNewFile(testFile);
    expect(service.getFileArray().length).toBe(1);
    expect(service.getFileArray()[0]).toEqual(service.getFileUploadItem(0));
    expect(service.getFileUploadItem(0).file).toEqual(testFile);
    expect(service.getFileUploadItem(0).name).toEqual(testFileName);
    expect(service.getFileUploadItem(0).isUploadingFlag).toBeFalsy();
    expect(() => service.getFileUploadItem(1)).toThrowError();
  }));

  it('should delete file successfully', inject([UserFileUploadService, UserService, UserFileService, HttpTestingController],
    (service: UserFileUploadService, userService: UserService) => {
    service.insertNewFile(testFile);
    expect(service.getFileArray().length).toBe(1);
    const testFileUploadItem = service.getFileUploadItem(0);
    service.deleteFile(testFileUploadItem);
    expect(service.getFileArray().length).toBe(0);
  }));

  // TODO writes tests for this service

  // it('should upload file successfully', inject([UserFileUploadService, userService, UserFileService, HttpTestingController],
  //   (service: UserFileUploadService, userService: userService, userFileService: UserFileService, httpMock: HttpTestingController) => {
  //   userService.login(STUB_USER_NAME);
  //   service.insertNewFile(testFile);
  //   expect(service.getFileArrayLength()).toBe(1);
  //   service.uploadAllFiles();

  //   spyOn(userFileService, 'refreshFiles');
  //   expect(userFileService.refreshFiles).toHaveBeenCalled();

  //   const req = httpMock.expectOne(`${ppSettings.getApiEndpoint()}/${postFileUrl}/${stubUserID}`);
  //   expect(req.request.method).toEqual('POST');
  //   req.flush({code: 0, message: ''});
  // }));
});
