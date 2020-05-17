import { TestBed, inject } from '@angular/core/testing';

import { UserFileService, getFilesUrl } from './user-file.service';
import { UserAccountService } from '../user-account/user-account.service';
import { StubUserAccountService } from '../user-account/stub-user-account.service';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { UserFile } from '../../type/user-file';
import { environment } from '../../../../environments/environment';

const id = 1;
const name = 'testFile';
const path = 'test/path';
const description = 'this is a test file';
const size = 1024;
const testFile: UserFile = {
  id: id,
  name: name,
  path: path,
  size: size,
  description: description
};

describe('UserFileService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UserFileService,
      { provide: UserAccountService, useClass: StubUserAccountService }
      ],
      imports: [
        HttpClientTestingModule
      ]
    });
  });

  afterEach(inject([HttpTestingController], (httpMock: HttpTestingController) => {
    httpMock.verify();
  }));

  it('should be created', inject([UserFileService, UserAccountService, HttpTestingController],
    (service: UserFileService) => {
    expect(service).toBeTruthy();
  }));

  it('should contain no files by default', inject([UserFileService, UserAccountService, HttpTestingController],
    (service: UserFileService) => {
    expect(service.getFileArrayLength()).toBe(0);
    expect(() => service.getFileField(0, 'id')).toThrowError();
  }));

  // it('should refresh file after user login', inject([UserFileService, UserAccountService, HttpTestingController],
  //   (service: UserFileService, userAccountService: UserAccountService, httpMock: HttpTestingController) => {
  //   expect(service.getFileArrayLength()).toBe(0);
  //   spyOn(service, 'refreshFiles').and.callThrough();
  //   expect(service.refreshFiles).toHaveBeenCalled();

  //   userAccountService.loginUser('').subscribe();

  //       // expect(service.getFileArrayLength()).toEqual(1);
  //       // expect(service.getFileArray()[0]).toEqual(testFile);
  //       // expect(service.getFileField(0, 'id')).toEqual(id);
  //       // expect(service.getFileField(0, 'name')).toEqual(name);
  //       // expect(service.getFileField(0, 'path')).toEqual(path);
  //       // expect(service.getFileField(0, 'description')).toEqual(description);
  //       // expect(service.getFileField(0, 'size')).toEqual(size);

  //   const req = httpMock.expectOne(`${environment.apiUrl}/${getFilesUrl}/${id}`);
  //   expect(req.request.method).toEqual('GET');
  //   req.flush([testFile]);
  // }));

  // it('should refresh file after user login', inject([UserFileService, UserAccountService, HttpTestingController],
  //   (service: UserFileService, userAccountService: UserAccountService, httpMock: HttpTestingController) => {
  //   expect(service.getFileArrayLength()).toBe(0);

  //   spyOn(service, 'refreshFiles').and.callThrough();
  //   expect(service.refreshFiles).toHaveBeenCalled();
  //   // .then(
  //     // () => {
  //     //   expect(service.getFileArrayLength()).toEqual(1);
  //     //   expect(service.getFileArray()[0]).toEqual(testFile);
  //     //   expect(service.getFileField(0, 'id')).toEqual(id);
  //     //   expect(service.getFileField(0, 'name')).toEqual(name);
  //     //   expect(service.getFileField(0, 'path')).toEqual(path);
  //     //   expect(service.getFileField(0, 'description')).toEqual(description);
  //     //   expect(service.getFileField(0, 'size')).toEqual(size);
  //     // }
  //   // );

  //   userAccountService.loginUser('').subscribe();

  //   const req = httpMock.expectOne(`${environment.apiUrl}/${getFilesUrl}/${id}`);
  //   expect(req.request.method).toEqual('GET');
  //   req.flush([testFile]);
  // }));
});
