import { TestBed } from '@angular/core/testing';

import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { UserFile, DashboardUserFileEntry } from '../../../type/dashboard-user-file-entry';

import {
  USER_FILE_ACCESS_LIST_URL,
  USER_FILE_ACCESS_GRANT_URL,
  USER_FILE_ACCESS_REVOKE_URL,
  UserFileService
} from './user-file.service';
import { UserService } from '../user.service';
import { StubUserService } from '../stub-user.service';
import { AppSettings } from 'src/app/common/app-setting';


const id = 1;
const name = 'testFile';
const path = 'test/path';
const description = 'this is a test file';
const size = 1024;
const username = 'Jim';
const accessLevel = 'read';
const fileContent: UserFile = {
  id: id,
  name: name,
  path: path,
  size: size,
  description: description
};
const testFile: DashboardUserFileEntry = {
  ownerName: 'Texera',
  file: fileContent,
  accessLevel: 'Write',
  isOwner: true,
};

describe('UserFileService', () => {
  let httpMock: HttpTestingController;
  let service: UserFileService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        UserFileService,
        {provide: UserService, useClass: StubUserService}
      ],
      imports: [
        HttpClientTestingModule
      ]
    });
    httpMock = TestBed.get(HttpTestingController);
    service = TestBed.get(UserFileService);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });


  it('can share access', () => {
    service.grantUserFileAccess(testFile, username, accessLevel).first().subscribe();
    const req = httpMock.expectOne(
      `${AppSettings.getApiEndpoint()}/${USER_FILE_ACCESS_GRANT_URL}/${testFile.file.name}/
      ${testFile.ownerName}/${username}/${accessLevel}`);
    expect(req.request.method).toEqual('POST');
    req.flush({code: 0, message: ''});
  });

  it('can revoke access', () => {
    service.revokeUserFileAccess(testFile, username).first().subscribe();
    const req = httpMock.expectOne(
      `${AppSettings.getApiEndpoint()}/${USER_FILE_ACCESS_REVOKE_URL}/${testFile.file.name}/${testFile.ownerName}/${username}`);
    expect(req.request.method).toEqual('POST');
    req.flush({code: 0, message: ''});
  });

  it('can get all access', () => {
    service.getUserFileAccessList(testFile).first().subscribe();
    const req = httpMock.expectOne(
      `${AppSettings.getApiEndpoint()}/${USER_FILE_ACCESS_LIST_URL}/${testFile.file.name}/${testFile.ownerName}`);
    expect(req.request.method).toEqual('GET');
    req.flush({code: 0, message: ''});
  });

});
