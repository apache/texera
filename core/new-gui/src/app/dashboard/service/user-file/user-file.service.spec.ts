import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { DashboardUserFileEntry, UserFile } from "../../type/dashboard-user-file-entry";
import {
  USER_FILE_ACCESS_GRANT_URL,
  USER_FILE_ACCESS_LIST_URL,
  USER_FILE_ACCESS_REVOKE_URL,
  UserFileService,
} from "./user-file.service";
import { UserService } from "../../../common/service/user/user.service";
import { StubUserService } from "../../../common/service/user/stub-user.service";
import { first } from "rxjs/operators";

const id = 1;
const name = "testFileEntry";
const path = "test/path";
const description = "this is a test file";
const size = 1024;
const username = "Jim";
const accessLevel = "read";
const testFile: UserFile = {
  fid: id,
  name: name,
  path: path,
  size: size,
  description: description,
};
const testFileEntry: DashboardUserFileEntry = {
  ownerName: "Texera",
  file: testFile,
  accessLevel: "Write",
  isOwner: true,
  projectIDs: [],
};

describe("UserFileService", () => {
  let httpMock: HttpTestingController;
  let service: UserFileService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UserFileService, { provide: UserService, useClass: StubUserService }],
      imports: [HttpClientTestingModule],
    });
    httpMock = TestBed.get(HttpTestingController);
    service = TestBed.get(UserFileService);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("can share access", () => {
    service.grantUserFileAccess(testFileEntry, username, accessLevel).pipe(first()).subscribe();
    const req = httpMock.expectOne(`${USER_FILE_ACCESS_GRANT_URL}`);
    expect(req.request.body).toEqual({
      fileName: testFileEntry.file.name,
      ownerName: testFileEntry.ownerName,
      username,
      accessLevel,
    });
    expect(req.request.method).toEqual("POST");
    req.flush({ code: 0, message: "" });
  });

  it("can revoke access", () => {
    service.revokeUserFileAccess(testFileEntry, username).pipe(first()).subscribe();
    const req = httpMock.expectOne(
      `${USER_FILE_ACCESS_REVOKE_URL}/${testFileEntry.file.name}/${testFileEntry.ownerName}/${username}`
    );
    expect(req.request.method).toEqual("DELETE");
    req.flush({ code: 0, message: "" });
  });

  it("can get all access", () => {
    service.getUserFileAccessList(testFileEntry).pipe(first()).subscribe();
    const req = httpMock.expectOne(
      `${USER_FILE_ACCESS_LIST_URL}/${testFileEntry.file.name}/${testFileEntry.ownerName}`
    );
    expect(req.request.method).toEqual("GET");
    req.flush({ code: 0, message: "" });
  });
});
