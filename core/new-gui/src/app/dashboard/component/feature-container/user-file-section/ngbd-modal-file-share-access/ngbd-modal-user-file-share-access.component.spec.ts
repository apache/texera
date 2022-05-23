import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { HttpClient, HttpHandler } from "@angular/common/http";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { NgbdModalUserFileShareAccessComponent } from "./ngbd-modal-user-file-share-access.component";
import { UserFileService } from "../../../../service/user-file/user-file.service";
import { DashboardUserFileEntry, UserFile } from "../../../../type/dashboard-user-file-entry";
import { StubUserFileService } from "../../../../service/user-file/stub-user-file-service";
import { StubUserService } from "src/app/common/service/user/stub-user.service";
import { GoogleApiService, GoogleAuthService } from "ng-gapi";

describe("NgbdModalFileShareAccessComponent", () => {
  let component: NgbdModalUserFileShareAccessComponent;
  let fixture: ComponentFixture<NgbdModalUserFileShareAccessComponent>;
  let service: UserFileService;

  const id = 1;
  const name = "testFile";
  const path = "test/path";
  const description = "this is a test file";
  const size = 1024;
  const fileContent: UserFile = {
    fid: id,
    name: name,
    path: path,
    size: size,
    description: description,
  };
  const file: DashboardUserFileEntry = {
    ownerName: "Texera",
    file: fileContent,
    accessLevel: "Write",
    isOwner: true,
    projectIDs: [],
  };

  beforeEach(
    waitForAsync(async () => {
      TestBed.configureTestingModule({
        imports: [ReactiveFormsModule, FormsModule],
        declarations: [NgbdModalUserFileShareAccessComponent],
        providers: [
          NgbActiveModal,
          HttpClient,
          HttpHandler,
          GoogleAuthService,
          GoogleApiService,
          StubUserService,
          {
            provide: UserFileService,
            useClass: StubUserFileService,
          },
        ],
      });
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalUserFileShareAccessComponent);
    component = fixture.componentInstance;
    service = TestBed.get(UserFileService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("form invalid when empty", () => {
    expect(component.shareForm.valid).toBeFalsy();
  });

  it("can get all accesses", () => {
    const mySpy = spyOn(service, "getUserFileAccessList").and.callThrough();
    component.dashboardUserFileEntry = file;
    fixture.detectChanges();
    component.refreshGrantedUserFileAccessList(component.dashboardUserFileEntry);
    expect(mySpy).toHaveBeenCalled();
  });

  it("can remove accesses", () => {
    const mySpy = spyOn(service, "revokeUserFileAccess").and.callThrough();
    component.onClickRemoveUserFileAccess(file, "Jim");
    expect(mySpy).toHaveBeenCalled();
  });

  it("submitting a form", () => {
    const mySpy = spyOn(service, "grantUserFileAccess").and.callThrough();
    expect(component.shareForm.valid).toBeFalsy();
    component.shareForm.controls["username"].setValue("testguy");
    component.shareForm.controls["accessLevel"].setValue("read");
    expect(component.shareForm.valid).toBeTruthy();
    component.onClickShareUserFile(file);
    expect(mySpy).toHaveBeenCalled();
  });
});
