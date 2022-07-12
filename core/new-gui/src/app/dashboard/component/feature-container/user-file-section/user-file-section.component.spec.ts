import { ComponentFixture, TestBed, inject, waitForAsync } from "@angular/core/testing";

import { NgbModal, NgbModalRef, NgbModule } from "@ng-bootstrap/ng-bootstrap";
import { CustomNgMaterialModule } from "../../../../common/custom-ng-material.module";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { MatListModule } from "@angular/material/list";
import { UserFileSectionComponent } from "./user-file-section.component";
import { UserFileService } from "../../../service/user-file/user-file.service";
import { UserService } from "../../../../common/service/user/user.service";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { DashboardUserFileEntry, UserFile } from "../../../type/dashboard-user-file-entry";
import { NgbdModalWorkflowShareAccessComponent } from "../saved-workflow-section/ngbd-modal-share-access/ngbd-modal-workflow-share-access.component";
import { RouterTestingModule } from "@angular/router/testing";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";

describe("UserFileSectionComponent", () => {
  let component: UserFileSectionComponent;
  let fixture: ComponentFixture<UserFileSectionComponent>;
  let modalService: NgbModal;

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
  const testFile: DashboardUserFileEntry = {
    ownerName: "Texera",
    file: fileContent,
    accessLevel: "Write",
    isOwner: true,
    projectIDs: [],
  };

  // for sorting tests
  const testFile1: DashboardUserFileEntry = {
    ownerName: "Texera",
    file: {
      fid: 2,
      name: "File 1",
      path: "test/path",
      description: "This is the test file 1",
      size: 128,
    },
    accessLevel: "Write",
    isOwner: true,
    projectIDs: [],
  };

  const testFile2: DashboardUserFileEntry = {
    ownerName: "Texera",
    file: {
      fid: 3,
      name: "File 2",
      path: "test/path",
      description: "This is the test file 2",
      size: 0,
    },
    accessLevel: "Write",
    isOwner: true,
    projectIDs: [],
  };

  const testFile3: DashboardUserFileEntry = {
    ownerName: "Texera",
    file: {
      fid: 4,
      name: "A File 3",
      path: "test/path",
      description: "This is the test file 3",
      size: 64,
    },
    accessLevel: "Write",
    isOwner: true,
    projectIDs: [],
  };

  const testFile4: DashboardUserFileEntry = {
    ownerName: "Alice",
    file: {
      fid: 5,
      name: "File 2",
      path: "test/path",
      description: "Alice's file 2",
      size: 512,
    },
    accessLevel: "Write",
    isOwner: true,
    projectIDs: [],
  };

  const testFile5: DashboardUserFileEntry = {
    ownerName: "Alex",
    file: {
      fid: 6,
      name: "File 3",
      path: "test/path",
      description: "Alex's file 3",
      size: 8,
    },
    accessLevel: "Write",
    isOwner: true,
    projectIDs: [],
  };

  const testFileEntries: DashboardUserFileEntry[] = [testFile, testFile1, testFile2, testFile3, testFile4, testFile5];

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [UserFileSectionComponent],
        providers: [NgbModal, { provide: UserService, useClass: StubUserService }, UserFileService],
        imports: [
          CustomNgMaterialModule,
          NgbModule,
          FormsModule,
          ReactiveFormsModule,
          MatListModule,
          HttpClientTestingModule,
          RouterTestingModule,
          NzDropDownModule,
        ],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(UserFileSectionComponent);
    component = fixture.componentInstance;
    modalService = TestBed.get(NgbModal);
    fixture.detectChanges();
  });

  it("Modal Opened, then Closed", () => {
    const modalRef: NgbModalRef = modalService.open(NgbdModalWorkflowShareAccessComponent);
    spyOn(modalService, "open").and.returnValue(modalRef);
    component.onClickOpenShareAccess(testFile);
    expect(modalService.open).toHaveBeenCalled();
    fixture.detectChanges();
    modalRef.dismiss();
  });

  it("should create", inject([HttpTestingController], (httpMock: HttpTestingController) => {
    expect(component).toBeTruthy();
  }));

  it("alphaSortTest increasingOrder", () => {
    component.dashboardUserFileEntries = [];
    component.dashboardUserFileEntries = component.dashboardUserFileEntries.concat(testFileEntries);
    component.ascSort();
    const SortedCase = component.dashboardUserFileEntries.map(item => item.file.fid);
    // Order: Alex/File 3, Alice/File 2, Texera/A File 3, Texera/File 1, Texera/File 2, Texera/testFile
    expect(SortedCase).toEqual([6, 5, 4, 2, 3, 1]);
  });

  it("alphaSortTest decreasingOrder", () => {
    component.dashboardUserFileEntries = [];
    component.dashboardUserFileEntries = component.dashboardUserFileEntries.concat(testFileEntries);
    component.dscSort();
    const SortedCase = component.dashboardUserFileEntries.map(item => item.file.fid);
    expect(SortedCase).toEqual([1, 3, 2, 4, 5, 6]);
  });

  it("fileSizeSortTest decreasingOrder", () => {
    component.dashboardUserFileEntries = [];
    component.dashboardUserFileEntries = component.dashboardUserFileEntries.concat(testFileEntries);
    component.sizeSort();
    const SortedCase = component.dashboardUserFileEntries.map(item => item.file.fid);
    // Order: Texera/testFile, Alice/File 2, Texera/File 1, Texera/A File 3,  Alex/File 3, Texera/File 2
    expect(SortedCase).toEqual([1, 5, 2, 4, 6, 3]);
  });
});
