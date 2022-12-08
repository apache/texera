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
import { By } from "@angular/platform-browser";

describe("UserFileSectionComponent", () => {
  let component: UserFileSectionComponent;
  let fixture: ComponentFixture<UserFileSectionComponent>;
  let modalService: NgbModal;

  const id = 1;
  const name = "testFile";
  const path = "test/path";
  const description = "this is a test file";
  const size = 1024;
  const uploadTime = "0";

  const fileContent: UserFile = {
    fid: id,
    name: name,
    path: path,
    size: size,
    description: description,
    uploadTime: uploadTime,
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
      uploadTime: "1000",
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
      uploadTime: "9999",
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
      uploadTime: "5000",
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
      uploadTime: "500",
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
      uploadTime: "1658145215",
    },
    accessLevel: "Write",
    isOwner: true,
    projectIDs: [],
  };

  const testFileEntries: DashboardUserFileEntry[] = [testFile, testFile1, testFile2, testFile3, testFile4, testFile5];

  beforeEach(waitForAsync(() => {
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
  }));

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

  it("uploadTimeSortTest decreasingOrder", () => {
    component.dashboardUserFileEntries = [];
    component.dashboardUserFileEntries = component.dashboardUserFileEntries.concat(testFileEntries);
    component.timeSortDesc();
    const SortedCase = component.dashboardUserFileEntries.map(item => item.file.fid);
    // Order: Alex/File 3, Texera/File 2, Texera/A File 3, Texera/File 1, Alice/File 2, Texera/testFile
    expect(SortedCase).toEqual([6, 3, 4, 2, 5, 1]);
  });

  it("uploadTimeSortTest increasingOrder", () => {
    component.dashboardUserFileEntries = [];
    component.dashboardUserFileEntries = component.dashboardUserFileEntries.concat(testFileEntries);
    component.timeSortAsc();
    const SortedCase = component.dashboardUserFileEntries.map(item => item.file.fid);
    // Order: Alex/File 3, Texera/File 2, Texera/A File 3, Texera/File 1, Alice/File 2, Texera/testFile
    expect(SortedCase).toEqual([1, 5, 2, 4, 3, 6]);
  });

  it("adding a file description adds a description to the file", waitForAsync(() => {
    fixture.whenStable().then(() => {
      let addFileDescriptionBtn1 = fixture.debugElement.query(By.css(".add-description-btn"));
      expect(addFileDescriptionBtn1).toBeFalsy();
      // add some test workflows
      component.dashboardUserFileEntries = testFileEntries;
      fixture.detectChanges();
      let addFileDescriptionBtn2 = fixture.debugElement.query(By.css(".add-description-btn"));
      // the button for adding workflow descriptions should appear now
      expect(addFileDescriptionBtn2).toBeTruthy();
      addFileDescriptionBtn2.triggerEventHandler("click", null);
      fixture.detectChanges();
      let editableDescriptionInput1 = fixture.debugElement.nativeElement.querySelector(
        ".file-editable-description-input"
      );
      expect(editableDescriptionInput1).toBeTruthy();

      spyOn(component, "confirmUpdateFileCustomDescription");
      sendInput(editableDescriptionInput1, "dummy description added by focusing out the input element.").then(() => {
        fixture.detectChanges();
        editableDescriptionInput1.dispatchEvent(new Event("focusout"));
        fixture.detectChanges();
        expect(component.confirmUpdateFileCustomDescription).toHaveBeenCalledTimes(1);
      });
    });
  }));

  it("Editing a file description edits a description to the file", waitForAsync(() => {
    fixture.whenStable().then(() => {
      let fileDescriptionLabel1 = fixture.debugElement.query(By.css(".file-description-label"));
      expect(fileDescriptionLabel1).toBeFalsy();
      // add some test workflows
      component.dashboardUserFileEntries = testFileEntries;
      fixture.detectChanges();
      let fileDescriptionLabel2 = fixture.debugElement.query(By.css(".file-description-label"));
      // the workflow description label should appear now
      expect(fileDescriptionLabel2).toBeTruthy();
      fileDescriptionLabel2.triggerEventHandler("click", null);
      fixture.detectChanges();
      let editableDescriptionInput1 = fixture.debugElement.nativeElement.querySelector(
        ".file-editable-description-input"
      );
      expect(editableDescriptionInput1).toBeTruthy();

      spyOn(component, "confirmUpdateFileCustomDescription");

      sendInput(editableDescriptionInput1, "dummy description added by focusing out the input element.").then(() => {
        fixture.detectChanges();
        editableDescriptionInput1.dispatchEvent(new Event("focusout"));
        fixture.detectChanges();
        expect(component.confirmUpdateFileCustomDescription).toHaveBeenCalledTimes(1);
      });
    });
  }));

  function sendInput(editableDescriptionInput: HTMLInputElement, text: string) {
    // editableDescriptionInput.dispatchEvent(new Event("focus"));
    // fixture.detectChanges();
    editableDescriptionInput.value = text;
    editableDescriptionInput.dispatchEvent(new Event("input"));
    fixture.detectChanges();
    return fixture.whenStable();
  }
});
