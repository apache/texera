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
import { NzMessageModule } from "ng-zorro-antd/message";

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
  };
  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [UserFileSectionComponent],
        providers: [NgbModal, { provide: UserService, useClass: StubUserService }, UserFileService],
        imports: [
          NzMessageModule,
          CustomNgMaterialModule,
          NgbModule,
          FormsModule,
          ReactiveFormsModule,
          MatListModule,
          HttpClientTestingModule,
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
});
