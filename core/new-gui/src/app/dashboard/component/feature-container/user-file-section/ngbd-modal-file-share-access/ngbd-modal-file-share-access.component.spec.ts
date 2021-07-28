import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { HttpClient, HttpHandler } from '@angular/common/http';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { NgbdModalFileShareAccessComponent } from "./ngbd-modal-file-share-access.component";
import { UserFileService } from "../../../../../common/service/user/user-file/user-file.service";
import { UserFile } from "../../../../../common/type/user-file";
import { StubUserFileService } from "../../../../../common/service/user/user-file/stub-user-file-service";
import { GoogleApiService, GoogleAuthService } from "ng-gapi";

describe('NgbdModalFileShareAccessComponent', () => {
  let component: NgbdModalFileShareAccessComponent;
  let fixture: ComponentFixture<NgbdModalFileShareAccessComponent>;
  let service: UserFileService;

  const file: UserFile = {
    fileName: "test",
    ownerName: "test",
    size: 1,
    description: "test",
    access: "Test"
  }

  beforeEach(async(async () => {
    TestBed.configureTestingModule({
      imports: [ReactiveFormsModule, FormsModule],
      declarations: [NgbdModalFileShareAccessComponent],
      providers: [NgbActiveModal, HttpClient, HttpHandler, GoogleAuthService, GoogleApiService, {
        provide: UserFileService,
        useClass: StubUserFileService
      }]
    });
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalFileShareAccessComponent);
    component = fixture.componentInstance;
    service = TestBed.get(UserFileService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('form invalid when empty', () => {
    expect(component.shareForm.valid).toBeFalsy();
  });

  it('can get all accesses', () => {
    const mySpy = spyOn(service, 'getSharedAccessesOfFile').and.callThrough();
    component.file = file;
    fixture.detectChanges();
    component.onClickGetAllSharedAccess(component.file);
    expect(mySpy).toHaveBeenCalled();
  });

  it('can share accesses', () => {
    const mySpy = spyOn(service, 'grantAccess').and.callThrough();
    component.file = file;
    fixture.detectChanges();
    component.grantAccess(component.file, 'Jim', 'read');
    expect(mySpy).toHaveBeenCalled();
  });

  it('can remove accesses', () => {
    const mySpy = spyOn(service, 'revokeFileAccess').and.callThrough();
    component.onClickRemoveAccess(file, 'Jim');
    expect(mySpy).toHaveBeenCalled();
  });

  it('submitting a form', () => {
    const mySpy = spyOn(component, 'onClickShareFile');
    expect(component.shareForm.valid).toBeFalsy();
    component.shareForm.controls['username'].setValue('testguy');
    component.shareForm.controls['accessLevel'].setValue('read');
    expect(component.shareForm.valid).toBeTruthy();
    component.onClickShareFile(file);
    expect(mySpy).toHaveBeenCalled();
  });

});
