import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { HttpClient, HttpHandler } from '@angular/common/http';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { NgbdModalFileShareAccessComponent } from './ngbd-modal-file-share-access.component';
import { UserFileService } from '../../../../../common/service/user/user-file/user-file.service';
import { DashboardUserFileEntry, UserFile } from '../../../../../common/type/dashboard-user-file-entry';
import { StubUserFileService } from '../../../../../common/service/user/user-file/stub-user-file-service';
import { GoogleApiService, GoogleAuthService } from 'ng-gapi';

describe('NgbdModalFileShareAccessComponent', () => {
  let component: NgbdModalFileShareAccessComponent;
  let fixture: ComponentFixture<NgbdModalFileShareAccessComponent>;
  let service: UserFileService;

  const id = 1;
  const name = 'testFile';
  const path = 'test/path';
  const description = 'this is a test file';
  const size = 1024;
  const fileContent: UserFile = {
    id: id,
    name: name,
    path: path,
    size: size,
    description: description
  };
  const file: DashboardUserFileEntry = {
    ownerName: 'Texera',
    file: fileContent,
    accessLevel: 'Write',
    isOwner: true,
  };

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
    const mySpy = spyOn(service, 'getUserFileAccessList').and.callThrough();
    component.dashboardUserFileEntry = file;
    fixture.detectChanges();
    component.refreshGrantedUserFileAccessList(component.dashboardUserFileEntry);
    expect(mySpy).toHaveBeenCalled();
  });

  it('can remove accesses', () => {
    const mySpy = spyOn(service, 'revokeUserFileAccess').and.callThrough();
    component.onClickRemoveUserFileAccess(file, 'Jim');
    expect(mySpy).toHaveBeenCalled();
  });

  it('submitting a form', () => {
    const mySpy = spyOn(service, 'grantUserFileAccess');
    expect(component.shareForm.valid).toBeFalsy();
    component.shareForm.controls['username'].setValue('testguy');
    component.shareForm.controls['accessLevel'].setValue('read');
    expect(component.shareForm.valid).toBeTruthy();
    component.onClickShareUserFile(file);
    expect(mySpy).toHaveBeenCalled();
  });

});
