import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { NgbActiveModal, NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

import { HttpClient } from '@angular/common/http';
import { NgbdModalResourceAddComponent } from './ngbd-modal-resource-add.component';
import { CustomNgMaterialModule } from '../../../../../common/custom-ng-material.module';

import { FileUploadModule } from 'ng2-file-upload';
import { UserService } from '../../../../../common/service/user/user.service';
import { UserDictionaryUploadService } from '../../../../../common/service/user/user-dictionary/user-dictionary-upload.service';
import { UserDictionaryService } from '../../../../../common/service/user/user-dictionary/user-dictionary.service';
import { StubUserService } from '../../../../../common/service/user/stub-user.service';

describe('NgbdModalResourceAddComponent', () => {
  let component: NgbdModalResourceAddComponent;
  let fixture: ComponentFixture<NgbdModalResourceAddComponent>;

  let httpClient: HttpClient;
  let httpTestingController: HttpTestingController;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [NgbdModalResourceAddComponent],
      providers: [
        NgbActiveModal,
        {provide: UserService, useClass: StubUserService},
        UserDictionaryService,
        UserDictionaryUploadService
      ],
      imports: [
        CustomNgMaterialModule,
        NgbModule,
        FormsModule,
        FileUploadModule,
        ReactiveFormsModule,
        HttpClientTestingModule
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    component = fixture.componentInstance;

    httpClient = TestBed.get(HttpClient);
    httpTestingController = TestBed.get(HttpTestingController);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

});
