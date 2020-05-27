import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { NgbModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { HttpClient } from '@angular/common/http';
import { NgbdModalResourceAddComponent } from './ngbd-modal-resource-add.component';
import { CustomNgMaterialModule } from '../../../../../common/custom-ng-material.module';

import { FileUploadModule } from 'ng2-file-upload';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { environment } from '../../../../../../environments/environment';
import { UserAccountService } from '../../../../service/user-account/user-account.service';
import { UserDictionaryUploadService } from '../../../../service/user-dictionary/user-dictionary-upload.service';
import { UserDictionaryService } from '../../../../service/user-dictionary/user-dictionary.service';


const dictionaryUrl = 'users/dictionaries';
const uploadFilesURL = 'users/dictionaries/upload-files';

describe('NgbdModalResourceAddComponent', () => {
  let component: NgbdModalResourceAddComponent;
  let fixture: ComponentFixture<NgbdModalResourceAddComponent>;

  let httpClient: HttpClient;
  let httpTestingController: HttpTestingController;

  const arrayOfBlob: Blob[] = Array<Blob>(); // just for test,needed for creating File object.
  const testTextFile: File = new File( arrayOfBlob, 'testTextFile', {type: 'text/plain'});
  const testPicFile: File = new File( arrayOfBlob, 'testPicFile', {type: 'image/jpeg'});
  const testDocFile: File = new File( arrayOfBlob, 'testDocFile', {type: 'application/msword'});

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgbdModalResourceAddComponent ],
      providers: [
        NgbActiveModal,
        UserAccountService,
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
