import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { NgbModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { HttpClient } from '@angular/common/http';
import { NgbdModalResourceAddComponent } from './ngbd-modal-resource-add.component';
import { CustomNgMaterialModule } from '../../../../../common/custom-ng-material.module';

import { FileUploadModule } from 'ng2-file-upload';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { environment } from '../../../../../../environments/environment';


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
        NgbActiveModal
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


  it('should be able to upload a text file, which is not be considered as duplicate or invalid', () => {
    const testFileList: File[] = Array<File>();
    testFileList.push(testTextFile);

    component.uploader.addToQueue(testFileList);
    expect(component.duplicateFiles.length).toEqual(0);
    expect(component.invalidFileTypeCounter).toEqual(0);
    expect(component.uploader.queue.length).toEqual(1);

  });

  it('should be able to detect invalid file types when user uploads invalid type files', () => {

    const testFileList: File[] = Array<File>();
    testFileList.push(testTextFile);
    testFileList.push(testPicFile);
    testFileList.push(testDocFile);

    component.uploader.addToQueue(testFileList);

    const fileList: FileList = {
      length: 3,
      item: () => null,
      0: testTextFile,
      1: testPicFile,
      2: testDocFile
    };

    component.getFileDropped(fileList);
    expect(component.duplicateFiles.length).toEqual(0);
    expect(component.invalidFileTypeCounter).toEqual(2);
    expect(component.uploader.queue.length).toEqual(3);

  });

  it('should be able to count the number of duplicate files', () => {

    const testFileList: File[] = Array<File>();
    testFileList.push(testTextFile);
    testFileList.push(testTextFile);

    component.uploader.addToQueue(testFileList);

    const fileList: FileList = {
      length: 2,
      item: () => null,
      0: testTextFile,
      1: testTextFile,
    };

    component.getFileDropped(fileList);

    expect(component.duplicateFiles.length).toEqual(1);
    expect(component.invalidFileTypeCounter).toEqual(0);
    expect(component.uploader.queue.length).toEqual(2);

  });

  it('should be able to delete the invalid files in the queue when deleteAllInvalidFile() is called', () => {

    const testFileList: File[] = Array<File>();
    testFileList.push(testTextFile);
    testFileList.push(testPicFile);
    testFileList.push(testDocFile);
    testFileList.push(testTextFile);

    component.uploader.addToQueue(testFileList);

    const fileList: FileList = {
      length: 4,
      item: () => null,
      0: testTextFile,
      1: testPicFile,
      2: testDocFile,
      3: testTextFile
    };
    component.getFileDropped(fileList);

    component.deleteAllInvalidFile();

    expect(component.duplicateFiles.length).toEqual(0);
    expect(component.invalidFileTypeCounter).toEqual(0);
    expect(component.uploader.queue.length).toEqual(1);
  });

  it(`should check if the dictionary form is valid currently`, () => {
    expect(component.checkDictionaryFormValid()).toBeFalsy();

    component.dictContent = 'mockContent';
    component.dictName = 'mockDictionaryName';
    component.dictionaryDescription = 'mockDictionaryDescription';

    expect(component.checkDictionaryFormValid()).toBeTruthy();
  });

  it(`should do nothing when addDictionary() or uploadFiles() is called when the content is uploading`, () => {
    component.isUploading = true;
    component.addDictionary();
    const req = httpTestingController.expectNone(`${environment.apiUrl}/${dictionaryUrl}`);
    httpTestingController.verify();

    component.uploadFiles();
    const req2 = httpTestingController.expectNone(`${environment.apiUrl}/${uploadFilesURL}`);
    httpTestingController.verify();
  });

  it(`should send an http request to the backend URL when uploadFiles() is called when isUploading is false`, () => {
    const testFileList: File[] = Array<File>();
    testFileList.push(testTextFile);

    component.uploader.addToQueue(testFileList);
    component.uploadFiles();

    const req = httpTestingController.expectOne(`${environment.apiUrl}/${uploadFilesURL}`);
    httpTestingController.verify();
  });

});
