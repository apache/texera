import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { NgbModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { HttpClientModule } from '@angular/common/http';
import { NgbdModalResourceAddComponent } from './ngbd-modal-resource-add.component';
import { CustomNgMaterialModule } from '../../../../../common/custom-ng-material.module';

import { UserDictionary } from '../../../../service/user-dictionary/user-dictionary.interface';

import { FileUploadModule } from 'ng2-file-upload';

describe('NgbdModalResourceAddComponent', () => {
  let component: NgbdModalResourceAddComponent;
  let fixture: ComponentFixture<NgbdModalResourceAddComponent>;

  let addcomponent: NgbdModalResourceAddComponent;
  let addfixture: ComponentFixture<NgbdModalResourceAddComponent>;

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
        NgbModule.forRoot(),
        FormsModule,
        HttpClientModule,
        FileUploadModule,
        ReactiveFormsModule]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  // it('resourceAddComponent addDictionary should add a new dictionary', () => {
  //   addfixture = TestBed.createComponent(NgbdModalResourceAddComponent);
  //   addcomponent = addfixture.componentInstance;

  //   let getResultDict: UserDictionary;
  //   getResultDict = {
  //     id: '1',
  //     name: 'test',
  //     items: [],
  //   };

  //   addcomponent.dictContent = 'key1,key2,key3';
  //   addcomponent.dictName = 'test';
  //   addcomponent.dictSeparator = ',';
  //   addcomponent.addDictionary.subscribe((outd: any) => getResultDict = outd);
  //   addcomponent.addDictionary();

  //   expect(getResultDict.id).toEqual('1');
  //   expect(getResultDict.name).toEqual('test');
  //   expect(getResultDict.items).toEqual([]);
  // });

  it('resourceAddComponent initialize variable', () => {
    addfixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    addcomponent = addfixture.componentInstance;

    expect(addcomponent.duplicateFile.length).toEqual(0);
    expect(addcomponent.haveDropZoneOver).toEqual(false);
    expect(addcomponent.invalidFileNumber).toEqual(0);
    expect(addcomponent.uploader.queue.length).toEqual(0);

  });

  it('resourceAddComponent be able to add text file', () => {
    addfixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    addcomponent = addfixture.componentInstance;
    const testFileList: File[] = Array<File>();
    testFileList.push(testTextFile);

    addcomponent.uploader.addToQueue(testFileList);
    expect(addcomponent.duplicateFile.length).toEqual(0);
    expect(addcomponent.invalidFileNumber).toEqual(0);
    expect(addcomponent.uploader.queue.length).toEqual(1);

  });

  it('resourceAddComponent be able to add invalid type file', () => {
    addfixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    addcomponent = addfixture.componentInstance;
    expect(addcomponent.duplicateFile.length).toEqual(0);
    expect(addcomponent.invalidFileNumber).toEqual(0);
    expect(addcomponent.uploader.queue.length).toEqual(0);
    const fileList: FileList = {
      length: 3,
      item: () => null,
      0: testTextFile,
      1: testPicFile,
      2: testDocFile
    };
    const testFileList: File[] = Array<File>();
    testFileList.push(testTextFile);
    testFileList.push(testPicFile);
    testFileList.push(testDocFile);

    addcomponent.uploader.addToQueue(testFileList);
    addcomponent.checkDuplicateFiles();
    addcomponent.getFileDropped(fileList);

    expect(addcomponent.duplicateFile.length).toEqual(0);
    expect(addcomponent.invalidFileNumber).toEqual(2);
    expect(addcomponent.uploader.queue.length).toEqual(3);

  });

  it('resourceAddComponent count duplicate file', () => {
    addfixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    addcomponent = addfixture.componentInstance;
    expect(addcomponent.duplicateFile.length).toEqual(0);
    expect(addcomponent.invalidFileNumber).toEqual(0);
    expect(addcomponent.uploader.queue.length).toEqual(0);
    const fileList: FileList = {
      length: 2,
      item: () => null,
      0: testTextFile,
      1: testTextFile,
    };
    const testFileList: File[] = Array<File>();
    testFileList.push(testTextFile);
    testFileList.push(testTextFile);

    addcomponent.uploader.addToQueue(testFileList);
    addcomponent.checkDuplicateFiles();
    addcomponent.getFileDropped(fileList);

    expect(addcomponent.duplicateFile.length).toEqual(1);
    expect(addcomponent.invalidFileNumber).toEqual(0);
    expect(addcomponent.uploader.queue.length).toEqual(2);

  });

  it('resourceAddComponent can delete the invalid file in the queue', () => {
    addfixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    addcomponent = addfixture.componentInstance;
    expect(addcomponent.duplicateFile.length).toEqual(0);
    expect(addcomponent.invalidFileNumber).toEqual(0);
    expect(addcomponent.uploader.queue.length).toEqual(0);
    const fileList: FileList = {
      length: 4,
      item: () => null,
      0: testTextFile,
      1: testPicFile,
      2: testDocFile,
      3: testTextFile
    };
    const testFileList: File[] = Array<File>();
    testFileList.push(testTextFile);
    testFileList.push(testPicFile);
    testFileList.push(testDocFile);
    testFileList.push(testTextFile);

    addcomponent.uploader.addToQueue(testFileList);
    addcomponent.checkDuplicateFiles();
    addcomponent.getFileDropped(fileList);

    addcomponent.deleteAllInvalidFile();

    expect(addcomponent.duplicateFile.length).toEqual(0);
    expect(addcomponent.invalidFileNumber).toEqual(0);
    expect(addcomponent.uploader.queue.length).toEqual(1);

  });
});
