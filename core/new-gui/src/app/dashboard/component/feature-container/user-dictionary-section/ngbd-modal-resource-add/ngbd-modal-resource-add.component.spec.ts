import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import {MatDividerModule} from '@angular/material/divider';
import {MatDialogModule} from '@angular/material/dialog';
import {MatFormFieldModule} from '@angular/material/form-field';

import { NgbModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';

import { HttpClientModule } from '@angular/common/http';
import { NgbdModalResourceAddComponent } from './ngbd-modal-resource-add.component';

import { UserDictionary } from '../../../../type/user-dictionary';

import { FileUploadModule } from 'ng2-file-upload';

describe('NgbdModalResourceAddComponent', () => {
  let component: NgbdModalResourceAddComponent;
  let fixture: ComponentFixture<NgbdModalResourceAddComponent>;

  let addcomponent: NgbdModalResourceAddComponent;
  let addfixture: ComponentFixture<NgbdModalResourceAddComponent>;

  let arrayOfBlob:Blob[] = Array<Blob>();//just for test,needed for creating File object.
  let testTextFile: File=new File( arrayOfBlob,"testTextFile",{type: 'text/plain'});
  let testPicFile: File=new File( arrayOfBlob,"testPicFile",{type: 'image/jpeg'});
  let testDocFile: File=new File( arrayOfBlob,"testDocFile",{type: 'application/msword'});

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgbdModalResourceAddComponent ],
      providers: [
        NgbActiveModal
      ],
      imports: [MatDividerModule,
        MatFormFieldModule,
        MatDialogModule,
        NgbModule.forRoot(),
        FormsModule,
        HttpClientModule,
        FileUploadModule]
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

  it('resourceAddComponent addKey should add a new dictionary', () => {
    addfixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    addcomponent = addfixture.componentInstance;

    let getResultDict = <UserDictionary>{};

    addcomponent.dictContent = 'key1,key2,key3';
    addcomponent.name = 'test';
    addcomponent.separator = ',';
    addcomponent.addedDictionary.subscribe((outd: any) => getResultDict = outd);
    addcomponent.addKey();

    expect(getResultDict.id).toEqual('1');
    expect(getResultDict.name).toEqual('test');
    expect(getResultDict.items).toEqual(['key1', 'key2', 'key3']);
  });

  it('resourceAddComponent initialize variable', () => {
    addfixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    addcomponent = addfixture.componentInstance;

    expect(addcomponent.duplicateFile.length).toEqual(0);
    expect(addcomponent.haveDropZoneOver).toEqual(false);
    expect(addcomponent.invalidFileNumbe).toEqual(0);
    expect(addcomponent.uploader.queue.length).toEqual(0);

  });

  it('resourceAddComponent be able to add text file', () => {
    addfixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    addcomponent = addfixture.componentInstance;
    let testFileList:File[]=Array<File>();
    testFileList.push(testTextFile);

    addcomponent.uploader.addToQueue(testFileList);
    expect(addcomponent.duplicateFile.length).toEqual(0);
    expect(addcomponent.invalidFileNumbe).toEqual(0);
    expect(addcomponent.uploader.queue.length).toEqual(1);

  });

  it('resourceAddComponent be able to add invalid type file', () => {
    addfixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    addcomponent = addfixture.componentInstance;
    expect(addcomponent.duplicateFile.length).toEqual(0);
    expect(addcomponent.invalidFileNumbe).toEqual(0);
    expect(addcomponent.uploader.queue.length).toEqual(0);
    const fileList: FileList = {
      length: 3,
      item: () => null,
      0: testTextFile,
      1:testPicFile,
      2:testDocFile
    };
    let testFileList:File[]=Array<File>();
    testFileList.push(testTextFile);
    testFileList.push(testPicFile);
    testFileList.push(testDocFile);

    addcomponent.uploader.addToQueue(testFileList);
    addcomponent.checkDuplicateFiles();
    addcomponent.getFileDropped(fileList);

    expect(addcomponent.duplicateFile.length).toEqual(0);
    expect(addcomponent.invalidFileNumbe).toEqual(2);
    expect(addcomponent.uploader.queue.length).toEqual(3);

  });

  it('resourceAddComponent count duplicate file', () => {
    addfixture = TestBed.createComponent(NgbdModalResourceAddComponent);
    addcomponent = addfixture.componentInstance;
    expect(addcomponent.duplicateFile.length).toEqual(0);
    expect(addcomponent.invalidFileNumbe).toEqual(0);
    expect(addcomponent.uploader.queue.length).toEqual(0);
    const fileList: FileList = {
      length: 2,
      item: () => null,
      0: testTextFile,
      1: testTextFile,
    };
    let testFileList:File[]=Array<File>();
    testFileList.push(testTextFile);
    testFileList.push(testTextFile);

    addcomponent.uploader.addToQueue(testFileList);
    addcomponent.checkDuplicateFiles();
    addcomponent.getFileDropped(fileList);

    expect(addcomponent.duplicateFile.length).toEqual(1);
    expect(addcomponent.invalidFileNumbe).toEqual(0);
    expect(addcomponent.uploader.queue.length).toEqual(2);

  });
});
