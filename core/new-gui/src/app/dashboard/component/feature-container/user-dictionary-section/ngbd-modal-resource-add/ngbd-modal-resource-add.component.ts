import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { UserDictionaryService } from '../../../../service/user-dictionary/user-dictionary.service';
import { UserDictionary } from '../../../../type/user-dictionary';
import { Event } from '_debugger';

import { FileUploader, FileItem } from 'ng2-file-upload';

/**
 * NgbdModalResourceAddComponent is the pop-up component to let
 * user upload dictionary. User can either input the dictionary
 * name and items or upload the dictionary file from local computer.
 *
 * @author Zhaomin Li
 * @author Adam
 */
@Component({
  selector: 'texera-resource-section-add-dict-modal',
  templateUrl: 'ngbd-modal-resource-add.component.html',
  styleUrls: ['./ngbd-modal-resource-add.component.scss', '../../../dashboard.component.scss'],
  providers: [
    UserDictionaryService,
  ]

})
export class NgbdModalResourceAddComponent {
  @Output() addedDictionary =  new EventEmitter<UserDictionary>();

  public newDictionary: any; // potential issue
  public name: string = '';
  public dictContent: string = '';
  public separator: string = '';
  public selectFile: any = null; // potential issue

  public duplicateFile: string[] = []; // store the name of invalid file due to duplication
  public haveDropZoneOver: boolean = false; // state for user draging over the area
  public invalidFileNumbe: number = 0; // counter for the number of invalid file in the uploader due to invalid type
  // uploader: https://github.com/valor-software/ng2-file-upload/blob/development/src/file-upload/file-uploader.class.ts
  // Uploader is from outside library. Here it is used to capture the file and store it.
  // It is capable of sending file but we don't use it. the url="..." is meanless since we send it through our own way.
  public uploader: FileUploader = new FileUploader({url: 'This string does not matter'});

  constructor(
    public activeModal: NgbActiveModal,
    public userDictionaryService: UserDictionaryService
  ) {}

  public onChange(event: any): void {
    this.selectFile = event.target.files[0];
  }

  public onClose(): void {
    this.activeModal.close('Close');
  }

  /**
  * addKey records the new dictionary information (DIY/file) and sends
  * it back to the main component.
  *
  * @param
  */
  public addKey(): void {

    if (this.selectFile !== null) {
        this.userDictionaryService.uploadDictionary(this.selectFile);
    }

    if (this.name !== '') {
      this.newDictionary = <UserDictionary> {
        id : '1',
        name : this.name,
        items : [],
      };

      if (this.dictContent !== '' && this.separator !== '') {
        this.newDictionary.items = this.dictContent.split(this.separator);
      }
      this.addedDictionary.emit(this.newDictionary);

      this.name = '';
      this.dictContent = '';
      this.separator = '';
    }
    this.onClose();
  }

  /**
   * For "upload" button. Upload the file in the queue and then clear the queue
   * The FileType object is a type from third part library, link below
   * FileItem: https://github.com/valor-software/ng2-file-upload/blob/development/src/file-upload/file-item.class.ts
   * typeof queue -> [FileItem]
   * typeof FileItem._file -> File
   */
  public uploadFile(): void {
    this.uploader.queue.forEach((item) => this.userDictionaryService.uploadDictionary(item._file));
    this.uploader.clearQueue();
  }

  public clickUploadFile(clickUploaEvent: any): void {
    const filelist: FileList = clickUploaEvent.target.files;
    const listOfFile: File[] = Array<File>();
    for (let i = 0; i < filelist.length; i++) {
      listOfFile.push(filelist[i]);
    }
    this.uploader.addToQueue(listOfFile);
    console.log(clickUploaEvent);
    console.log(typeof clickUploaEvent);
    console.log(clickUploaEvent.target.files);
    // this.uploader.addToQueue()
  }

  /**
   * For "delete" button. Remove the specific file and then check the number of invalidFile from duplication and type
   */
  public removeFile(item: FileItem): void {
    if (!item._file.type.includes('text')) {
      this.invalidFileNumbe--;
    }
    item.remove();
    this.checkDuplicateFiles();
  }

  /**
   * when user drag file over the area, this function will be called with a bool
   * @param fileOverEvent
   */
  public haveFileOver(fileOverEvent: boolean): void {
    this.haveDropZoneOver = fileOverEvent;
  }

  /**
   * passed by single file name. check if this file is duplicated in the array. True indicate invalid
   * @param file
   */
  public checkThisFileInvalid(file: File): boolean {
    return !file.type.includes('text') || this.duplicateFile.includes(file.name);
  }

  /**
   * go through the uploader to check if there exist file with the same name, store the duplicate file in this.duplicateFile
   */
  public checkDuplicateFiles(): void {
    const filesArray = this.uploader.queue.map(item => item._file.name);
    this.duplicateFile = filesArray.filter(
      (fileName: string, index: number, fileArray: string[]) => fileArray.indexOf(fileName) !== index);
  }

  /**
   * check it's type to find if it's valid. if not, counter for invalidfile will plus one. Also check duplicates at the end
   * the real file is store in this.uploader.
   * @param fileDropEvent
   */
  public getFileDropped(fileDropEvent: FileList): void {
    const filelist: FileList = fileDropEvent;
    for (let i = 0; i < filelist.length; i++) {
      if (!filelist[i].type.includes('text')) {
        this.invalidFileNumbe++;
      }
    }
    this.checkDuplicateFiles();
  }

}
