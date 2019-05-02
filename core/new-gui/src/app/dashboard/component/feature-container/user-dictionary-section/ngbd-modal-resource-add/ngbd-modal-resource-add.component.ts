import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { UserDictionaryService } from '../../../../service/user-dictionary/user-dictionary.service';
import { UserDictionary } from '../../../../type/user-dictionary';
import { Event } from '_debugger';

import { FileUploader, FileItem } from 'ng2-file-upload';
import isEqual from 'lodash-es/isEqual';

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

  public newDictionary: UserDictionary | undefined; // potential issue
  public name: string = '';
  public dictContent: string = '';
  public separator: string = '';

  public duplicateFile: string[] = []; // store the name of invalid file due to duplication
  public haveDropZoneOver: boolean = false; // state for user draging over the area
  public invalidFileNumber: number = 0; // counter for the number of invalid files in the uploader due to invalid type

  // Uploader is from outside library. Here it is used to capture the file and store it.
  // It is capable of sending file but we don't use it. the url="..." is meanless since we send it through our own way.
  public uploader: FileUploader = new FileUploader({url: 'This string does not matter'});

  constructor(
    public activeModal: NgbActiveModal,
    public userDictionaryService: UserDictionaryService
  ) {

  }

  /**
  * addKey records the new dictionary information (DIY/file) and sends
  * it back to the main component.
  *
  * @param
  */
  public addDictionary(): void {
    // assume button is disabled when invalid
    if (this.name === '' || this.dictContent === '') {
      throw new Error('one of the parameters required for creating a dictionary is not provided');
    }

    // when separator is not provided, use comma as default separator
    if (this.separator === '') { this.separator = ','; }
    this.newDictionary = {
      id : '1', // TODO: need unique ID
      name : this.name,
      items : this.dictContent.split(this.separator),
    };
    const result = {
      command: 0, // commend 0 means user wants to upload the manually created dict.
      savedQueue: [],
      savedManualDict : {
        name : '',
        content : '',
        separator : ''
      },
      dictionaryData: [this.newDictionary]
    };
    this.activeModal.close(result);
  }

  /**
   * Doesn't work yet. Try to hide the drag on the right when the text is short
   * @param
   */
  public checkContentLength(): boolean {
    console.log(this.dictContent.split('\n').length >= 5);
    return this.dictContent.split('\n').length >= 5;
  }

  public onClose() {
    this.deleteAllInvalidFile();
    const result = {
      command: 1, // commannd 1 means close the pop up and save the queue.
      savedQueue: this.uploader.queue,
      savedManualDict: {
        name : this.name,
        content : this.dictContent,
        separator : this.separator
      },
      dictionaryData: []
    };
    this.activeModal.close(result);
  }

  /**
   * For "upload" button. Upload the file in the queue and then clear the queue
   * The FileType object is a type from third part library, link below
   * FileItem: https://github.com/valor-software/ng2-file-upload/blob/development/src/file-upload/file-item.class.ts
   * typeof queue -> [FileItem]
   * typeof FileItem._file -> File
   *
   * TODO: send http request to the backend and update the user console dictionary list
   */
  public uploadFile(): void {
    const result = {
      command: 0, // command 0 indicates upload
      savedQueue: [],
      dictionaryData: []
    };
    // this.uploader.queue.forEach((item) => this.userDictionaryService.uploadDictionary(item._file));
    // const result = {
    //   command: 0,
    //   savedQueue: this.uploader.queue.map((fileitem: FileItem, index: number) => {
    //     const filereader: FileReader = new FileReader();
    //     filereader.readAsText(fileitem._file);
    //     filereader.onload = (e) => {
    //       if (typeof filereader.result === 'string') {
    //         result.savedQueue[index].items = filereader.result.split('\n');
    //       }
    //     };
    //     return <UserDictionary> {
    //       id : '1', // TODO: need unique ID
    //       name : fileitem._file.name.split('.')[0],
    //       items : []
    //     };
    //   }),
    //   dictionaryData: []
    // };
    this.uploader.queue = [];
    this.activeModal.close(result);
  }

  /**
   * handle the event when user click the file upload area
   * move all the files out from the event and put in the queue.
   * @param clickUploaEvent
   */
  public clickUploadFile(clickUploaEvent: {target: HTMLInputElement}): void {
    const filelist: FileList | null = clickUploaEvent.target.files;
    if (filelist === null) {
      throw new Error(`browser upload does not work as intended`);
    }
    const listOfFile: File[] = Array<File>();
    for (let i = 0; i < filelist.length; i++) {
      listOfFile.push(filelist[i]);
    }
    this.uploader.addToQueue(listOfFile);
    this.checkDuplicateFiles();
  }

  /**
   * For "delete" button. Remove the specific file and then check the number
   *  of invalidFile from duplication and type
   * @param item
   */
  public removeFile(item: FileItem): void {
    if (!item._file.type.includes('text/plain')) {
      this.invalidFileNumber--;
    }

    this.uploader.queue = this.uploader.queue.filter(file => !isEqual(file, item));
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
    return !file.type.includes('text/plain') || this.duplicateFile.includes(file.name);
  }

  /**
   * go through the uploader to check if there exist file with the same name, store the duplicate file in this.duplicateFile
   * @param
   */
  public checkDuplicateFiles(): void {
    this.duplicateFile = this.uploader.queue.map(item => item._file.name)
    .filter((fileName: string, index: number, fileArray: string[]) => fileArray.indexOf(fileName) !== index);
  }

  /**
   * check it's type to find if it's valid. if not, counter for invalidfile will plus one. Also check duplicates at the end
   * the real file is store in this.uploader.
   * the type FileList doesn't have .forEach() or .filter()
   * @param fileDropEvent
   */
  public getFileDropped(fileDropEvent: FileList): void {
    for (let i = 0; i < fileDropEvent.length; i++) {
      if (!fileDropEvent[i].type.includes('text/plain')) {
        this.invalidFileNumber++;
      }
    }
    this.checkDuplicateFiles();
  }

  /**
   * delete all the invalid file, including type error and duplication.
   * @param
   */
  public deleteAllInvalidFile(): void {
    this.uploader.queue = this.uploader.queue.filter( // delete invalid type file
      (fileitem: FileItem) => fileitem._file.type.includes('text/plain'));
    this.invalidFileNumber = 0;

    const map: Map<string, number> = new Map(); // create map to count file with the same name
    this.uploader.queue.map(fileitem => fileitem._file.name)
    .forEach(name => {
      const count: number | undefined = map.get(name);
      if (count === undefined) {
        map.set(name, 1);
      } else {
        map.set(name, count + 1);
      }
    });
    // this.uploader.queue.filter(fileitem => {
    //   if (map.get(fileitem._file.name) === 1) {
    //     return true;
    //   } else {
    //     map.set(fileitem._file.name, --map.get(fileitem._file.name));
    //     return false;
    //   }
    // });
    // this.uploader.queue = this.uploader.queue.filter( // delete all the file with occurrence more than one.
    //   fileitem => (map.get(fileitem._file.name) === 1 || !(map.set(fileitem._file.name, map.get(fileitem._file.name) - 1)))
    // );
    this.uploader.queue = this.uploader.queue.filter( // delete all the file with occurrence more than one.
      fileitem => {
        const count: number | undefined = map.get(fileitem._file.name);
        if (count === undefined) { throw new Error('count for map of file shouldn`t be undefined'); }
        return (count === 1 || !(map.set(fileitem._file.name, count - 1)));
      });
    // this.duplicateFile.forEach(
    //   fileName => {
    //     const file: FileItem|undefined = this.uploader.queue.find(fileitem => fileitem._file.name === fileName);
    //     if (file) { this.uploader.queue = this.uploader.queue.filter(fileItem => !isEqual(file, fileItem)); }
    //     });


    this.checkDuplicateFiles();
  }

}
