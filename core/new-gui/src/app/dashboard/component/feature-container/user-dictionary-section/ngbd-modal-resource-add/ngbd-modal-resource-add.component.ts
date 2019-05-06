import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { UserDictionaryService } from '../../../../service/user-dictionary/user-dictionary.service';
import { UserDictionary, SavedManualDictionary, SavedDictionaryResult } from '../../../../type/user-dictionary';
import { Event } from '_debugger';

import { FileUploader, FileItem } from 'ng2-file-upload';
import isEqual from 'lodash-es/isEqual';
import { MatTabChangeEvent } from '@angular/material';

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

  public isInUploadFileTab: boolean = true;

  constructor(
    public activeModal: NgbActiveModal,
    public userDictionaryService: UserDictionaryService
  ) {

  }

  /**
   * Handles the tab change event in the modal popup
   * @param event
   */
  public onTabChangeEvent(event: MatTabChangeEvent) {
    this.isInUploadFileTab = (event.tab.textLabel === 'Upload');
  }

  /**
  * This method will add user dictionary based on user's manually added contents
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

    const manualDictData: SavedManualDictionary = {name: '', content: '', separator: ''};
    const result: SavedDictionaryResult = {
      command: 0, // commend 0 means user wants to upload the manually created dict.
      savedQueue: [],
      savedManualDictionary: manualDictData
    };
    this.activeModal.close(result);
  }

  /**
   * This method closes the popup modals and temporarily save user uploaded dictionary
   *  but not yet send to backend or user manually added contents
   */
  public onClose() {
    this.deleteAllInvalidFile();
    const result: SavedDictionaryResult = {
      command: 1, // commannd 1 means close the pop up and save the queue.
      savedQueue: this.uploader.queue,
      savedManualDictionary: {
        name : this.name,
        content : this.dictContent,
        separator : this.separator
      }
    };
    this.activeModal.close(result);
  }

  /**
   * This method will handle the upload file action in the user drag file tab
   *
   * This will upload the files currently in the file queue nad then clear the queue
   *  after the files have been successfully uploaded to the user.
   *
   * The FileItem type is a type introduced by the ng2-file-upload library. The uploader
   *  introduced by the library contains a queue of FileItem.
   *
   * FileItem._file => regular JS File type
   *
   * TODO: send http request to the backend and update the user console dictionary list
   */
  public uploadFile(): void {
    const result = {
      command: 0, // command 0 indicates upload
      savedQueue: [],
      dictionaryData: []
    };

    // reset the uploader queue
    this.uploader.queue = [];

    this.activeModal.close(result);
  }

  /**
   * This method handles the event when user click the file upload area
   *  and save their local files to the uploader queue.
   *
   * @param clickUploaEvent
   */
  public clickUploadFile(clickUploaEvent: {target: HTMLInputElement}): void {
    const filelist: FileList | null = clickUploaEvent.target.files;
    if (filelist === null) {
      throw new Error(`browser upload does not work as intended`);
    }

    const listOfFile: File[] = [];
    for (let i = 0; i < filelist.length; i++) {
      listOfFile.push(filelist[i]);
    }

    this.uploader.addToQueue(listOfFile);
    this.checkDuplicateFiles();
  }

  /**
   * This method handles the delete file event in the user drag upload tab
   *  by removing the deleted file from the uploader queue.
   *
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

    this.uploader.queue = this.uploader.queue.filter( // delete all the file with occurrence more than one.
      fileitem => {
        const count: number | undefined = map.get(fileitem._file.name);
        if (count === undefined) { throw new Error('count for map of file shouldn`t be undefined'); }
        return (count === 1 || !(map.set(fileitem._file.name, count - 1)));
      });

    this.checkDuplicateFiles();
  }

}
