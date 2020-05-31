import { Component } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { UserDictionaryService } from '../../../../service/user-dictionary/user-dictionary.service';
import { UserDictionary } from '../../../../service/user-dictionary/user-dictionary.interface';
import { v4 as uuid } from 'uuid';

import { FileUploader, FileItem } from 'ng2-file-upload';
import { isEqual } from 'lodash';
import { MatTabChangeEvent } from '@angular/material/tabs';

import { ErrorStateMatcher } from '@angular/material/core';
import { FormControl, FormGroupDirective, NgForm, Validators } from '@angular/forms';

class MyErrorStateMatcher implements ErrorStateMatcher {
  isErrorState(control: FormControl | null, form: FormGroupDirective | NgForm | null): boolean {
    return !!(control && control.invalid && (control.dirty || control.touched));
  }
}


/**
 * NgbdModalResourceAddComponent is the pop-up component to let
 * user upload dictionary. User can either input the dictionary
 * name and items or drag and drop the dictionary file from
 * local computer.
 *
 * @author Zhaomin Li
 * @author Adam
 *
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

  // These are the form data that will be saved in cache if the user close the modal accidently
  public dictName: string = '';
  public dictContent: string = '';
  public dictSeparator: string = '';
  public dictionaryDescription: string = '';

  // This stores the names of invalid files due to duplication
  public duplicateFiles: string[] = [];

  // This checks whether the user has hover a file over the file upload area
  public haveDropZoneOver: boolean = false;

  // This keeps a counter for the number of invalid files in the uploader due to invalid type
  public invalidFileTypeCounter: number = 0;

  // uploader is a data type introduced in ng2-uploader library, which can be used to capture files and store them
  //  inside the uploader queue.
  public uploader: FileUploader = new FileUploader({url: ''});

  public isInUploadFileTab: boolean = true;
  public isUploading: boolean = false;

  // These are used to create custom form control validators.
  public matcher = new MyErrorStateMatcher();
  public nameValidator: FormControl =  new FormControl('', [Validators.required]);
  public contentValidator: FormControl =  new FormControl('', [Validators.required]);
  public descriptionValidator: FormControl =  new FormControl('', [Validators.required]);

  constructor(
    public activeModal: NgbActiveModal,
    public userDictionaryService: UserDictionaryService
  ) { }

  /**
   * Handles the tab change event in the modal popup
   *
   * @param event
   */
  public onTabChangeEvent(event: MatTabChangeEvent) {
    this.isInUploadFileTab = (event.tab.textLabel === 'Upload');
  }

  /**
   * This method will check if the current form is valid to submit to
   *  the backend. This will be used to disable the submit button
   *  on the upload dictionary panel.
   *
   */
  public checkDictionaryFormValid() {
    return this.dictName !== '' && this.dictContent !== ''
      && this.dictionaryDescription !== '';
  }

  /**
   * This method will record the new dictionary information provided
   *  by the user in the manual add dictionary panel and sends the
   *  data to the backend.
   */
  public addDictionary(): void {
    if (this.isUploading) {
      return;
    }

    this.isUploading = true;
    // assume button is disabled when invalid
    if (!this.checkDictionaryFormValid()) {
      throw new Error('One of the parameters required for creating a dictionary is not provided');
    }

    // when separator is not provided, use comma as default separator
    if (this.dictSeparator === '') { this.dictSeparator = ','; }

    // remove the spaces between the entries in the dictionary content
    const listWithDup = this.dictContent.trim().split(this.dictSeparator)
      .map(dictItem => dictItem.trim()).filter(item => item.length !== 0);

    // remove the duplicates inside the entries
    const contentWithoutDup = listWithDup.filter((value, index) => listWithDup.indexOf(value) === index);

    const userDictionary: UserDictionary = {
      id: this.getDictionaryRandomID(),
      name: this.dictName,
      items: contentWithoutDup,
      description: this.dictionaryDescription
    };

    this.userDictionaryService.putUserDictionaryData(userDictionary)
      .subscribe(() => {
        this.isUploading = false;
        this.resetDictionary();
        this.activeModal.dismiss('close');
      }, error => {
        this.isUploading = false;
        console.log(error);
        alert(`Error encountered: ${error.status}\nMessage: ${error.message}`);
      }
    );
  }

  /**
   * This method will handle the upload files action in the file upload tab.
   *  When this method is called, it will upload the files currently in the file
   *  queue and then clear the queue after the files have been successfully
   *  uploaded to the backend.
   *
   * The FileItem type is a type introduced by the ng2-file-upload library. The uploader
   *  introduced by the library contains a queue of FileItem, where `FileItem._file`
   *  = regular JS File type object.
   *
   */
  public uploadFiles(): void {
    if (this.isUploading) {
      return;
    }

    this.isUploading = true;

    // get a list of Files
    const fileList = this.uploader.queue.map(fileitem => fileitem._file);
    this.userDictionaryService.uploadFileList(fileList)
      .subscribe(() => {
        this.isUploading = false;
        this.resetDictionary();
        this.activeModal.dismiss('close');
      }, error => {
        this.isUploading = false;
        console.log(error);
        alert(`Error encountered: ${error.status}\nMessage: ${error.message}`);
      }
    );
  }

  /**
   * This method handles the event when user click the file upload area
   *  and save their local files to the uploader queue.
   *
   * @param clickUploaEvent
   */
  public saveUploadFile(clickUploadEvent: {target: HTMLInputElement}): void {
    const filelist: FileList | null = clickUploadEvent.target.files;
    if (filelist === null) {
      throw new Error(`browser upload does not work as intended`);
    }

    const listOfFiles: File[] = [];
    for (let i = 0; i < filelist.length; i++) {
      listOfFiles.push(filelist[i]);
    }

    this.uploader.addToQueue(listOfFiles);
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
      this.invalidFileTypeCounter--;
    }

    this.uploader.queue = this.uploader.queue.filter(file => !isEqual(file, item));
    this.checkDuplicateFiles();
  }

  /**
   * When user drag file over the area, this function will be called with a boolean variable
   *  indicating whether there is a file over the uploader view.
   *
   * @param fileOverEvent
   */
  public haveFileOver(fileOverEvent: boolean): void {
    this.haveDropZoneOver = fileOverEvent;
  }

  /**
   * This method takes a single file name and checks if it has the correct type and
   *  if it is duplicated in the uploader queue.
   *
   * @param file
   */
  public checkThisFileInvalid(file: File): boolean {
    return !file.type.includes('text/plain') || this.duplicateFiles.includes(file.name);
  }

  /**
   * This method will loop through the uploader queue to check if there exist files
   *  with identical names. When it founds duplicate files, this method will store
   *  them into `duplicateFiles` for later use.
   * @param
   */
  public checkDuplicateFiles(): void {
    const fileNames = this.uploader.queue.map(item => item._file.name);
    this.duplicateFiles = fileNames.filter((fileName, index) => fileNames.indexOf(fileName) !== index);
  }

  /**
   * This method will detect the file drop events happening in the uploader. It will call
   *  `checkCurrentFilesValid()` to check if the files uploaded to the UI are valid.
   *
   * @param fileDropEvent
   */
  public getFileDropped(fileDropEvent: FileList): void {
    this.checkCurrentFilesValid();
  }

  /**
   * This method deletes all the invalid files inside the upload queue. Invalid
   *  files include files with type errors and duplicate names.
   *
   * @param
   */
  public deleteAllInvalidFile(): void {

    // delete files with invalid types
    const validTypeFiles = this.uploader.queue.filter(
      fileItem => fileItem._file.type.includes('text/plain')
    );
    this.uploader.queue = validTypeFiles;

    this.invalidFileTypeCounter = 0;

    // create map to count files with the same name
    const fileNameMap: Map<string, number> = new Map();

    this.uploader.queue.map(fileitem => fileitem._file.name)
    .forEach(name => {
      const count: number | undefined = fileNameMap.get(name);
      if (count === undefined) {
        fileNameMap.set(name, 1);
      } else {
        fileNameMap.set(name, count + 1);
      }
    });

    // remove all the file with one or more occurance.
    const noDupFiles = this.uploader.queue.filter(fileitem => {
      const count: number | undefined = fileNameMap.get(fileitem._file.name);
      if (count === undefined) {
        throw new Error('the number of occurance should not be undefined');
      }

      // if only 1 item left for that name, return true
      if (count === 1) {
        return true;
      }

      // if there are more than one items that have the same name, decrease 1 in
      //  the map and return false.
      fileNameMap.set(fileitem._file.name, count - 1);
      return false;
    });

    this.uploader.queue = noDupFiles;
    this.checkDuplicateFiles();
  }

  /**
   * This method checks whether the current files in the queue are valid or not.
   *
   * It will first check the file types of the files currently in the uploader
   *  queue. Then, it will check for the duplicate files with identical names
   *  inside the upload queue.
   */
  public checkCurrentFilesValid() {
    this.invalidFileTypeCounter = this.uploader.queue.map(fileitem => fileitem._file)
      .filter(file => !file.type.includes('text/plain')).length;
    this.checkDuplicateFiles();
  }


  /**
   * Generates a random dictionary UUID for the new dictionary generated.
   */
  private getDictionaryRandomID(): string {
    return 'dictionary-' + uuid();
  }

  /**
   * This method will reset all the contents in the add dictionary panel.
   */
  private resetDictionary(): void {
    this.uploader.queue = [];
    this.dictName = '';
    this.dictContent = '';
    this.dictSeparator = '';
    this.dictionaryDescription = '';
    this.isInUploadFileTab = true;
  }
}
