import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { HttpClientModule } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { UserDictionary} from '../../../type/user-dictionary';

import { UserDictionaryService } from '../../../service/user-dictionary/user-dictionary.service';

import { NgbdModalResourceAddComponent } from './ngbd-modal-resource-add/ngbd-modal-resource-add.component';
import { NgbdModalResourceDeleteComponent } from './ngbd-modal-resource-delete/ngbd-modal-resource-delete.component';
import { NgbdModalResourceViewComponent } from './ngbd-modal-resource-view/ngbd-modal-resource-view.component';

import { cloneDeep } from 'lodash';
import { FileItem } from 'ng2-file-upload';

interface SavedData extends Readonly<{
  name: string;
  content: string;
  separator: string;
  savedQueue: FileItem[]
}> { }

/**
 * UserDictionarySectionComponent is the main interface
 * for managing all the user dictionaries. On this interface,
 * user can view all the dictionaries by the order he/she defines,
 * upload dictionary, and delete dictionary.
 *
 * @author Zhaomin Li
 */
@Component({
  selector: 'texera-user-dictionary-section',
  templateUrl: './user-dictionary-section.component.html',
  styleUrls: ['./user-dictionary-section.component.scss', '../../dashboard.component.scss']
})
export class UserDictionarySectionComponent implements OnInit {

  public UserDictionary: UserDictionary[] = [];
  public savedData: SavedData = {
    name: '',
    content: '',
    separator: '',
    savedQueue: []
  };

  constructor(
    private userDictionaryService: UserDictionaryService,
    private modalService: NgbModal
  ) { }

  ngOnInit() {
    this.userDictionaryService.getUserDictionaryData().subscribe(
      value => this.UserDictionary = value,
    );
  }

  /**
  * openNgbdModalResourceViewComponent triggers the view dictionary
  * component. It calls the method in service to send request to
  * backend and to fetch info package for a specific dictionary.
  * addItemEmitter receives information about adding a item
  * into dictionary and calls method in service. deleteItemEmitter
  * receives information about deleting a item in dictionary and
  * calls method in service.
  *
  * @param dictionary: the dictionary that user wants to view
  */
  public openNgbdModalResourceViewComponent(dictionary: UserDictionary): void {
    const modalRef = this.modalService.open(NgbdModalResourceViewComponent);
    modalRef.componentInstance.dictionary = cloneDeep(dictionary);

    const addItemEventEmitter = <EventEmitter<string>>(modalRef.componentInstance.addedName);
    const subscription = addItemEventEmitter
      .do(value => console.log(value))
      .subscribe(
        value => {
          dictionary.items.push(value);
          modalRef.componentInstance.dictionary = cloneDeep(dictionary);
        }
      );

    const deleteItemEventEmitter = <EventEmitter<string>>(modalRef.componentInstance.deleteName);
    const delSubscription = deleteItemEventEmitter
      .do(value => console.log(value))
      .subscribe(
        value => {
          dictionary.items = dictionary.items.filter(obj => obj !== value);
          modalRef.componentInstance.dictionary = cloneDeep(dictionary);
        }
      );
  }

  /**
  * openNgbdModalResourceAddComponent triggers the add dictionary
  * component. The component returns the information of new dictionary,
  *  and this method adds new dictionary in to the list on UI. It calls
  * the addUserDictionaryData method in to store user-define dictionary,
  * or uploadDictionary in service to upload dictionary file.
  *
  *
  * response received from NgbdModalResourceAddComponent:
  * commend === 0:
  * User wants to upload the manual dictionary
  * - clear the current saved value
  * - upload the manual dictionary received from response
  * - download new dictionary from the server and refresh the view
  *
  * commend === 1:
  * User wants to upload the files in the queue
  * - clear the current saved value
  * - upload the file in the queue received from response
  * - download new dictionary from the server and refresh the view
  *
  * commend === 2:
  * user close the pop up, but we temporarily store the file queue and manual dictionary
  * - save the manual dictionary and files in the queue from the response
  *
  * @param
  */
  public openNgbdModalResourceAddComponent(): void {
    const modalRef = this.modalService.open(NgbdModalResourceAddComponent, {
      beforeDismiss: (): boolean => {
        this.savedData = {
          name: modalRef.componentInstance.dictNname,
          content: modalRef.componentInstance.dictContent,
          separator: modalRef.componentInstance.dictSeparator,
          savedQueue: modalRef.componentInstance.uploader.queue
        };
        return true;
      }
    });
    // initialize the value from saving, used when user close the popup and then temporarily save dictionary.
    modalRef.componentInstance.uploader.queue = this.savedData.savedQueue;
    modalRef.componentInstance.dictName = this.savedData.name;
    modalRef.componentInstance.dictContent = this.savedData.content;
    modalRef.componentInstance.dictSeparator = this.savedData.separator;
  }

  /**
  * openNgbdModalResourceDeleteComponent trigger the delete
  * dictionary component. If user confirms the deletion, the method
  * sends message to frontend and delete the dicrionary on frontend.
  * It calls the deleteUserDictionaryData method in service which
  * using backend API.
  *
  * @param dictionary: the dictionary that user wants to remove
  */
  public openNgbdModalResourceDeleteComponent(dictionary: UserDictionary): void {
    const modalRef = this.modalService.open(NgbdModalResourceDeleteComponent);
    modalRef.componentInstance.dictionary = cloneDeep(dictionary);

    const deleteItemEventEmitter = <EventEmitter<boolean>>(modalRef.componentInstance.deleteDict);
    const subscription = deleteItemEventEmitter
      .subscribe(
        (value: UserDictionary) => {
          if (value) {
            this.UserDictionary = this.UserDictionary.filter(obj => obj.id !== dictionary.id);
            this.userDictionaryService.deleteUserDictionaryData(dictionary);
          }
        }
      );

  }

  /**
  * sort the dictionary by name in ascending order
  *
  * @param
  */
  public ascSort(): void {
    this.UserDictionary.sort((t1, t2) => {
      if (t1.name.toLowerCase() > t2.name.toLowerCase()) { return 1; }
      if (t1.name.toLowerCase() < t2.name.toLowerCase()) { return -1; }
      return 0;
    });
  }

  /**
  * sort the dictionary by name in descending order
  *
  * @param
  */
  public dscSort(): void {
    this.UserDictionary.sort((t1, t2) => {
      if (t1.name.toLowerCase() > t2.name.toLowerCase()) { return -1; }
      if (t1.name.toLowerCase() < t2.name.toLowerCase()) { return 1; }
      return 0;
    });
  }

  /**
  * sort the dictionary by size
  *
  * @param
  */
  public sizeSort(): void {
    this.UserDictionary.sort((t1, t2) => {
      if (t1.items.length > t2.items.length) { return -1; }
      if (t1.items.length < t2.items.length) { return 1; }
      return 0;
    });
  }

}
