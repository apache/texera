import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { HttpClientModule } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { UserDictionary } from '../../../type/user-dictionary';

import { UserDictionaryService } from '../../../service/user-dictionary/user-dictionary.service';

import { NgbdModalResourceAddComponent } from './ngbd-modal-resource-add/ngbd-modal-resource-add.component';
import { NgbdModalResourceDeleteComponent } from './ngbd-modal-resource-delete/ngbd-modal-resource-delete.component';
import { NgbdModalResourceViewComponent } from './ngbd-modal-resource-view/ngbd-modal-resource-view.component';

import { cloneDeep } from 'lodash';
import { FileItem } from 'ng2-file-upload';

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
  public savedQueue: FileItem[] = [];
  public savedManualDict = {
    name : '',
    content : '',
    separator : ''
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
  * @param
  */
  public openNgbdModalResourceAddComponent(): void {
    const modalRef = this.modalService.open(NgbdModalResourceAddComponent);
    modalRef.componentInstance.uploader.queue = this.savedQueue;
    modalRef.componentInstance.name = this.savedManualDict.name;
    modalRef.componentInstance.dictContent = this.savedManualDict.content;
    modalRef.componentInstance.separator = this.savedManualDict.separator;
    Observable.from(modalRef.result).subscribe(
      (value: {command: number,
              savedQueue: FileItem[],
              savedManualDict: {
                name: string,
                content: string,
                separator: string
              };
              dictionaryData: UserDictionary[]}) => {
        if (value.command === 0) { // user wants to upload the file
          value.dictionaryData.forEach((userdictionary: UserDictionary) => {
            userdictionary.id = (this.UserDictionary.length + 1).toString(); // TODO: need unique ID
            this.UserDictionary.push(userdictionary);
            this.userDictionaryService.addUserDictionaryData(userdictionary);
          });
          this.savedQueue = [];
          this.savedManualDict = {
            name : '',
            content : '',
            separator : ''
          };
        } else if (value.command === 1) { // user close the pop up, but we temporarily store the file array
          this.savedQueue = value.savedQueue;
          this.savedManualDict = {
            name : value.savedManualDict.name,
            content : value.savedManualDict.content,
            separator : value.savedManualDict.separator
          };
        }
      }
    );
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
