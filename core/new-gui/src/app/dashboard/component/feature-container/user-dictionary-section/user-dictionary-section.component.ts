import { Component, ViewChild, Directive, ViewContainerRef} from '@angular/core';

import { Observable } from 'rxjs/Observable';
import { UserDictionary } from '../../../service/user-dictionary/user-dictionary.interface';

import { UserDictionaryService } from '../../../service/user-dictionary/user-dictionary.service';

import { NgbdModalResourceAddComponent } from './ngbd-modal-resource-add/ngbd-modal-resource-add.component';
import { NgbdModalResourceDeleteComponent } from './ngbd-modal-resource-delete/ngbd-modal-resource-delete.component';
import { NgbdModalResourceViewComponent } from './ngbd-modal-resource-view/ngbd-modal-resource-view.component';

import { ComponentInserterService } from '../../../../service/component-inserter.service';

import { cloneDeep } from 'lodash';
import { FileItem } from 'ng2-file-upload';

@Directive({
  selector: '[texeraSaveProjectSectionModalRoot]',
})
export class ModalRootDirective {
  constructor(public viewContainerRef: ViewContainerRef) { }
}

export interface SavedAddDictionaryState extends Readonly<{
  name: string;
  content: string;
  separator: string;
  description: string;
  savedQueue: FileItem[];
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
export class UserDictionarySectionComponent {

  public userDictionaries: UserDictionary[] = [];

  public savedState: SavedAddDictionaryState = {
    name: '',
    content: '',
    separator: '',
    description: '',
    savedQueue: [],
  };

  @ViewChild(ModalRootDirective, {static: true}) modalRoot!: ModalRootDirective;

  constructor(
    private userDictionaryService: UserDictionaryService,
    private componentInserter: ComponentInserterService,
    private viewContainerRef: ViewContainerRef
  ) {
    this.refreshUserDictionary();
  }

  public refreshUserDictionary(): void {
    this.userDictionaryService.listUserDictionaries().subscribe(
      value => this.userDictionaries = value,
    );
  }

  /**
  * openNgbdModalResourceViewComponent triggers the view dictionary
  * component. It calls the method in service to send request to
  * backend and to fetch info package for a specific dictionary.
  * addModelObservable receives information about adding a item
  * into dictionary and calls method in service. deleteModelObservable
  * receives information about deleting a item in dictionary and
  * calls method in service.
  *
  * @param dictionary: the dictionary that user wants to view
  */
  public openNgbdModalResourceViewComponent(dictionary: UserDictionary): void {
    const modalRef = this.componentInserter.injectComponent<NgbdModalResourceViewComponent>(
      NgbdModalResourceViewComponent,
      this.viewContainerRef);
    modalRef.instance.result.then(() => {
      this.refreshUserDictionary();
    });
    modalRef.instance.dictionary = cloneDeep(dictionary);
  }

  /**
  * openNgbdModalResourceAddComponent triggers the add dictionary
  * component. The component returns the information of new dictionary,
  *  and this method adds new dictionary in to the list on UI. It calls
  * the addUserDictionaryData method in to store user-define dictionary,
  * or uploadDictionary in service to upload dictionary file.
  *
  *
  * @param
  */
  public openNgbdModalResourceAddComponent(): void {
    const modalRef = this.componentInserter.injectComponent<NgbdModalResourceAddComponent>(
      NgbdModalResourceAddComponent,
      this.viewContainerRef);
    modalRef.instance.result.catch(() => {
      this.savedState = {
      name: modalRef.instance.dictName,
      content: modalRef.instance.dictContent,
      separator: modalRef.instance.dictSeparator,
      description: modalRef.instance.dictionaryDescription,
      savedQueue: modalRef.instance.uploader.queue
      };

      // refresh the dictionaries in the panel to show the new updates done by users
      this.refreshUserDictionary();
    });

    // initialize the value from saving, used when user close the popup and then temporarily save dictionary.
    modalRef.instance.uploader.queue = this.savedState.savedQueue;
    modalRef.instance.dictName = this.savedState.name;
    modalRef.instance.dictContent = this.savedState.content;
    modalRef.instance.dictSeparator = this.savedState.separator;
    modalRef.instance.dictionaryDescription = this.savedState.description;

    // checks if the files saved in the state are valid.
    modalRef.instance.checkCurrentFilesValid();
  }

  /**
  * openNgbdModalResourceDeleteComponent trigger the delete
  * dictionary component. If user confirms the deletion, the method
  * sends message to frontend and delete the dicrionary on backend and
  * update the frontend. It calls the deleteUserDictionaryData method
  * in service which using backend API.
  *
  * @param dictionary: the dictionary that user wants to remove
  */
  public openNgbdModalResourceDeleteComponent(dictionary: UserDictionary): void {
    const modalRef = this.componentInserter.injectComponent<NgbdModalResourceDeleteComponent>(
      NgbdModalResourceDeleteComponent,
      this.viewContainerRef);
    modalRef.instance.dictionary = cloneDeep(dictionary);

    Observable.from(modalRef.instance.result).subscribe(
      (confirmDelete: boolean) => {
        if (confirmDelete) {
          this.userDictionaryService.deleteUserDictionaryData(dictionary.id).subscribe(res => {
            this.refreshUserDictionary();
          });
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
    this.userDictionaries.sort((t1, t2) => {
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
    this.userDictionaries.sort((t1, t2) => {
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
    this.userDictionaries.sort((t1, t2) => {
      if (t1.items.length > t2.items.length) { return -1; }
      if (t1.items.length < t2.items.length) { return 1; }
      return 0;
    });
  }

}
