import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { UserDictionaryService } from '../../../../service/user-dictionary/user-dictionary.service';
import { UserDictionary } from '../../../../type/user-dictionary';
import { Event } from '_debugger';

/**
 * NgbdModalResourceAddComponent is the pop-up component to let
 * user upload dictionary. User can either input the dictionary
 * name and items or upload the dictionary file from local computer.
 *
 * @author Zhaomin Li
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
}
