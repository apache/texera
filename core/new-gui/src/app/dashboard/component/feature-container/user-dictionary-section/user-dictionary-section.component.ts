import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { MatDialog, MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';
import { MatIconModule } from '@angular/material/icon';
import { MatChipsModule } from '@angular/material/chips';
import { HttpClientModule } from '@angular/common/http';

import {MatChipInputEvent} from '@angular/material';
import {ENTER, COMMA} from '@angular/cdk/keycodes';


import { Observable } from 'rxjs/Observable';
import { UserDictionary } from '../../../type/user-dictionary';

import { UserDictionaryService } from '../../../service/user-dictionary/user-dictionary.service';

import { NgbdModalResourceAddComponent } from './ngbd-modal-resource-add/ngbd-modal-resource-add.component';
import { NgbdModalResourceDeleteComponent } from './ngbd-modal-resource-delete/ngbd-modal-resource-delete.component';
import { NgbdModalResourceViewComponent } from './ngbd-modal-resource-view/ngbd-modal-resource-view.component';

import { cloneDeep } from 'lodash';


@Component({
  selector: 'texera-user-dictionary-section',
  templateUrl: './user-dictionary-section.component.html',
  styleUrls: ['./user-dictionary-section.component.scss', '../../dashboard.component.scss']
})
export class UserDictionarySectionComponent implements OnInit {

  public UserDictionary: UserDictionary[] = [];

  constructor(
    private userDictionaryService: UserDictionaryService,
    private modalService: NgbModal
  ) { }

  ngOnInit() {
    this.userDictionaryService.getUserDictionaryData().subscribe(
      value => this.UserDictionary = value,
    );
  }

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

  public openNgbdModalResourceAddComponent(): void {
    const modalRef = this.modalService.open(NgbdModalResourceAddComponent);

    const addItemEventEmitter = <EventEmitter<UserDictionary>>(modalRef.componentInstance.addedDictionary);
    const subscription = addItemEventEmitter
      .do(value => value.id = (this.UserDictionary.length + 1).toString()) // leave for correct
      .subscribe(
        value => {
          this.UserDictionary.push(value);
          this.userDictionaryService.addUserDictionaryData(value);
        }
      );

  }

  public openNgbdModalResourceDeleteComponent(dictionary: UserDictionary): void {
    const modalRef = this.modalService.open(NgbdModalResourceDeleteComponent);
    modalRef.componentInstance.dictionary = cloneDeep(dictionary);

    const deleteItemEventEmitter = <EventEmitter<boolean>>(modalRef.componentInstance.deleteDict);
    const subscription = deleteItemEventEmitter
      .subscribe(
        (value: any) => {
          if (value) {
            this.UserDictionary = this.UserDictionary.filter(obj => obj.id !== dictionary.id);
            this.userDictionaryService.deleteUserDictionaryData(dictionary);
          }
        }
      );

  }

  public ascSort(): void {
    this.UserDictionary.sort((t1, t2) => {
      if (t1.name.toLowerCase() > t2.name.toLowerCase()) { return 1; }
      if (t1.name.toLowerCase() < t2.name.toLowerCase()) { return -1; }
      return 0; });
  }

  public dscSort(): void {
    this.UserDictionary.sort((t1, t2) => {
      if (t1.name.toLowerCase() > t2.name.toLowerCase()) { return -1; }
      if (t1.name.toLowerCase() < t2.name.toLowerCase()) { return 1; }
      return 0; });
  }

  public sizeSort(): void {
    this.UserDictionary.sort((t1, t2) => {
      if (t1.items.length > t2.items.length) { return -1; }
      if (t1.items.length < t2.items.length) { return 1; }
      return 0; });
  }

}
