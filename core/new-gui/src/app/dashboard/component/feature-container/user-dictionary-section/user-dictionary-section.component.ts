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
    console.log(this.UserDictionary);
  }

  openNgbdModalResourceViewComponent(dictionary: UserDictionary) {
    const modalRef = this.modalService.open(NgbdModalResourceViewComponent);
    // const copyDict = <UserDictionary> {
    //   id: dictionary.id,
    //   name: dictionary.name,
    //   items: dictionary.items,
    //   description: dictionary.description
    // };
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

  openNgbdModalResourceAddComponent() {
    const modalRef = this.modalService.open(NgbdModalResourceAddComponent);

    const addItemEventEmitter = <EventEmitter<UserDictionary>>(modalRef.componentInstance.addedDictionary);
    const subscription = addItemEventEmitter
      .do(value => value.id = (this.UserDictionary.length + 1).toString()) // leave for correct
      .subscribe(
        value => {
          console.log(value);
          this.UserDictionary.push(value);
          this.userDictionaryService.addUserDictionaryData(value);
        }
      );

  }

  openNgbdModalResourceDeleteComponent(dictionary: UserDictionary) {
    const modalRef = this.modalService.open(NgbdModalResourceDeleteComponent);
    modalRef.componentInstance.dictionary = cloneDeep(dictionary);

    const deleteItemEventEmitter = <EventEmitter<boolean>>(modalRef.componentInstance.deleteDict);
    const subscription = deleteItemEventEmitter
      .subscribe(
        value => {
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

// sub component for view-dictionary popup window
@Component({
  selector: 'texera-resource-section-modal',
  template: `
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
  <div class="modal-header">
    <h4 class="modal-title">{{dictionary.name}}</h4>
    <button type="button" class="close" aria-label="Close" (click)="onClose()">
      <span aria-hidden="true">&times;</span>
    </button>
  </div>

  <div class="modal-body">
    <!--<p>[ {{dictionary.items}} ]</p>-->

    <mat-chip-list #chipList>
      <mat-chip *ngFor="let item of dictionary.items" [selectable]="selectable" [removable]="removable" (removed) = "remove(item)">
        {{item}}
        <mat-icon matChipRemove  *ngIf="removable">
          <i class="fa">&#xf057;</i>
        </mat-icon>
      </mat-chip>
    </mat-chip-list>
    <br>
    <mat-divider></mat-divider>
    <br>

    <mat-dialog-actions>
      <input *ngIf="ifAdd" matInput [(ngModel)]="name" placeholder="Add into dictionary">
      <button type="button" class="btn btn-outline-dark add-button" (click)="addKey()"  >Add</button>
    </mat-dialog-actions>
  </div>

  <div class="modal-footer">
    <button type="button" class="btn btn-outline-dark" (click)="onClose()">Close</button>
  </div>
  `,
  styleUrls: ['./user-dictionary-section.component.scss', '../../dashboard.component.scss']

})
export class NgbdModalResourceViewComponent {
  @Input() dictionary;
  @Output() addedName =  new EventEmitter<string>();
  @Output() deleteName =  new EventEmitter<string>();

  public name: string;
  public ifAdd = false;
  public removable = true;
  public visible = true;
  public selectable = true;

  constructor(public activeModal: NgbActiveModal) {}

  onClose() {
    this.activeModal.close('Close');
  }

  addKey() {

    if (this.ifAdd && this.name !== undefined) {
      console.log('add ' + this.name + ' into dict ' + this.dictionary.name);
      this.addedName.emit(this.name);
      this.name = undefined;
    }
    this.ifAdd = !this.ifAdd;

  }

  remove(item: any): void {
    console.log('delete ' + item + ' in dict ' + this.dictionary.name);
    this.deleteName.emit(item);
  }
}



// sub component for add-dictionary popup window
@Component({
  selector: 'texera-resource-section-add-dict-modal',
  template: `
  <div class="modal-header">
    <h4 class="modal-title">Add Dictionary</h4>
    <button type="button" class="close" aria-label="Close" (click)="onClose()">
      <span aria-hidden="true">&times;</span>
    </button>
  </div>

  <div class="modal-body">
    <mat-dialog-content class= "add-dictionary-container">
      <mat-form-field class= "name-area">
        <input  matInput [(ngModel)]="name" placeholder="Name of Dictionary">
      </mat-form-field>
      <mat-form-field class= "content-area" >
        <textarea matInput
          [(ngModel)]="dictContent" placeholder="Content of Dictionary"
          matTextareaAutosize matAutosizeMinRows="3">
        </textarea>
      </mat-form-field>
      <mat-form-field class= "separator-area">
        <input  matInput [(ngModel)]="separator" placeholder="Content Separator (' , '    ' \\t '    ' \\n ')">
      </mat-form-field>
      <div class="transmit-area">
        <mat-divider></mat-divider>
        <span style="font-family:Roboto, Arial, sans-serif;">OR</span>
        <mat-divider></mat-divider>
      </div>

      <div class="file-upload-area" id="hide">
        <label class="btn-primary" ngbButtonLabel>
          <input type="file"
            accept=".txt"  class="file-upload-btn" (change)="onChange($event)"/>
          <span>Upload Your Dictionary</span>
        </label>
      </div>

    </mat-dialog-content>
  </div>

  <div class="modal-footer">
    <button type="button" class="btn btn-outline-dark add-button" (click)="addKey()"  >Add</button>
    <button type="button" class="btn btn-outline-dark" (click)="onClose()">Close</button>
  </div>
  `,
  styleUrls: ['./user-dictionary-section.component.scss', '../../dashboard.component.scss'],
  providers: [
    UserDictionaryService,
  ]

})
export class NgbdModalResourceAddComponent {
  @Output() addedDictionary =  new EventEmitter<UserDictionary>();

  public newDictionary: UserDictionary;
  public name: string;
  public dictContent: string;
  public separator: string;
  public selectFile = null;

  constructor(
    public activeModal: NgbActiveModal,
    public userDictionaryService: UserDictionaryService
  ) {}

  onChange(event) {
    this.selectFile = event.target.files[0];
  }

  onClose() {
    this.activeModal.close('Close');
  }

  addKey() {

    if (this.selectFile !== null) {
        // console.log(this.selectFile);
        this.userDictionaryService.uploadDictionary(this.selectFile);
    }

    if (this.name !== undefined) {
      console.log('add ' + this.name );
      this.newDictionary = <UserDictionary> {
        id : '1',
        name : this.name,
        items : [],
      };

      if (this.dictContent !== undefined && this.separator !== undefined) {
        this.newDictionary.items = this.dictContent.split(this.separator);
      }
      this.addedDictionary.emit(this.newDictionary);

      this.name = undefined;
      this.dictContent = undefined;
      this.separator = undefined;
    }
    this.onClose();
  }
}


// sub component for delete-dictionary popup window
@Component({
  selector: 'texera-resource-section-delete-dict-modal',
  template: `
  <div class="modal-header">
    <h4 class="modal-title">Delete the Dictionary</h4>
    <button type="button" class="close" aria-label="Close" (click)="onClose()">
      <span aria-hidden="true">&times;</span>
    </button>
  </div>

  <div class="modal-body">
    <mat-dialog-actions>
      <p class="modal-title">Confirm to Delete the Dictionary {{dictionary.name}}</p>
    </mat-dialog-actions>
  </div>

  <div class="modal-footer">
    <button type="button" class="btn btn-outline-dark delete-confirm-button" (click)="deleteDictionary()"  >Confirm</button>
    <button type="button" class="btn btn-outline-dark" (click)="onClose()">Close</button>
  </div>
  `,
  styleUrls: ['./user-dictionary-section.component.scss', '../../dashboard.component.scss']

})
export class NgbdModalResourceDeleteComponent {
  @Input() dictionary;
  @Output() deleteDict =  new EventEmitter<boolean>();

  constructor(public activeModal: NgbActiveModal) {}

  onClose() {
    this.activeModal.close('Close');
  }

  deleteDictionary() {
    console.log('delete ' + this.dictionary.name);
    this.deleteDict.emit(true);
    this.onClose();
  }

}
