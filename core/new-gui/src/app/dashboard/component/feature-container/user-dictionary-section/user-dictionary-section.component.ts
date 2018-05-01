import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { MatDialog, MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';
import { HttpClientModule } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { UserDictionary } from '../../../type/user-dictionary';

import { UserDictionaryService } from '../../../service/user-dictionary/user-dictionary.service';
import { StubUserDictionaryService } from '../../../service/user-dictionary/stub-user-dictionary.service';

@Component({
  selector: 'texera-user-dictionary-section',
  templateUrl: './user-dictionary-section.component.html',
  styleUrls: ['./user-dictionary-section.component.scss', '../../dashboard.component.scss']
})
export class UserDictionarySectionComponent implements OnInit {

  public UserDictionary: UserDictionary[] = [];

  constructor(
    private mockUserDictionaryService: StubUserDictionaryService,
    private modalService: NgbModal
  ) { }

  ngOnInit() {
    this.mockUserDictionaryService.getUserDictionaryData().subscribe(
      value => this.UserDictionary = value,
    );
    console.log(this.UserDictionary);
  }

  openNgbdModalResourceViewComponent(dictionary: UserDictionary) {
    const modalRef = this.modalService.open(NgbdModalResourceViewComponent);
    const copyDict = <UserDictionary> {
      id: dictionary.id,
      name: dictionary.name,
      items: dictionary.items,
      description: dictionary.description
    };
    modalRef.componentInstance.dictionary = copyDict;

    const addItemEventEmitter = <EventEmitter<string>>(modalRef.componentInstance.addedName);
    const subscription = addItemEventEmitter
      .do(value => console.log(value))
      .subscribe(
        value => {
          console.log(value);
          dictionary.items.push(value);
        }
      );
  }

  openNgbdModalResourceAddComponent() {
    const modalRef = this.modalService.open(NgbdModalResourceAddComponent);

    const addItemEventEmitter = <EventEmitter<UserDictionary>>(modalRef.componentInstance.addedDictionary);
    const subscription = addItemEventEmitter
      .do(value => console.log(value))
      .do(value => value.id = (this.UserDictionary.length + 1).toString())
      .subscribe(
        value => {
          console.log(value);
          this.UserDictionary.push(value);
          this.mockUserDictionaryService.addUserDictionaryData(value);
        }
      );

  }

  public ascSort(): void {
    this.UserDictionary.sort((t1, t2) => {
      if (t1.name > t2.name) { return 1; }
      if (t1.name < t2.name) { return -1; }
      return 0; });
  }

  public dscSort(): void {
    this.UserDictionary.sort((t1, t2) => {
      if (t1.name > t2.name) { return -1; }
      if (t1.name < t2.name) { return 1; }
      return 0; });
  }

  public sizeSort(): void {
    this.UserDictionary.sort((t1, t2) => {
      if (t1.items.length > t2.items.length) { return 1; }
      if (t1.items.length < t2.items.length) { return -1; }
      return 0; });
  }

}

// sub component for view-dictionary popup window
@Component({
  selector: 'texera-resource-section-modal',
  template: `
  <div class="modal-header">
    <h4 class="modal-title">{{dictionary.name}}</h4>
    <button type="button" class="close" aria-label="Close" (click)="onClose()">
      <span aria-hidden="true">&times;</span>
    </button>
  </div>

  <div class="modal-body">
    <p>[ {{dictionary.items}},{{name}} ]</p>
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

  public name: string;
  public ifAdd = false;

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
      <input class= "name-area" matInput [(ngModel)]="name" placeholder="Name of Dictionary">
      <textarea class= "content-area" matInput
        [(ngModel)]="dictContent" placeholder="Content of Dictionary"
        matTextareaAutosize matAutosizeMinRows="3">
      </textarea>
      <input class= "separator-area" matInput [(ngModel)]="separator" placeholder="Content Separator">
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
    StubUserDictionaryService
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
    public subMockUserDictionaryService: StubUserDictionaryService
  ) {}

  onChange(event) {
    this.selectFile = event.target.files[0];
  }

  onClose() {
    this.activeModal.close('Close');
  }

  addKey() {

    if (this.selectFile !== null) {
        console.log(this.selectFile);
        this.subMockUserDictionaryService.uploadDictionary(this.selectFile);
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
