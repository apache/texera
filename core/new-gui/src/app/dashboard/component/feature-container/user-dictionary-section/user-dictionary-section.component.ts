import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { MatDialog, MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';

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
    modalRef.componentInstance.dictionary = dictionary;
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
    <p>[ {{dictionary.items}} ]</p>
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
  @Output() addedName =  new EventEmitter<UserDictionary>();

  public name: string;
  public ifAdd = false;

  constructor(public activeModal: NgbActiveModal) {}

  onNoClick(): void {
    this.activeModal.close();
  }
  onClose() {
    this.activeModal.close('Close');
  }
  addKey() {

    if (this.ifAdd && this.name !== undefined) {
      console.log('add ' + this.name + ' into dict ' + this.dictionary.name);
      this.dictionary.items.push(this.name);
      this.addedName.emit(this.dictionary);
      this.name = undefined;
    }
    this.ifAdd = !this.ifAdd;

  }
}
