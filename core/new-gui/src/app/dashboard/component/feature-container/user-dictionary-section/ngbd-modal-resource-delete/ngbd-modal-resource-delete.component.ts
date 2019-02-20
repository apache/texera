import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';


// sub component for delete-dictionary popup window
@Component({
  selector: 'texera-resource-section-delete-dict-modal',
  templateUrl: './ngbd-modal-resource-delete.component.html',
  styleUrls: ['./ngbd-modal-resource-delete.component.scss', '../../../dashboard.component.scss']

})
export class NgbdModalResourceDeleteComponent {
  @Input() dictionary: any;
  @Output() deleteDict =  new EventEmitter<boolean>();

  constructor(public activeModal: NgbActiveModal) {}

  onClose() {
    this.activeModal.close('Close');
  }

  deleteDictionary() {
    this.deleteDict.emit(true);
    this.onClose();
  }

}
