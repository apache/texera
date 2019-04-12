import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { UserDictionary } from '../../../../type/user-dictionary';

/**
 * NgbdModalResourceDeleteComponent is the pop-up
 * component for undoing the delete. User may cancel
 * a dictionary deletion.
 *
 * @author Zhaomin Li
 */
@Component({
  selector: 'texera-resource-section-delete-dict-modal',
  templateUrl: './ngbd-modal-resource-delete.component.html',
  styleUrls: ['./ngbd-modal-resource-delete.component.scss', '../../../dashboard.component.scss']

})
export class NgbdModalResourceDeleteComponent {
  @Input() dictionary: any;
  @Output() deleteDict =  new EventEmitter<boolean>();

  constructor(public activeModal: NgbActiveModal) {
  }

  public onClose(): void {
    this.activeModal.close('Close');
  }

  /**
  * deleteDictionary sends the user confirm to the frontend to delete
  * a certain dictionary in user storage.
  *
  * @param
  */
  public deleteDictionary(): void {
    this.deleteDict.emit(true);
    this.onClose();
  }

}
