import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

// sub component for delete-dictionary popup window
@Component({
  selector: 'texera-resource-section-delete-project-modal',
  templateUrl: './ngbd-modal-delete-project.component.html',
  styleUrls: ['./ngbd-modal-delete-project.component.scss', '../../../dashboard.component.scss']

})
export class NgbdModalDeleteProjectComponent {
  @Input() project: any; // potential issue
  @Output() deleteProject =  new EventEmitter<boolean>();

  constructor(public activeModal: NgbActiveModal) {}

  onClose() {
    this.activeModal.close('Close');
  }

  deleteSavedProject() {
    this.deleteProject.emit(true);
    this.onClose();
  }

}
