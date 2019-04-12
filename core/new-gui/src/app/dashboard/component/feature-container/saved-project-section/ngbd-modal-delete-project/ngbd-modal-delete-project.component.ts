import { Component, Input, Output, EventEmitter } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { SavedProject } from '../../../../type/saved-project';

// sub component for delete-dictionary popup window
@Component({
  selector: 'texera-resource-section-delete-project-modal',
  templateUrl: './ngbd-modal-delete-project.component.html',
  styleUrls: ['./ngbd-modal-delete-project.component.scss', '../../../dashboard.component.scss']

})
export class NgbdModalDeleteProjectComponent {
  @Input() project: object = {};
  @Output() deleteProject =  new EventEmitter<boolean>();

  constructor(public activeModal: NgbActiveModal) {}

  public onClose(): void {
    this.activeModal.close('Close');
  }

  public deleteSavedProject(): void {
    this.deleteProject.emit(true);
    this.onClose();
  }

}
