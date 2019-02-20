import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

// Sub Component for adding-project popup window
@Component({
  selector: 'texera-add-project-section-modal',
  templateUrl: 'ngbd-modal-add-project.component.html',
  styleUrls: ['./ngbd-modal-add-project.component.scss', '../../../dashboard.component.scss']
})
export class NgbdModalAddProjectComponent {
  @Output() newProject = new EventEmitter<string>();

  public name: string = '';

  constructor(public activeModal: NgbActiveModal) { }

  public onNoClick(): void {
    this.activeModal.close();
  }
  public onClose(): void {
    this.activeModal.close('Close');
  }
  public addProject(): void {
    if (this.name !== '') {
      this.newProject.emit(this.name);
      this.name = '';
    }
    this.onClose();
  }
}
