import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { MatDialog, MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';

import { Observable } from 'rxjs/Observable';
import { SavedProject } from '../../../type/saved-project';

import { SavedProjectService } from '../../../service/saved-project/saved-project.service';
import { StubSavedProjectService } from '../../../service/saved-project/stub-saved-project.service';

@Component({
  selector: 'texera-saved-project-section',
  templateUrl: './saved-project-section.component.html',
  styleUrls: ['./saved-project-section.component.scss', '../../dashboard.component.scss']
})
export class SavedProjectSectionComponent implements OnInit {

  public projects: SavedProject[] = [];

  constructor(
    private mockSavedProjectService: StubSavedProjectService,
    private modalService: NgbModal
  ) { }

  ngOnInit() {
    this.mockSavedProjectService.getSavedProjectData().subscribe(
      value => this.projects = value,
    );
    console.log(this.projects);
  }

  public ascSort(): void {
    this.projects.sort((t1, t2) => {
      if (t1.name > t2.name) { return 1; }
      if (t1.name < t2.name) { return -1; }
      return 0; });
  }

  public dscSort(): void {
    this.projects.sort((t1, t2) => {
      if (t1.name > t2.name) { return -1; }
      if (t1.name < t2.name) { return 1; }
      return 0; });
  }

  public dateSort(): void {
    this.projects.sort((t1, t2) => {
      if (Date.parse(t1.creationTime) > Date.parse(t2.creationTime)) { return -1; }
      if (Date.parse(t1.creationTime) < Date.parse(t2.creationTime)) { return 1; }
      return 0; });
  }

  public lastSort(): void {
    this.projects.sort((t1, t2) => {
        if (Date.parse(t1.lastModifiedTime) > Date.parse(t2.lastModifiedTime)) { return -1; }
        if (Date.parse(t1.lastModifiedTime) < Date.parse(t2.lastModifiedTime)) { return 1; }
        return 0; });
  }

  openNgbdModalAddProjectComponent() {
    const modalRef = this.modalService.open(NgbdModalAddProjectComponent);
    const projectEventEmitter = <EventEmitter<string>>(modalRef.componentInstance.newProject);
    const subscription = projectEventEmitter
      .do(value => console.log(value))
      .map(value => {return {
        id: (this.projects.length + 1).toString(),
        name: value,
        creationTime: Date.now().toString(),
        lastModifiedTime: Date.now().toString()
      }; })
      .subscribe(
        value => {
          console.log(value);
          this.projects.push(value);
        }
      );
  }
}


// Sub Component for adding-project popup window
@Component({
  selector: 'texera-add-project-section-modal',
  template: `
  <div class="modal-header">
  <h4 class="modal-title">Add New Project</h4>
  <button type="button" class="close" aria-label="Close" (click)="onClose()">
    <span aria-hidden="true">&times;</span>
  </button>
</div>
<div class="modal-body">

      <mat-dialog-content>
          <input matInput [(ngModel)]="name" placeholder="Name of New Project">
      </mat-dialog-content>


</div>
<div class="modal-footer">
  <button type="button" class="btn btn-outline-dark add-button" (click)="addProject()">Add</button>
  <button type="button" class="btn btn-outline-dark" (click)="onClose()">Close</button>
</div>
  `,
  styleUrls: ['./saved-project-section.component.scss', '../../dashboard.component.scss']
})
export class NgbdModalAddProjectComponent {
  @Output() newProject =  new EventEmitter<string>();

  public name: string;

  constructor(public activeModal: NgbActiveModal) {}

  onNoClick(): void {
    this.activeModal.close();
  }
  onClose() {
    this.activeModal.close('Close');
  }
  addProject() {
      if (this.name !== undefined) {
          this.newProject.emit(this.name);
          this.name = undefined;
        }
      this.onClose();
    }
}
