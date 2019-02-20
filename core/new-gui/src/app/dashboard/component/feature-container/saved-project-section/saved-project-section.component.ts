import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbModal, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { MatDialog, MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';

import { Observable } from 'rxjs/Observable';
import { SavedProject } from '../../../type/saved-project';

import { SavedProjectService } from '../../../service/saved-project/saved-project.service';
import { StubSavedProjectService } from '../../../service/saved-project/stub-saved-project.service';

import { NgbdModalAddProjectComponent} from './ngbd-modal-add-project/ngbd-modal-add-project.component';
import { NgbdModalDeleteProjectComponent } from './ngbd-modal-delete-project/ngbd-modal-delete-project.component';

import { cloneDeep } from 'lodash';


@Component({
  selector: 'texera-saved-project-section',
  templateUrl: './saved-project-section.component.html',
  styleUrls: ['./saved-project-section.component.scss', '../../dashboard.component.scss']
})
export class SavedProjectSectionComponent implements OnInit {

  public projects: SavedProject[] = [];

  defaultWeb: String = 'http://localhost:4200/';

  constructor(
    private savedProjectService: SavedProjectService,
    private modalService: NgbModal
  ) { }

  ngOnInit() {
    this.savedProjectService.getSavedProjectData().subscribe(
      value => this.projects = value,
    );
  }

  public ascSort(): void {
    this.projects.sort((t1, t2) => {
      if (t1.name.toLowerCase() > t2.name.toLowerCase()) { return 1; }
      if (t1.name.toLowerCase() < t2.name.toLowerCase()) { return -1; }
      return 0;
    });
  }

  public dscSort(): void {
    this.projects.sort((t1, t2) => {
      if (t1.name.toLowerCase() > t2.name.toLowerCase()) { return -1; }
      if (t1.name.toLowerCase() < t2.name.toLowerCase()) { return 1; }
      return 0;
    });
  }

  public dateSort(): void {
    this.projects.sort((t1, t2) => {
      if (Date.parse(t1.creationTime) > Date.parse(t2.creationTime)) { return -1; }
      if (Date.parse(t1.creationTime) < Date.parse(t2.creationTime)) { return 1; }
      return 0;
    });
  }

  public lastSort(): void {
    this.projects.sort((t1, t2) => {
      if (Date.parse(t1.lastModifiedTime) > Date.parse(t2.lastModifiedTime)) { return -1; }
      if (Date.parse(t1.lastModifiedTime) < Date.parse(t2.lastModifiedTime)) { return 1; }
      return 0;
    });
  }

  openNgbdModalAddProjectComponent() {
    const modalRef = this.modalService.open(NgbdModalAddProjectComponent);
    const projectEventEmitter = <EventEmitter<string>>(modalRef.componentInstance.newProject);
    const subscription = projectEventEmitter
      .do(value => console.log(value))
      .map(value => ({
        id: (this.projects.length + 1).toString(),
        name: value,
        creationTime: Date.now().toString(),
        lastModifiedTime: Date.now().toString()
      }))
      .subscribe(
        value => {
          this.projects.push(value);
        }
      );
  }

  openNgbdModalDeleteProjectComponent(project: SavedProject) {
    const modalRef = this.modalService.open(NgbdModalDeleteProjectComponent);
    modalRef.componentInstance.project = cloneDeep(project);

    const deleteItemEventEmitter = <EventEmitter<boolean>>(modalRef.componentInstance.deleteProject);
    const subscription = deleteItemEventEmitter
      .subscribe(
        (value: any) => {
          if (value) {
            this.projects = this.projects.filter(obj => obj.id !== project.id);
            this.savedProjectService.deleteSavedProjectData(project);
          }
        }
      );

  }
}
