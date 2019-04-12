import { Component, OnInit, EventEmitter } from '@angular/core';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';

import { SavedProject } from '../../../type/saved-project';
import { SavedProjectService } from '../../../service/saved-project/saved-project.service';

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

  public defaultWeb: String = 'http://localhost:4200/';

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

  public openNgbdModalAddProjectComponent(): void {
    const modalRef = this.modalService.open(NgbdModalAddProjectComponent);
    modalRef.componentInstance.newProject
      .map((value: boolean) => ({
        id: (this.projects.length + 1).toString(),
        name: value,
        creationTime: Date.now().toString(),
        lastModifiedTime: Date.now().toString()
      }))
      .subscribe((value: SavedProject) => this.projects.push(value));
  }

  public openNgbdModalDeleteProjectComponent(project: SavedProject): void {
    const modalRef = this.modalService.open(NgbdModalDeleteProjectComponent);
    modalRef.componentInstance.project = cloneDeep(project);

    modalRef.componentInstance.deleteProject.subscribe(
      (value: boolean) => {
        if (value) {
          this.projects = this.projects.filter(obj => obj.id !== project.id);
          this.savedProjectService.deleteSavedProjectData(project);
        }
      }
    );

  }
}
