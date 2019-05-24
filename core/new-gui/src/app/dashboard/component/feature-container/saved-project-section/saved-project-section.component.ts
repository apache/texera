import { Component, OnInit } from '@angular/core';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';

import { SavedProject } from '../../../type/saved-project';
import { SavedProjectService } from '../../../service/saved-project/saved-project.service';

import { NgbdModalAddProjectComponent } from './ngbd-modal-add-project/ngbd-modal-add-project.component';
import { NgbdModalDeleteProjectComponent } from './ngbd-modal-delete-project/ngbd-modal-delete-project.component';

import { cloneDeep } from 'lodash';
import { Observable } from 'rxjs';

/**
 * SavedProjectSectionComponent is the main interface for
 * managing all the personal projects. On this interface,
 * user can view the project list by the order he/she defines,
 * add project into list, delete project, and access the projects.
 *
 * @author Zhaomin Li
 */
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

  /**
  * sort the project by name in ascending order
  *
  * @param
  */
  public ascSort(): void {
    this.projects.sort((t1, t2) => {
      if (t1.name.toLowerCase() > t2.name.toLowerCase()) { return 1; }
      if (t1.name.toLowerCase() < t2.name.toLowerCase()) { return -1; }
      return 0;
    });
  }

  /**
  * sort the project by name in descending order
  *
  * @param
  */
  public dscSort(): void {
    this.projects.sort((t1, t2) => {
      if (t1.name.toLowerCase() > t2.name.toLowerCase()) { return -1; }
      if (t1.name.toLowerCase() < t2.name.toLowerCase()) { return 1; }
      return 0;
    });
  }

  /**
  * sort the project by creating time
  *
  * @param
  */
  public dateSort(): void {
    this.projects.sort((t1, t2) => {
      if (Date.parse(t1.creationTime) > Date.parse(t2.creationTime)) { return -1; }
      if (Date.parse(t1.creationTime) < Date.parse(t2.creationTime)) { return 1; }
      return 0;
    });
  }

  /**
  * sort the project by last edited time
  *
  * @param
  */
  public lastSort(): void {
    this.projects.sort((t1, t2) => {
      if (Date.parse(t1.lastModifiedTime) > Date.parse(t2.lastModifiedTime)) { return -1; }
      if (Date.parse(t1.lastModifiedTime) < Date.parse(t2.lastModifiedTime)) { return 1; }
      return 0;
    });
  }

  /**
  * openNgbdModalAddProjectComponent triggers the add project
  * component. The component returns the information of new project,
  * and this method adds new project in to the list. It calls the
  * saveProject method in service which implements backend API.
  *
  * @param
  */
  public openNgbdModalAddProjectComponent(): void {
    const modalRef = this.modalService.open(NgbdModalAddProjectComponent);

    Observable.from(modalRef.result)
      .subscribe((value: string) => {
        if (value) {
          const newProject: SavedProject = {
            id: (this.projects.length + 1).toString(),
            name: value,
            creationTime: Date.now().toString(),
            lastModifiedTime: Date.now().toString()
          };
          this.projects.push(newProject);
        }
      });
  }

  /**
  * openNgbdModalDeleteProjectComponent trigger the delete project
  * component. If user confirms the deletion, the method sends
  * message to frontend and delete the project on frontend. It
  * calls the deleteProject method in service which implements backend API.
  *
  * @param
  */
  public openNgbdModalDeleteProjectComponent(project: SavedProject): void {
    const modalRef = this.modalService.open(NgbdModalDeleteProjectComponent);
    modalRef.componentInstance.project = cloneDeep(project);

    Observable.from(modalRef.result).subscribe(
      (value: boolean) => {
        if (value) {
          this.projects = this.projects.filter(obj => obj.id !== project.id);
          this.savedProjectService.deleteSavedProjectData(project);
        }
      }
    );

  }
}
