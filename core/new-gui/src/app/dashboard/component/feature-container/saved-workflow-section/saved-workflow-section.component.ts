import {Component, OnInit} from '@angular/core';
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';

import {SavedWorkflowService} from '../../../service/saved-project/saved-workflow.service';

import {NgbdModalAddWorkflowComponent} from './ngbd-modal-add-workflow/ngbd-modal-add-workflow.component';
import {NgbdModalDeleteWorkflowComponent} from './ngbd-modal-delete-workflow/ngbd-modal-delete-workflow.component';

import {cloneDeep} from 'lodash';
import {Observable} from 'rxjs';
import {Workflow} from '../../../../common/type/workflow';
import {Router} from '@angular/router';

/**
 * SavedProjectSectionComponent is the main interface for
 * managing all the personal projects. On this interface,
 * user can view the project list by the order he/she defines,
 * add project into list, delete project, and access the projects.
 *
 * @author Zhaomin Li
 */
@Component({
  selector: 'texera-saved-workflow-section',
  templateUrl: './saved-workflow-section.component.html',
  styleUrls: ['./saved-workflow-section.component.scss', '../../dashboard.component.scss']
})
export class SavedWorkflowSectionComponent implements OnInit {

  public workflows: Workflow[] = [];

  public defaultWeb: String = 'http://localhost:4200/';

  constructor(
    private savedProjectService: SavedWorkflowService,
    private modalService: NgbModal,
    private router: Router
  ) {
  }

  ngOnInit() {
    this.savedProjectService.getSavedWorkflows().subscribe(
      value => this.workflows = value,
    );
  }

  /**
   * sort the workflow by name in ascending order
   *
   * @param
   */
  public ascSort(): void {
    this.workflows.sort((t1, t2) => {
      if (t1.name.toLowerCase() > t2.name.toLowerCase()) {
        return 1;
      }
      if (t1.name.toLowerCase() < t2.name.toLowerCase()) {
        return -1;
      }
      return 0;
    });
  }

  /**
   * sort the project by name in descending order
   *
   * @param
   */
  public dscSort(): void {
    this.workflows.sort((t1, t2) => {
      if (t1.name.toLowerCase() > t2.name.toLowerCase()) {
        return -1;
      }
      if (t1.name.toLowerCase() < t2.name.toLowerCase()) {
        return 1;
      }
      return 0;
    });
  }

  /**
   * sort the project by creating time
   *
   * @param
   */
  public dateSort(): void {
    this.workflows.sort((t1, t2) => {
      if (Date.parse(t1.creationTime) > Date.parse(t2.creationTime)) {
        return -1;
      }
      if (Date.parse(t1.creationTime) < Date.parse(t2.creationTime)) {
        return 1;
      }
      return 0;
    });
  }

  /**
   * sort the project by last edited time
   *
   * @param
   */
  public lastSort(): void {
    this.workflows.sort((t1, t2) => {
      if (Date.parse(t1.lastModifiedTime) > Date.parse(t2.lastModifiedTime)) {
        return -1;
      }
      if (Date.parse(t1.lastModifiedTime) < Date.parse(t2.lastModifiedTime)) {
        return 1;
      }
      return 0;
    });
  }

  /**
   * openNgbdModalAddWorkflowComponent triggers the add project
   * component. The component returns the information of new project,
   * and this method adds new project in to the list. It calls the
   * saveProject method in service which implements backend API.
   *
   * @param
   */
  public openNgbdModalAddWorkflowComponent(): void {
    const modalRef = this.modalService.open(NgbdModalAddWorkflowComponent);

    Observable.from(modalRef.result)
      .subscribe((value: string) => {
        console.log('creating a new workflow');
        // if (value) {
        //   const newProject: SavedWorkflow = {
        //     id: (this.workflows.length + 1).toString(),
        //     name: value,
        //     creationTime: Date.now().toString(),
        //     lastModifiedTime: Date.now().toString()
        //   };
        //   this.workflows.push(newProject);
        // }
      });
  }

  /**
   * openNgbdModalDeleteWorkflowComponent trigger the delete workflow
   * component. If user confirms the deletion, the method sends
   * message to frontend and delete the workflow on frontend. It
   * calls the deleteProject method in service which implements backend API.
   *
   * @param
   */
  public openNgbdModalDeleteWorkflowComponent(savedWorkflow: Workflow): void {
    const modalRef = this.modalService.open(NgbdModalDeleteWorkflowComponent);
    modalRef.componentInstance.project = cloneDeep(savedWorkflow);

    Observable.from(modalRef.result).subscribe(
      (value: boolean) => {
        if (value) {
          this.workflows = this.workflows.filter(workflow => workflow.wfId !== savedWorkflow.wfId);
          this.savedProjectService.deleteSavedProjectData(savedWorkflow);
        }
      }
    );

  }

  jumpToWorkflow(workflow: Workflow) {
    localStorage.setItem('workflow', workflow.content);
    localStorage.setItem('wfId', workflow.wfId.toString());
    this.router.navigate(['/']);
  }
}
