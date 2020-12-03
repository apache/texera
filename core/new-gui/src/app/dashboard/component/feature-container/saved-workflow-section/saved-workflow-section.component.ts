import { Component, OnInit } from '@angular/core';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { NgbdModalDeleteWorkflowComponent } from './ngbd-modal-delete-workflow/ngbd-modal-delete-workflow.component';

import { cloneDeep } from 'lodash';
import { Observable } from 'rxjs';
import { Workflow } from '../../../../common/type/workflow';
import { Router } from '@angular/router';
import { WorkflowPersistService } from '../../../../common/service/user/workflow-persist/workflow-persist.service';

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
    private workflowPersistService: WorkflowPersistService,
    private modalService: NgbModal,
    private router: Router
  ) {
  }

  ngOnInit() {
    this.workflowPersistService.retrieveWorkflowsBySessionUser().subscribe(
      workflows => this.workflows = workflows,
    );
  }

  /**
   * sort the workflow by name in ascending order
   */
  public ascSort(): void {
    this.workflows.sort((t1, t2) => t1.name.toLowerCase().localeCompare(t2.name.toLowerCase()));
  }

  /**
   * sort the project by name in descending order
   */
  public dscSort(): void {
    this.workflows.sort((t1, t2) => t2.name.toLowerCase().localeCompare(t1.name.toLowerCase()));
  }

  /**
   * sort the project by creating time
   */
  public dateSort(): void {
    this.workflows.sort((left: Workflow, right: Workflow) => left.creationTime - right.creationTime);
  }

  /**
   * sort the project by last modified time
   */
  public lastSort(): void {
    this.workflows.sort((left: Workflow, right: Workflow) => left.lastModifiedTime - right.lastModifiedTime);
  }

  /**
   * openNgbdModalAddWorkflowComponent triggers the add project
   * component. The component returns the information of new project,
   * and this method adds new project in to the list. It calls the
   * saveProject method in service which implements backend API.
   */
  public openNgbdModalAddWorkflowComponent(): void {
    // const modalRef = this.modalService.open(NgbdModalAddWorkflowComponent);
    //
    // Observable.from(modalRef.result)
    //   .subscribe((value: string) => {
    //     console.log('creating a new workflow');
    //     if (value) {
    //       const newProject: Workflow = {
    //         id: (this.workflows.length + 1).toString(),
    //         name: value,
    //         creationTime: Date.now().toString(),
    //         lastModifiedTime: Date.now().toString()
    //       };
    //       this.workflows.push(newProject);
    //     }
    //   });
    alert('this feature is disabled now');
  }

  /**
   * openNgbdModalDeleteWorkflowComponent trigger the delete workflow
   * component. If user confirms the deletion, the method sends
   * message to frontend and delete the workflow on frontend. It
   * calls the deleteProject method in service which implements backend API.
   */
  public openNgbdModalDeleteWorkflowComponent(savedWorkflow: Workflow): void {
    const modalRef = this.modalService.open(NgbdModalDeleteWorkflowComponent);
    modalRef.componentInstance.project = cloneDeep(savedWorkflow);

    Observable.from(modalRef.result).subscribe(
      (value: boolean) => {
        if (value) {
          this.workflows = this.workflows.filter(workflow => workflow.wid !== savedWorkflow.wid);
          this.workflowPersistService.deleteWorkflow(savedWorkflow);
        }
      }
    );

  }

  jumpToWorkflow(workflow: Workflow) {
    this.router.navigate([`/workflow/${workflow.wid}`]).then(null);
  }
}
