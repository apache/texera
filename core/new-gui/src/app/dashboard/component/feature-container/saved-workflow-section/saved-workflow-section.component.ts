import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { cloneDeep } from 'lodash';
import { Observable } from 'rxjs';
import { WorkflowPersistService } from '../../../../common/service/user/workflow-persist/workflow-persist.service';
import { WorkflowGrantAccessService } from '../../../../common/service/user/workflow-access-control/workflow-grant-access.service';
import { Workflow } from '../../../../common/type/workflow';
import { NgbdModalDeleteWorkflowComponent } from './ngbd-modal-delete-workflow/ngbd-modal-delete-workflow.component';
import { NgbdModalShareAccessComponent } from './ngbd-modal-share-access/ngbd-modal-share-access.component';

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
    private workflowGrantAccessService: WorkflowGrantAccessService,
    private modalService: NgbModal,
    private router: Router
  ) {
  }


  ngOnInit() {
    this.workflowPersistService.retrieveWorkflowsBySessionUser().subscribe(
      workflows => this.workflows = workflows
    );
  }

  /**
   * open the Modal based on the workflow clicked on
   */
  public onClickOpenShareAccess(workflow: Workflow): void {
    const modalRef = this.modalService.open(NgbdModalShareAccessComponent);
    modalRef.componentInstance.workflow = workflow;
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
    this.workflows.sort((left: Workflow, right: Workflow) =>
      left.creationTime !== undefined && right.creationTime !== undefined ? left.creationTime - right.creationTime : 0);
  }

  /**
   * sort the project by last modified time
   */
  public lastSort(): void {
    this.workflows.sort((left: Workflow, right: Workflow) =>
      left.lastModifiedTime !== undefined && right.lastModifiedTime !== undefined ? left.lastModifiedTime - right.lastModifiedTime : 0);
  }

  /**
   * create a new workflow. will redirect to a pre-emptied workspace
   */
  public onClickCreateNewWorkflowFromDashboard(): void {
    this.router.navigate(['/workflow/new']).then(null);
  }

  /**
   * duplicate the current workflow. A new record will appear in frontend
   * workflow list and backend database.
   */
  public onClickDuplicateWorkflow(workflowToDuplicate: Workflow): void {
    this.workflowPersistService.createWorkflow(workflowToDuplicate.content, workflowToDuplicate.name + '_copy')
      .subscribe((duplicatedWorkflow: Workflow) => {
        this.workflows.push(duplicatedWorkflow);
      }, error => {
        alert(error);
      });
  }

  /**
   * openNgbdModalDeleteWorkflowComponent trigger the delete workflow
   * component. If user confirms the deletion, the method sends
   * message to frontend and delete the workflow on frontend. It
   * calls the deleteProject method in service which implements backend API.
   */
  public openNgbdModalDeleteWorkflowComponent(workflowToDelete: Workflow): void {
    const modalRef = this.modalService.open(NgbdModalDeleteWorkflowComponent);
    modalRef.componentInstance.workflow = cloneDeep(workflowToDelete);

    Observable.from(modalRef.result).subscribe((confirmToDelete: boolean) => {
      if (confirmToDelete && workflowToDelete.wid !== undefined) {
        this.workflows = this.workflows.filter(workflow => workflow.wid !== workflowToDelete.wid);
        this.workflowPersistService.deleteWorkflow(workflowToDelete.wid).subscribe(_ => {
          }, alert // TODO: handle error messages properly.
        );
      }
    });
  }

  /**
   * jump to the target workflow canvas
   */
  jumpToWorkflow(workflow: Workflow) {
    this.router.navigate([`/workflow/${workflow.wid}`]).then(null);
  }


}
