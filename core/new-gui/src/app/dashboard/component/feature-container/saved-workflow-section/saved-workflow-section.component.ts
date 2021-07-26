import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { cloneDeep } from 'lodash';
import { Observable } from 'rxjs';
import { WorkflowPersistService } from '../../../../common/service/user/workflow-persist/workflow-persist.service';
import { Workflow } from '../../../../common/type/workflow';
import { NgbdModalDeleteWorkflowComponent } from './ngbd-modal-delete-workflow/ngbd-modal-delete-workflow.component';
import { NgbdModalShareAccessComponent } from './ngbd-modal-share-access/ngbd-modal-share-access.component';
import { UserService } from '../../../../common/service/user/user.service';

export const ROUTER_WORKFLOW_BASE_URL = `/workflow`;
export const ROUTER_WORKFLOW_CREATE_NEW_URL = `${ROUTER_WORKFLOW_BASE_URL}/new`;

@Component({
  selector: 'texera-saved-workflow-section',
  templateUrl: './saved-workflow-section.component.html',
  styleUrls: ['./saved-workflow-section.component.scss', '../../dashboard.component.scss']
})
export class SavedWorkflowSectionComponent implements OnInit {

  public workflows: Workflow[] = [];

  constructor(
    private userService: UserService,
    private workflowPersistService: WorkflowPersistService,
    private modalService: NgbModal,
    private router: Router
  ) {
  }


  ngOnInit() {
    this.registerWorkflowRefresh();
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
    this.router.navigate([`${ROUTER_WORKFLOW_CREATE_NEW_URL}`]).then(null);
  }

  /**
   * duplicate the current workflow. A new record will appear in frontend
   * workflow list and backend database.
   */
  public onClickDuplicateWorkflow(workflowToDuplicate: Workflow): void {
    this.workflowPersistService.createWorkflow(workflowToDuplicate.content, workflowToDuplicate.name + '_copy')
      .subscribe((duplicatedWorkflow: Workflow) => {
        this.workflows.push(duplicatedWorkflow);
      }, err => {
        alert(err.error);
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
          }, err => alert(err.error) // TODO: handle error messages properly.
        );
      }
    });
  }

  /**
   * jump to the target workflow canvas
   */
  public jumpToWorkflow(workflow: Workflow): void {
    this.router.navigate([`${ROUTER_WORKFLOW_BASE_URL}/${workflow.wid}`]).then(null);
  }

  private registerWorkflowRefresh(): void {
    this.userService.userChanged().subscribe(
      () => {
        if (this.userService.isLogin()) {
          this.refreshWorkflows();
        } else {
          this.clearWorkflows();
        }
      }
    );

  }

  private refreshWorkflows(): void {
    this.workflowPersistService.retrieveWorkflowsBySessionUser().subscribe(
      workflows => this.workflows = workflows
    );
  }

  private clearWorkflows(): void {
    this.workflows = [];
  }

}
