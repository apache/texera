import { Component, OnInit } from "@angular/core";
import { Router } from "@angular/router";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { cloneDeep } from "lodash-es";
import { from } from "rxjs";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { NgbdModalDeleteWorkflowComponent } from "./ngbd-modal-delete-workflow/ngbd-modal-delete-workflow.component";
import { NgbdModalWorkflowShareAccessComponent } from "./ngbd-modal-share-access/ngbd-modal-workflow-share-access.component";
import { DashboardWorkflowEntry } from "../../../type/dashboard-workflow-entry";
import { UserService } from "../../../../common/service/user/user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "src/app/common/service/notification/notification.service";

export const ROUTER_WORKFLOW_BASE_URL = "/workflow";
export const ROUTER_WORKFLOW_CREATE_NEW_URL = "/";

@UntilDestroy()
@Component({
  selector: "texera-saved-workflow-section",
  templateUrl: "./saved-workflow-section.component.html",
  styleUrls: ["./saved-workflow-section.component.scss", "../../dashboard.component.scss"],
})
export class SavedWorkflowSectionComponent implements OnInit {
  public dashboardWorkflowEntries: DashboardWorkflowEntry[] = [];
  public dashboardWorkflowEntriesIsEditingName: number[] = [];
  private defaultWorkflowName: string = "Untitled Workflow";

  constructor(
    private userService: UserService,
    private workflowPersistService: WorkflowPersistService,
    private notificationService: NotificationService,
    private modalService: NgbModal,
    private router: Router
  ) {}

  ngOnInit() {
    this.registerDashboardWorkflowEntriesRefresh();
  }

  /**
   * open the Modal based on the workflow clicked on
   */
  public onClickOpenShareAccess({ workflow }: DashboardWorkflowEntry): void {
    const modalRef = this.modalService.open(NgbdModalWorkflowShareAccessComponent);
    modalRef.componentInstance.workflow = workflow;
  }

  /**
   * sort the workflow by name in ascending order
   */
  public ascSort(): void {
    this.dashboardWorkflowEntries.sort((t1, t2) =>
      t1.workflow.name.toLowerCase().localeCompare(t2.workflow.name.toLowerCase())
    );
  }

  /**
   * sort the project by name in descending order
   */
  public dscSort(): void {
    this.dashboardWorkflowEntries.sort((t1, t2) =>
      t2.workflow.name.toLowerCase().localeCompare(t1.workflow.name.toLowerCase())
    );
  }

  /**
   * sort the project by creating time
   */
  public dateSort(): void {
    this.dashboardWorkflowEntries.sort((left: DashboardWorkflowEntry, right: DashboardWorkflowEntry) =>
      left.workflow.creationTime !== undefined && right.workflow.creationTime !== undefined
        ? left.workflow.creationTime - right.workflow.creationTime
        : 0
    );
  }

  /**
   * sort the project by last modified time
   */
  public lastSort(): void {
    this.dashboardWorkflowEntries.sort((left: DashboardWorkflowEntry, right: DashboardWorkflowEntry) =>
      left.workflow.lastModifiedTime !== undefined && right.workflow.lastModifiedTime !== undefined
        ? left.workflow.lastModifiedTime - right.workflow.lastModifiedTime
        : 0
    );
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
  public onClickDuplicateWorkflow({ workflow: { wid } }: DashboardWorkflowEntry): void {
    if (wid) {
      this.workflowPersistService
        .duplicateWorkflow(wid)
        .pipe(untilDestroyed(this))
        .subscribe(
          (duplicatedWorkflowInfo: DashboardWorkflowEntry) => {
            this.dashboardWorkflowEntries.push(duplicatedWorkflowInfo);
          },
          // @ts-ignore // TODO: fix this with notification component
          (err: unknown) => alert(err.error)
        );
    }
  }

  /**
   * openNgbdModalDeleteWorkflowComponent trigger the delete workflow
   * component. If user confirms the deletion, the method sends
   * message to frontend and delete the workflow on frontend. It
   * calls the deleteProject method in service which implements backend API.
   */
  public openNgbdModalDeleteWorkflowComponent({ workflow }: DashboardWorkflowEntry): void {
    const modalRef = this.modalService.open(NgbdModalDeleteWorkflowComponent);
    modalRef.componentInstance.workflow = cloneDeep(workflow);

    from(modalRef.result)
      .pipe(untilDestroyed(this))
      .subscribe((confirmToDelete: boolean) => {
        const wid = workflow.wid;
        if (confirmToDelete && wid !== undefined) {
          this.workflowPersistService
            .deleteWorkflow(wid)
            .pipe(untilDestroyed(this))
            .subscribe(
              _ => {
                this.dashboardWorkflowEntries = this.dashboardWorkflowEntries.filter(
                  workflowEntry => workflowEntry.workflow.wid !== wid
                );
              },
              // @ts-ignore // TODO: fix this with notification component
              (err: unknown) => alert(err.error)
            );
        }
      });
  }

  /**
   * jump to the target workflow canvas
   */
  public jumpToWorkflow({ workflow: { wid } }: DashboardWorkflowEntry): void {
    this.router.navigate([`${ROUTER_WORKFLOW_BASE_URL}/${wid}`]).then(null);
  }

  private registerDashboardWorkflowEntriesRefresh(): void {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.userService.isLogin()) {
          this.refreshDashboardWorkflowEntries();
        } else {
          this.clearDashboardWorkflowEntries();
        }
      });
  }

  private refreshDashboardWorkflowEntries(): void {
    this.workflowPersistService
      .retrieveWorkflowsBySessionUser()
      .pipe(untilDestroyed(this))
      .subscribe(dashboardWorkflowEntries => (this.dashboardWorkflowEntries = dashboardWorkflowEntries));
  }

  private clearDashboardWorkflowEntries(): void {
    this.dashboardWorkflowEntries = [];
  }

  public confirmUpdateWorkflowCustomName(
    dashboardWorkflowEntry: DashboardWorkflowEntry,
    name: string,
    index: number
  ): void {
    const { workflow } = dashboardWorkflowEntry;
    this.workflowPersistService
      .updateWorkflowName(workflow.wid, name || this.defaultWorkflowName)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        let updatedDashboardWorkFlowEntry = { ...dashboardWorkflowEntry };
        updatedDashboardWorkFlowEntry.workflow = { ...workflow };
        updatedDashboardWorkFlowEntry.workflow.name = name || this.defaultWorkflowName;

        this.dashboardWorkflowEntries[index] = updatedDashboardWorkFlowEntry;
      })
      .add(() => {
        this.dashboardWorkflowEntriesIsEditingName = this.dashboardWorkflowEntriesIsEditingName.filter(
          entryIsEditingIndex => entryIsEditingIndex != index
        );
      });
  }
}
