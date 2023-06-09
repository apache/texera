import { Component, EventEmitter, Input, OnInit, Output } from "@angular/core";
import { environment } from "src/environments/environment";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { NgbdModalWorkflowExecutionsComponent } from "../ngbd-modal-workflow-executions/ngbd-modal-workflow-executions.component";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "src/app/common/service/workflow-persist/workflow-persist.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ShareAccessComponent } from "../../share-access/share-access.component";
import { Workflow } from "src/app/common/type/workflow";
import { FileSaverService } from "../../../service/user-file/file-saver.service";
import { UserProject } from "../../../type/user-project";
import { UserProjectService } from "../../../service/user-project/user-project.service";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { firstValueFrom } from "rxjs";

@UntilDestroy()
@Component({
  selector: "texera-user-workflow-list-item",
  templateUrl: "./user-workflow-list-item.component.html",
  styleUrls: ["./user-workflow-list-item.component.scss"],
})
export class UserWorkflowListItemComponent {
  ROUTER_WORKFLOW_BASE_URL = "/workflow";
  ROUTER_USER_PROJECT_BASE_URL = "/dashboard/user-project";
  private _entry?: DashboardEntry;

  @Input()
  get entry(): DashboardEntry {
    if (!this._entry) {
      throw new Error("entry property must be provided to UserWorkflowListItemComponent.");
    }
    return this._entry;
  }

  set entry(value: DashboardEntry) {
    this._entry = value;
  }

  get workflow(): Workflow {
    if (!this.entry.workflow) {
      throw new Error(
        "Incorrect type of DashboardEntry provided to UserWorkflowListItemComponent. Entry must be workflow."
      );
    }
    return this.entry.workflow.workflow;
  }

  @Input() editable = false;
  @Input() public pid: number = 0;
  userProjectsMap: ReadonlyMap<number, UserProject> = new Map();
  @Output() checkedChange: EventEmitter<boolean> = new EventEmitter<boolean>();
  @Output() deleted = new EventEmitter<void>();
  @Output() duplicated = new EventEmitter<void>();

  editingName = false;
  editingDescription = false;
  /** Whether tracking metadata information about executions is enabled. */
  workflowExecutionsTrackingEnabled: boolean = environment.workflowExecutionsTrackingEnabled;

  constructor(
    private modalService: NgbModal,
    private workflowPersistService: WorkflowPersistService,
    private fileSaverService: FileSaverService,
    private userProjectService: UserProjectService
  ) {
    this.userProjectService
      .retrieveProjectList()
      .pipe(untilDestroyed(this))
      .subscribe(userProjectsList => {
        this.userProjectsMap = new Map(userProjectsList.map(userProject => [userProject.pid, userProject]));
      });
  }

  /**
   * open the workflow executions page
   */
  public onClickGetWorkflowExecutions(): void {
    const modalRef = this.modalService.open(NgbdModalWorkflowExecutionsComponent, {
      size: "xl",
      modalDialogClass: "modal-dialog-centered",
    });
    modalRef.componentInstance.workflow = this.workflow;
    modalRef.componentInstance.workflowName = this.workflow.name;
  }

  public confirmUpdateWorkflowCustomName(name: string): void {
    this.workflowPersistService
      .updateWorkflowName(this.workflow.wid, name || DEFAULT_WORKFLOW_NAME)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.workflow.name = name || DEFAULT_WORKFLOW_NAME;
      })
      .add(() => {
        this.editingName = false;
      });
  }

  public confirmUpdateWorkflowCustomDescription(description: string): void {
    this.workflowPersistService
      .updateWorkflowDescription(this.workflow.wid, description)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.workflow.description = description;
      })
      .add(() => {
        this.editingDescription = false;
      });
  }

  /**
   * open the Modal based on the workflow clicked on
   */
  public async onClickOpenShareAccess(): Promise<void> {
    const owners = await firstValueFrom(this.workflowPersistService.retrieveOwners());
    const modalRef = this.modalService.open(ShareAccessComponent);
    modalRef.componentInstance.type = "workflow";
    modalRef.componentInstance.id = this.workflow.wid;
    modalRef.componentInstance.allOwners = owners;
  }

  /**
   * Download the workflow as a json file
   */
  public onClickDownloadWorkfllow(): void {
    if (this.workflow.wid) {
      this.workflowPersistService
        .retrieveWorkflow(this.workflow.wid)
        .pipe(untilDestroyed(this))
        .subscribe(data => {
          const workflowCopy: Workflow = {
            ...data,
            wid: undefined,
            creationTime: undefined,
            lastModifiedTime: undefined,
          };
          const workflowJson = JSON.stringify(workflowCopy.content);
          const fileName = workflowCopy.name + ".json";
          this.fileSaverService.saveAs(new Blob([workflowJson], { type: "text/plain;charset=utf-8" }), fileName);
        });
    }
  }

  public isLightColor(color: string): boolean {
    return UserProjectService.isLightColor(color);
  }

  /**
   * For color tags, enable clicking 'x' to remove a workflow from a project
   */
  public removeWorkflowFromProject(pid: number): void {
    this.userProjectService
      .removeWorkflowFromProject(pid, this.workflow.wid!)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.entry.workflow.projectIDs = this.entry.workflow.projectIDs.filter(projectID => projectID != pid);
      });
  }
}
