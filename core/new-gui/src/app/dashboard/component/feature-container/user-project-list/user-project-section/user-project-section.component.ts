import { Component, OnInit } from "@angular/core";
import { UserProjectService } from "../../../../service/user-project/user-project.service";
import { ActivatedRoute } from "@angular/router";
import { Router } from "@angular/router";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { NgbdModalAddProjectWorkflowComponent } from "./ngbd-modal-add-project-workflow/ngbd-modal-add-project-workflow.component";
import { NgbdModalRemoveProjectWorkflowComponent } from "./ngbd-modal-remove-project-workflow/ngbd-modal-remove-project-workflow.component";
import { NgbdModalAddProjectFileComponent } from "./ngbd-modal-add-project-file/ngbd-modal-add-project-file.component";
import { NgbdModalRemoveProjectFileComponent } from "./ngbd-modal-remove-project-file/ngbd-modal-remove-project-file.component";
import { DashboardWorkflowEntry } from "../../../../type/dashboard-workflow-entry";
import { DashboardUserFileEntry } from "../../../../type/dashboard-user-file-entry";
import { concatMap, catchError } from "rxjs/operators";

// ---- for workflow card
import { WorkflowPersistService } from "../../../../../common/service/workflow-persist/workflow-persist.service";
import { NgbdModalDeleteWorkflowComponent } from "../../saved-workflow-section/ngbd-modal-delete-workflow/ngbd-modal-delete-workflow.component";
import { NgbdModalWorkflowShareAccessComponent } from "../../saved-workflow-section/ngbd-modal-share-access/ngbd-modal-workflow-share-access.component";
import { cloneDeep } from "lodash-es";
import { from } from "rxjs";
import { NotificationService } from "../../../../../common/service/notification/notification.service";

// ---- for file card
import { UserFileService } from "../../../../service/user-file/user-file.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgbdModalUserFileShareAccessComponent } from "../../user-file-section/ngbd-modal-file-share-access/ngbd-modal-user-file-share-access.component";

export const ROUTER_WORKFLOW_BASE_URL = "/workflow";

@UntilDestroy()
@Component({
  selector: "texera-user-project-section",
  templateUrl: "./user-project-section.component.html",
  styleUrls: ["./user-project-section.component.scss"]
})
export class UserProjectSectionComponent implements OnInit {
  private pid: number = 0;
  public name: string = "";
  public ownerID: number = 0;
  public creationTime: number = 0;
  public workflows: DashboardWorkflowEntry[] = [];

  // ----- for workflow card
  public isEditingWorkflowName: number[] = [];
  private defaultWorkflowName: string = "Untitled Workflow";

  // ----- for file card
  public isEditingFileName: number[] = [];

  constructor(
    private userProjectService: UserProjectService,
    private route: ActivatedRoute,
    private router: Router,
    private modalService: NgbModal,
    private workflowPersistService: WorkflowPersistService,
    private userFileService: UserFileService,
    private notificationService: NotificationService
    ) { }

  ngOnInit(): void {
    // extract passed PID from parameter
    if (this.route.snapshot.params.pid) {
      this.pid = this.route.snapshot.params.pid;

      this.getUserProjectMetadata();
      this.getWorkflowsOfProject();
      this.userProjectService.refreshFilesOfProject(this.pid);
    }

    // otherwise no project ID, no project to load
  }

  public onClickOpenAddWorkflow() {
    const modalRef = this.modalService.open(NgbdModalAddProjectWorkflowComponent);
    modalRef.componentInstance.addedWorkflows = this.workflows;
    modalRef.componentInstance.projectId = this.pid;

    // retrieve updated values from modal via promise
    modalRef.result.then((result) => {
      if (result) {
        this.workflows = result;
      }
    });
  }

  public onClickOpenRemoveWorkflow() {
    const modalRef = this.modalService.open(NgbdModalRemoveProjectWorkflowComponent);
    modalRef.componentInstance.addedWorkflows = this.workflows;
    modalRef.componentInstance.projectId = this.pid;

    // retrieve updated values from modal via promise
    modalRef.result.then((result) => {
      if (result) {
        this.workflows = result;
      }
    });
  }

  public onClickOpenAddFile() {
    const modalRef = this.modalService.open(NgbdModalAddProjectFileComponent);
    modalRef.componentInstance.addedFiles = this.getUserProjectFilesArray();
    modalRef.componentInstance.projectId = this.pid;
  }

  public onClickOpenRemoveFile() {
    const modalRef = this.modalService.open(NgbdModalRemoveProjectFileComponent);
    modalRef.componentInstance.addedFiles = this.getUserProjectFilesArray();
    modalRef.componentInstance.projectId = this.pid;
  }

  private getUserProjectMetadata() {
    this.userProjectService
      .retrieveProject(this.pid)
      .pipe(untilDestroyed(this))
      .subscribe(project => {
        this.name = project.name;
        this.ownerID = project.ownerID;
        this.creationTime = project.creationTime;
      });
  }

  private getWorkflowsOfProject() {
    this.userProjectService
      .retrieveWorkflowsOfProject(this.pid)
      .pipe(untilDestroyed(this))
      .subscribe(workflows => {
        this.workflows = workflows;
      });
  }


  public getUserProjectFilesArray() : ReadonlyArray<DashboardUserFileEntry>{
    const fileArray = this.userProjectService.getProjectFiles();
    if (!fileArray) {
      return [];
    }
    return fileArray;
  }

  public jumpToWorkflow({ workflow: { wid } }: DashboardWorkflowEntry): void {
    this.router.navigate([`${ROUTER_WORKFLOW_BASE_URL}/${wid}`]).then(null);
  }


  // ----------------- for workflow card
  public confirmEditWorkflowName(
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

        this.workflows[index] = updatedDashboardWorkFlowEntry;
      })
      .add(() => {
        this.isEditingWorkflowName = this.isEditingWorkflowName.filter(
          entryIsEditingIndex => entryIsEditingIndex != index
        );
      });
  }

  /**
   * open the Modal based on the workflow clicked on
   */
   public onClickOpenShareAccess({ workflow }: DashboardWorkflowEntry): void {
    const modalRef = this.modalService.open(NgbdModalWorkflowShareAccessComponent);
    modalRef.componentInstance.workflow = workflow;
  }

  /**
   * duplicate the current workflow. A new record will appear in frontend
   * workflow list and backend database.
   * 
   * Modified to also add new workflow to project
   */

  public onClickDuplicateWorkflow({ workflow: { wid } }: DashboardWorkflowEntry): void {
    if (wid) {
      this.workflowPersistService
        .duplicateWorkflow(wid)
        .pipe(
          concatMap((duplicatedWorkflowInfo: DashboardWorkflowEntry) => {
            this.workflows.push(duplicatedWorkflowInfo); 
            return this.userProjectService.addWorkflowToProject(this.pid, duplicatedWorkflowInfo.workflow.wid!);
          }),
          catchError((err : unknown) => {
            throw err;
          }),
          untilDestroyed(this))
        .subscribe(
          () => {},
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
   public onClickDeleteWorkflow({ workflow }: DashboardWorkflowEntry): void {
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
                this.workflows = this.workflows.filter(
                  workflowEntry => workflowEntry.workflow.wid !== wid
                );
              },
              // @ts-ignore // TODO: fix this with notification component
              (err: unknown) => alert(err.error)
            );
        }
      });
  }



  // ----------------- for file card
  public addFileSizeUnit(fileSize: number): string {
    return this.userFileService.addFileSizeUnit(fileSize);
  }

  public confirmEditFileName(
    dashboardUserFileEntry: DashboardUserFileEntry,
    name: string,
    index: number
  ): void {
    const {
      file: { fid },
    } = dashboardUserFileEntry;
    this.userFileService
      .updateFileName(fid, name)
      .pipe(untilDestroyed(this))
      .subscribe(
        () => {
          this.userFileService.refreshDashboardUserFileEntries();
          this.userProjectService.refreshFilesOfProject(this.pid); // -- perform appropriate call for project page
        },
        (err: unknown) => {
          // @ts-ignore // TODO: fix this with notification component
          this.notificationService.error(err.error.message);
          this.userFileService.refreshDashboardUserFileEntries();
          this.userProjectService.refreshFilesOfProject(this.pid); // -- perform appropriate call for project page
        }
      )
      .add(() => (this.isEditingFileName = this.isEditingFileName.filter(fileIsEditing => fileIsEditing != index)));
  }

  public onClickOpenFileShareAccess(dashboardUserFileEntry: DashboardUserFileEntry): void {
    const modalRef = this.modalService.open(NgbdModalUserFileShareAccessComponent);
    modalRef.componentInstance.dashboardUserFileEntry = dashboardUserFileEntry;
  }

  public downloadUserFile(userFileEntry: DashboardUserFileEntry): void {
    this.userFileService
      .downloadUserFile(userFileEntry.file)
      .pipe(untilDestroyed(this))
      .subscribe(
        (response: Blob) => {
          // prepare the data to be downloaded.
          const dataType = response.type;
          const binaryData = [];
          binaryData.push(response);

          // create a download link and trigger it.
          const downloadLink = document.createElement("a");
          downloadLink.href = URL.createObjectURL(new Blob(binaryData, { type: dataType }));
          downloadLink.setAttribute("download", userFileEntry.file.name);
          document.body.appendChild(downloadLink);
          downloadLink.click();
          URL.revokeObjectURL(downloadLink.href);
        },
        (err: unknown) => {
          // @ts-ignore
          this.notificationService.error(err.error.message);
        }
      );
  }

  /**
   * Created new implementation in project service to
   * ensure files in the project page are refreshed
   * 
   * @param userFileEntry 
   */
  public deleteUserFileEntry(userFileEntry: DashboardUserFileEntry): void {
    this.userProjectService.deleteDashboardUserFileEntry(this.pid, userFileEntry);
  }
}
