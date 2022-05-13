import { Component, OnInit } from "@angular/core";
import { UserProjectService } from "../../../../service/user-project/user-project.service";
import { ActivatedRoute } from "@angular/router";
import { Router } from "@angular/router";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { NgbdModalAddProjectFileComponent } from "./ngbd-modal-add-project-file/ngbd-modal-add-project-file.component";
import { NgbdModalRemoveProjectFileComponent } from "./ngbd-modal-remove-project-file/ngbd-modal-remove-project-file.component";
import { DashboardWorkflowEntry } from "../../../../type/dashboard-workflow-entry";
import { DashboardUserFileEntry } from "../../../../type/dashboard-user-file-entry";

// ---- for file card
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { UserFileService } from "../../../../service/user-file/user-file.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgbdModalUserFileShareAccessComponent } from "../../user-file-section/ngbd-modal-file-share-access/ngbd-modal-user-file-share-access.component";

export const ROUTER_WORKFLOW_BASE_URL = "/workflow";

@UntilDestroy()
@Component({
  selector: "texera-user-project-section",
  templateUrl: "./user-project-section.component.html",
  styleUrls: ["./user-project-section.component.scss"],
})
export class UserProjectSectionComponent implements OnInit {
  public pid: number = 0;
  public name: string = "";
  public ownerID: number = 0;
  public creationTime: number = 0;
  public workflows: DashboardWorkflowEntry[] = [];

  // ----- for file card
  public isEditingFileName: number[] = [];

  constructor(
    private userProjectService: UserProjectService,
    private route: ActivatedRoute,
    private router: Router,
    private modalService: NgbModal,
    private userFileService: UserFileService,
    private notificationService: NotificationService
  ) {}

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

  public getUserProjectFilesArray(): ReadonlyArray<DashboardUserFileEntry> {
    const fileArray = this.userProjectService.getProjectFiles();
    if (!fileArray) {
      return [];
    }
    return fileArray;
  }

  public jumpToWorkflow({ workflow: { wid } }: DashboardWorkflowEntry): void {
    this.router.navigate([`${ROUTER_WORKFLOW_BASE_URL}/${wid}`]).then(null);
  }

  // ----------------- for file card
  public addFileSizeUnit(fileSize: number): string {
    return this.userFileService.addFileSizeUnit(fileSize);
  }

  public confirmEditFileName(dashboardUserFileEntry: DashboardUserFileEntry, name: string, index: number): void {
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
          // @ts-ignore // TODO: fix this with notification component
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
