import { Component, Input, OnChanges, OnInit, SimpleChanges, ViewChild } from "@angular/core";
import { Router } from "@angular/router";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { firstValueFrom, map } from "rxjs";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "../../../../common/service/workflow-persist/workflow-persist.service";
import { NgbdModalAddProjectWorkflowComponent } from "../user-project/user-project-section/ngbd-modal-add-project-workflow/ngbd-modal-add-project-workflow.component";
import { NgbdModalRemoveProjectWorkflowComponent } from "../user-project/user-project-section/ngbd-modal-remove-project-workflow/ngbd-modal-remove-project-workflow.component";
import { DashboardEntry } from "../../type/dashboard-entry";
import { UserService } from "../../../../common/service/user/user.service";
import { UserProjectService } from "../../service/user-project/user-project.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { concatMap, catchError } from "rxjs/operators";
import { Workflow, WorkflowContent } from "../../../../common/type/workflow";
import { NzUploadFile } from "ng-zorro-antd/upload";
import * as JSZip from "jszip";
import { FileSaverService } from "../../service/user-file/file-saver.service";
import { DashboardWorkflow } from "../../type/dashboard-workflow.interface";
import { FiltersComponent } from "../filters/filters.component";

export const ROUTER_WORKFLOW_CREATE_NEW_URL = "/";
export const ROUTER_USER_PROJECT_BASE_URL = "/dashboard/user-project";

export const WORKFLOW_BASE_URL = "workflow";
export const WORKFLOW_OWNER_URL = WORKFLOW_BASE_URL + "/owners";
export const WORKFLOW_ID_URL = WORKFLOW_BASE_URL + "/workflow-ids";

/**
 * Saved-workflow-section component contains information and functionality
 * of the saved workflows section and is re-used in the user projects section when a project is clicked
 *
 * This component:
 *  - displays the workflows the user has access to
 *  - allows easy searching for workflows by name or other parameters using Fuse.js
 *  - sorting options
 *  - creation of a new workflow
 *
 * Steps to add new search parameter:
 *  1. Add a newly formatted dropdown menu in the html and css files, and a backend call to retrieve any necessary data
 *  2. Create an array of objects to hold data for the search parameter and a boolean "checked" variable
 *  3. Write a callback function that triggers when new dropdown menu changes and updates a "filtered" array of the selected options
 *  4. Add call to searchWorkflows() in this function
 *  5. Add parameter to buildMasterFilterList()
 *  6. Update synchronousSearch() to search based on the new parameter (either through filter iteration or fuse)
 *    - If it uses Fuse.js, create OrPathQuery object for multiple of the same new parameter and push it to the AndPathQuery array
 *    - Do this in asyncSearch(if it requires a backend call)
 *  7. Add parameter as key to searchCriteria
 *  8. If it uses Fuse.js, update fuse keys and searchCriteriaPathMapping
 *  9. Add parameter to updateDropdownMenus() and setDropdownSelectionsToUnchecked()
 *
 *
 *
 */
@UntilDestroy()
@Component({
  selector: "texera-saved-workflow-section",
  templateUrl: "user-workflow.component.html",
  styleUrls: ["user-workflow.component.scss"],
})
export class UserWorkflowComponent implements OnInit, OnChanges {
  private _filters?: FiltersComponent;
  @ViewChild(FiltersComponent) get filters(): FiltersComponent {
    if (this._filters) {
      return this._filters;
    }
    throw new Error("Property cannot be accessed before it is initialized.");
  }
  set filters(value: FiltersComponent) {
    value.masterFilterListChange.pipe(untilDestroyed(this)).subscribe({ next: () => this.searchWorkflow() });
    this._filters = value;
  }
  // receive input from parent components (UserProjectSection), if any
  @Input() public pid: number = 0;
  /* variables for workflow editing / search / sort */
  // virtual scroll requires replacing the entire array reference in order to update view
  // see https://github.com/angular/components/issues/14635
  public dashboardWorkflowEntries: ReadonlyArray<DashboardEntry> = [];
  public dashboardWorkflowEntriesIsEditingName: number[] = [];
  public dashboardWorkflowEntriesIsEditingDescription: number[] = [];
  public allDashboardWorkflowEntries: DashboardEntry[] = [];
  public filteredDashboardWorkflowNames: Array<string> = [];
  public workflowSearchValue: string = "";
  public owners = this.workflowPersistService.retrieveOwners().pipe(
    map((owners: string[]) => {
      return owners.map((user: string) => {
        return {
          userName: user,
          checked: false,
        };
      });
    })
  );
  public projectFilterList: number[] = []; // for filter by project mode, track which projects are selected

  public ROUTER_WORKFLOW_BASE_URL = "/workflow";
  public ROUTER_USER_PROJECT_BASE_URL = "/dashboard/user-project";

  constructor(
    private userService: UserService,
    private userProjectService: UserProjectService,
    private workflowPersistService: WorkflowPersistService,
    private notificationService: NotificationService,
    private modalService: NgbModal,
    private router: Router,
    private fileSaverService: FileSaverService
  ) {}

  get zipDownloadButtonEnabled(): boolean {
    return this.dashboardWorkflowEntries.filter(i => i.checked).length > 0;
  }

  ngOnInit() {
    this.registerDashboardWorkflowEntriesRefresh();
  }

  ngOnChanges(changes: SimpleChanges) {
    for (const propName in changes) {
      if (propName === "pid" && changes[propName].currentValue) {
        // listen to see if component is to be re-rendered inside a different project
        this.pid = changes[propName].currentValue;
        this.refreshDashboardWorkflowEntries();
      }
    }
  }

  /**
   * open the Modal to add workflow(s) to project
   */
  public onClickOpenAddWorkflow() {
    const modalRef = this.modalService.open(NgbdModalAddProjectWorkflowComponent);
    modalRef.componentInstance.projectId = this.pid;

    // retrieve updated values from modal via promise
    modalRef.result.then(result => {
      if (result) {
        this.updateDashboardWorkflowEntryCache(result);
      }
    });
  }

  /**
   * open the Modal to remove workflow(s) from project
   */
  public onClickOpenRemoveWorkflow() {
    const modalRef = this.modalService.open(NgbdModalRemoveProjectWorkflowComponent);
    modalRef.componentInstance.projectId = this.pid;

    // retrieve updated values from modal via promise
    modalRef.result.then(result => {
      if (result) {
        this.updateDashboardWorkflowEntryCache(result);
      }
    });
  }

  /**
   * Search workflows by owner name, workflow name, or workflow id
   * Use fuse.js https://fusejs.io/ as the tool for searching
   *
   * search value Format (must follow this):
   *  - WORKFLOWNAME owner:OWNERNAME(S) id:ID(S) operator:OPERATOR(S)
   */
  public async searchWorkflow(): Promise<void> {
    if (this.filters.masterFilterList.length === 0) {
      //if there are no tags, return all workflow entries
      this.dashboardWorkflowEntries = this.allDashboardWorkflowEntries;
      return;
    }
    this.dashboardWorkflowEntries = (await this.search()).map(i => new DashboardEntry(i));
  }

  /**
   * Searches workflows with keywords and filters given in the masterFilterList.
   * @returns
   */
  private async search(): Promise<ReadonlyArray<DashboardWorkflow>> {
    return await firstValueFrom(
      this.workflowPersistService.searchWorkflows(
        this.filters.getSearchKeywords(),
        this.filters.getSearchFilterParameters()
      )
    );
  }

  /**
   * create a new workflow. will redirect to a pre-emptied workspace
   */
  public onClickCreateNewWorkflowFromDashboard(): void {
    this.router.navigate([`${ROUTER_WORKFLOW_CREATE_NEW_URL}`], { queryParams: { pid: this.pid } }).then(null);
  }

  /**
   * duplicate the current workflow. A new record will appear in frontend
   * workflow list and backend database.
   *
   * for workflow components inside a project-section, it will also add
   * the workflow to the project
   */
  public onClickDuplicateWorkflow(entry: DashboardEntry): void {
    if (entry.workflow.workflow.wid) {
      if (this.pid === 0) {
        // not nested within user project section
        this.workflowPersistService
          .duplicateWorkflow(entry.workflow.workflow.wid)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: duplicatedWorkflowInfo => {
              this.dashboardWorkflowEntries = [
                ...this.dashboardWorkflowEntries,
                new DashboardEntry(duplicatedWorkflowInfo),
              ];
            }, // TODO: fix this with notification component
            error: (err: unknown) => alert(err),
          });
      } else {
        // is nested within project section, also add duplicate workflow to project
        this.workflowPersistService
          .duplicateWorkflow(entry.workflow.workflow.wid)
          .pipe(
            concatMap(duplicatedWorkflowInfo => {
              this.dashboardWorkflowEntries = [
                ...this.dashboardWorkflowEntries,
                new DashboardEntry(duplicatedWorkflowInfo),
              ];
              return this.userProjectService.addWorkflowToProject(this.pid, duplicatedWorkflowInfo.workflow.wid!);
            }),
            catchError((err: unknown) => {
              throw err;
            }),
            untilDestroyed(this)
          )
          .subscribe({
            next: () => {},
            // @ts-ignore // TODO: fix this with notification component
            error: (err: unknown) => alert(err.error),
          });
      }
    }
  }

  /**
   * deleteWorkflow trigger the delete workflow
   * component. If user confirms the deletion, the method sends
   * message to frontend and delete the workflow on frontend. It
   * calls the deleteWorkflow method in service which implements backend API.
   */

  public deleteWorkflow(entry: DashboardEntry): void {
    if (entry.workflow.workflow.wid == undefined) {
      return;
    }
    this.workflowPersistService
      .deleteWorkflow(entry.workflow.workflow.wid)
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.dashboardWorkflowEntries = this.dashboardWorkflowEntries.filter(
          workflowEntry => workflowEntry.workflow.workflow.wid !== entry.workflow.workflow.wid
        );
      });
  }

  private registerDashboardWorkflowEntriesRefresh(): void {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.userService.isLogin()) {
          this.refreshDashboardWorkflowEntries();
          this.userProjectService.refreshProjectList();
        } else {
          this.clearDashboardWorkflowEntries();
        }
      });
  }

  /**
   * This is a search function that filters displayed workflows by
   * the project(s) they belong to.  It is currently separated
   * from the fuzzy search logic
   */
  public filterWorkflowsByProject() {
    let newWorkflowEntries = this.allDashboardWorkflowEntries.slice();
    this.projectFilterList.forEach(
      pid => (newWorkflowEntries = newWorkflowEntries.filter(workflow => workflow.workflow.projectIDs.includes(pid)))
    );
    this.dashboardWorkflowEntries = newWorkflowEntries;
  }

  private refreshDashboardWorkflowEntries(): void {
    const observable =
      this.pid === 0
        ? this.workflowPersistService.retrieveWorkflowsBySessionUser()
        : this.userProjectService.retrieveWorkflowsOfProject(this.pid);

    observable.pipe(untilDestroyed(this)).subscribe(dashboardWorkflowEntries => {
      this.allDashboardWorkflowEntries = dashboardWorkflowEntries.map(i => new DashboardEntry(i));
      this.dashboardWorkflowEntries = [...this.allDashboardWorkflowEntries];
      const newEntries = dashboardWorkflowEntries.map(e => e.workflow.name);
      this.filteredDashboardWorkflowNames = [...newEntries];
    });
  }

  /**
   * Used for adding / removing workflow(s) from a project.
   *
   * Updates local caches to reflect what was pushed into backend / returned
   * from the modal
   *
   * @param dashboardWorkflowEntries - returned local cache of workflows
   */
  private updateDashboardWorkflowEntryCache(dashboardWorkflowEntries: DashboardWorkflow[]): void {
    this.allDashboardWorkflowEntries = dashboardWorkflowEntries.map(i => new DashboardEntry(i));
    // update searching / filtering
    this.searchWorkflow();
  }

  private clearDashboardWorkflowEntries(): void {
    this.dashboardWorkflowEntries = [];
  }

  /**
   * Verify Uploaded file name and upload the file
   */
  public onClickUploadExistingWorkflowFromLocal = (file: NzUploadFile): boolean => {
    const fileExtensionIndex = file.name.lastIndexOf(".");
    if (file.name.substring(fileExtensionIndex) === ".zip") {
      this.handleZipUploads(file as unknown as Blob);
    } else {
      this.handleFileUploads(file as unknown as Blob, file.name);
    }
    return false;
  };

  /**
   * process .zip file uploads
   */
  private handleZipUploads(zipFile: Blob) {
    let zip = new JSZip();
    zip.loadAsync(zipFile).then(zip => {
      zip.forEach((relativePath, file) => {
        file.async("blob").then(content => {
          this.handleFileUploads(content, relativePath);
        });
      });
    });
  }

  /**
   * Process .json file uploads
   */
  private handleFileUploads(file: Blob, name: string) {
    let reader = new FileReader();
    reader.readAsText(file);
    reader.onload = () => {
      try {
        const result = reader.result;
        if (typeof result !== "string") {
          throw new Error("Incorrect format: file is not a string");
        }
        const workflowContent = JSON.parse(result) as WorkflowContent;
        const fileExtensionIndex = name.lastIndexOf(".");
        let workflowName: string;
        if (fileExtensionIndex === -1) {
          workflowName = name;
        } else {
          workflowName = name.substring(0, fileExtensionIndex);
        }
        if (workflowName.trim() === "") {
          workflowName = DEFAULT_WORKFLOW_NAME;
        }
        this.workflowPersistService
          .createWorkflow(workflowContent, workflowName)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: uploadedWorkflow => {
              this.dashboardWorkflowEntries = [...this.dashboardWorkflowEntries, new DashboardEntry(uploadedWorkflow)];
            },
            error: (err: unknown) => alert(err),
          });
      } catch (error) {
        this.notificationService.error(
          "An error occurred when importing the workflow. Please import a workflow json file."
        );
      }
    };
  }

  /**
   * Download selected workflow as zip file
   */
  public async onClickOpenDownloadZip() {
    const checkedEntries = this.dashboardWorkflowEntries.filter(i => i.checked);
    if (checkedEntries.length > 0) {
      const zip = new JSZip();
      try {
        for (const entry of checkedEntries) {
          if (!entry.workflow) {
            throw new Error(
              "Incorrect type of DashboardEntry provided to onClickOpenDownloadZip. Entry must be workflow."
            );
          }
          const fileName = this.nameWorkflow(entry.workflow.workflow.name, zip) + ".json";
          if (entry.workflow.workflow.wid) {
            const workflowCopy: Workflow = {
              ...(await firstValueFrom(
                this.workflowPersistService.retrieveWorkflow(entry.workflow.workflow.wid).pipe(untilDestroyed(this))
              )),
              wid: undefined,
              creationTime: undefined,
              lastModifiedTime: undefined,
            };
            const workflowJson = JSON.stringify(workflowCopy.content);
            zip.file(fileName, workflowJson);
          }
        }
      } catch (e) {
        this.notificationService.error(`Workflow download failed. ${e instanceof Error ? e.message : ""}`);
      }
      let dateTime = new Date();
      let filename = "workflowExports-" + dateTime.toISOString() + ".zip";
      const content = await zip.generateAsync({ type: "blob" });
      this.fileSaverService.saveAs(content, filename);
      for (const entry of checkedEntries) {
        entry.checked = false;
      }
    }
  }

  /**
   * Resolve name conflict
   */
  private nameWorkflow(name: string, zip: JSZip) {
    let count = 0;
    let copyName = name;
    while (true) {
      if (!zip.files[copyName + ".json"]) {
        return copyName;
      } else {
        copyName = name + "-" + ++count;
      }
    }
  }
}
