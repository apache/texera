import { AfterViewInit, Component, Input, ViewChild } from "@angular/core";
import { Router } from "@angular/router";
import { NzModalService } from "ng-zorro-antd/modal";
import { firstValueFrom, of } from "rxjs";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "../../../../common/service/workflow-persist/workflow-persist.service";
import { NgbdModalAddProjectWorkflowComponent } from "../user-project/user-project-section/ngbd-modal-add-project-workflow/ngbd-modal-add-project-workflow.component";
import { NgbdModalRemoveProjectWorkflowComponent } from "../user-project/user-project-section/ngbd-modal-remove-project-workflow/ngbd-modal-remove-project-workflow.component";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { UserService } from "../../../../common/service/user/user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { Workflow, WorkflowContent } from "../../../../common/type/workflow";
import { NzUploadFile } from "ng-zorro-antd/upload";
import * as JSZip from "jszip";
import { FileSaverService } from "../../../service/user/file/file-saver.service";
import { FiltersComponent } from "../filters/filters.component";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { SearchService } from "../../../service/user/search.service";
import { SortMethod } from "../../../type/sort-method";
import { isDefined } from "../../../../common/util/predicate";
import { UserProjectService } from "../../../service/user/project/user-project.service";
import { map, mergeMap, tap } from "rxjs/operators";
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
export class UserWorkflowComponent implements AfterViewInit {
  public ROUTER_WORKFLOW_BASE_URL = "/dashboard/workspace";
  private _searchResultsComponent?: SearchResultsComponent;
  @ViewChild(SearchResultsComponent) get searchResultsComponent(): SearchResultsComponent {
    if (this._searchResultsComponent) {
      return this._searchResultsComponent;
    }
    throw new Error("Property cannot be accessed before it is initialized.");
  }
  set searchResultsComponent(value: SearchResultsComponent) {
    this._searchResultsComponent = value;
  }
  private _filters?: FiltersComponent;
  @ViewChild(FiltersComponent) get filters(): FiltersComponent {
    if (this._filters) {
      return this._filters;
    }
    throw new Error("Property cannot be accessed before it is initialized.");
  }
  set filters(value: FiltersComponent) {
    value.masterFilterListChange.pipe(untilDestroyed(this)).subscribe({ next: () => this.search() });
    this._filters = value;
  }
  private masterFilterList: ReadonlyArray<string> | null = null;

  // receive input from parent components (UserProjectSection), if any
  @Input() public pid?: number = undefined;
  @Input() public accessLevel?: string = undefined;
  public sortMethod = SortMethod.EditTimeDesc;
  lastSortMethod: SortMethod | null = null;

  constructor(
    private userService: UserService,
    private workflowPersistService: WorkflowPersistService,
    private userProjectService: UserProjectService,
    private notificationService: NotificationService,
    private modalService: NzModalService,
    private router: Router,
    private fileSaverService: FileSaverService,
    private searchService: SearchService
  ) {}

  public multiWorkflowsOperationButtonEnabled(): boolean {
    if (this._searchResultsComponent) {
      return this.searchResultsComponent?.entries.filter(i => i.checked).length > 0;
    } else {
      return false;
    }
  }

  ngAfterViewInit() {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.search());
  }

  /**
   * open the Modal to add workflow(s) to project
   */
  public onClickOpenAddWorkflow() {
    const modalRef = this.modalService.create({
      nzContent: NgbdModalAddProjectWorkflowComponent,
      nzData: { projectId: this.pid },
      nzFooter: null,
      nzTitle: "Add Workflows To Project",
      nzCentered: true,
    });
    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe(() => this.search(true));
  }

  /**
   * open the Modal to remove workflow(s) from project
   */
  public onClickOpenRemoveWorkflow() {
    const modalRef = this.modalService.create({
      nzContent: NgbdModalRemoveProjectWorkflowComponent,
      nzData: { projectId: this.pid },
      nzFooter: null,
      nzTitle: "Remove Workflows From Project",
      nzCentered: true,
    });
    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe(() => this.search(true));
  }

  /**
   * Searches workflows with keywords and filters given in the masterFilterList.
   * @returns
   */
  async search(forced: Boolean = false): Promise<void> {
    const sameList =
      this.masterFilterList !== null &&
      this.filters.masterFilterList.length === this.masterFilterList.length &&
      this.filters.masterFilterList.every((v, i) => v === this.masterFilterList![i]);
    if (!forced && sameList && this.sortMethod === this.lastSortMethod) {
      // If the filter lists are the same, do no make the same request again.
      return;
    }
    this.lastSortMethod = this.sortMethod;
    this.masterFilterList = this.filters.masterFilterList;
    let filterParams = this.filters.getSearchFilterParameters();
    if (isDefined(this.pid)) {
      // force the project id in the search query to be the current pid.
      filterParams.projectIds = [this.pid];
    }
    this.searchResultsComponent.reset(async (start, count) => {
      const results = await firstValueFrom(
        this.searchService.search(
          this.filters.getSearchKeywords(),
          filterParams,
          start,
          count,
          "workflow",
          this.sortMethod
        )
      );
      return {
        entries: results.results.map(i => {
          if (i.workflow) {
            return new DashboardEntry(i.workflow);
          } else {
            throw new Error("Unexpected type in SearchResult.");
          }
        }),
        more: results.more,
      };
    });
    await this.searchResultsComponent.loadMore();
  }

  /**
   * create a new workflow. will redirect to a pre-emptied workspace
   */
  public onClickCreateNewWorkflowFromDashboard(): void {
    const emptyWorkflowContent: WorkflowContent = {
      operators: [],
      commentBoxes: [],
      groups: [],
      links: [],
      operatorPositions: {},
    };
    let localPid = this.pid;
    this.workflowPersistService
      .createWorkflow(emptyWorkflowContent, DEFAULT_WORKFLOW_NAME)
      .pipe(
        tap(createdWorkflow => {
          if (!createdWorkflow.workflow.wid) {
            throw new Error("Workflow creation failed.");
          }
        }),
        mergeMap(createdWorkflow => {
          // Check if localPid is defined; if so, add the workflow to the project
          if (localPid) {
            return this.userProjectService.addWorkflowToProject(localPid, createdWorkflow.workflow.wid!).pipe(
              // Regardless of the project addition outcome, pass the wid downstream
              map(() => createdWorkflow.workflow.wid)
            );
          } else {
            // If there's no localPid, skip adding to the project and directly pass the wid downstream
            return of(createdWorkflow.workflow.wid);
          }
        }),
        untilDestroyed(this)
      )
      .subscribe({
        next: (wid: number | undefined) => {
          // Use the wid here for navigation
          this.router.navigate([this.ROUTER_WORKFLOW_BASE_URL, wid]).then(null);
        },
        error: (err: unknown) => this.notificationService.error("Workflow creation failed"),
      });
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
      if (!isDefined(this.pid)) {
        // not nested within user project section
        this.workflowPersistService
          .duplicateWorkflow([entry.workflow.workflow.wid])
          .pipe(untilDestroyed(this))
          .subscribe({
            next: duplicatedWorkflowsInfo => {
              this.searchResultsComponent.entries = [
                ...duplicatedWorkflowsInfo.map(duplicatedWorkflowInfo => new DashboardEntry(duplicatedWorkflowInfo)),
                ...this.searchResultsComponent.entries,
              ];
            },
            // @ts-ignore // TODO: fix this with notification component
            error: (err: unknown) => alert(err.error),
          });
      } else {
        // is nested within project section, also add duplicate workflow to project
        let localPid = this.pid;
        this.workflowPersistService
          .duplicateWorkflow([entry.workflow.workflow.wid], localPid)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: duplicatedWorkflowsInfo => {
              this.searchResultsComponent.entries = [
                ...duplicatedWorkflowsInfo.map(duplicatedWorkflowInfo => new DashboardEntry(duplicatedWorkflowInfo)),
                ...this.searchResultsComponent.entries,
              ];
            },
            // @ts-ignore // TODO: fix this with notification component
            error: (err: unknown) => alert(err),
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
      .deleteWorkflow([entry.workflow.workflow.wid])
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.searchResultsComponent.entries = this.searchResultsComponent.entries.filter(
          workflowEntry => workflowEntry.workflow.workflow.wid !== entry.workflow.workflow.wid
        );
      });
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
              this.searchResultsComponent.entries = [
                ...this.searchResultsComponent.entries,
                new DashboardEntry(uploadedWorkflow),
              ];
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
    const checkedEntries = this.searchResultsComponent.entries.filter(i => i.checked);
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
              readonly: false,
            };
            const workflowJson = JSON.stringify(workflowCopy.content);
            zip.file(fileName, workflowJson);
          }
        }
      } catch (e) {
        this.notificationService.error(`Workflow download failed. ${(e as Error).message}`);
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

  public onClickDuplicateSelectedWorkflows(): void {
    const checkedEntries = this.searchResultsComponent.entries.filter(i => i.checked);
    let targetWids: number[] = [];

    for (const entry of checkedEntries) {
      const wid = entry.workflow.workflow.wid;
      if (wid) {
        targetWids.push(wid);
      } else {
        return;
      }
    }

    if (targetWids.length > 0) {
      if (!isDefined(this.pid)) {
        this.workflowPersistService
          .duplicateWorkflow(targetWids)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: duplicatedWorkflowsInfo => {
              this.searchResultsComponent.entries = [
                ...duplicatedWorkflowsInfo.map(duplicatedWorkflowInfo => new DashboardEntry(duplicatedWorkflowInfo)),
                ...this.searchResultsComponent.entries,
              ];
            }, // TODO: fix this with notification component
            error: (err: unknown) => alert(err),
          });
      } else {
        const localPid = this.pid;
        this.workflowPersistService
          .duplicateWorkflow(targetWids, localPid)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: duplicatedWorkflowsInfo => {
              this.searchResultsComponent.entries = [
                ...duplicatedWorkflowsInfo.map(duplicatedWorkflowInfo => new DashboardEntry(duplicatedWorkflowInfo)),
                ...this.searchResultsComponent.entries,
              ];
            }, // TODO: fix this with notification component
            error: (err: unknown) => alert(err),
          });
      }
    }
  }

  public handleConfirmDeleteSelectedWorkflows(): void {
    const checkedEntries = this.searchResultsComponent.entries.filter(i => i.checked);
    let targetWids: number[] = [];

    for (const entry of checkedEntries) {
      const wid = entry.workflow.workflow.wid;
      if (wid) {
        targetWids.push(wid);
      } else {
        return;
      }
    }

    if (targetWids.length > 0) {
      this.workflowPersistService
        .deleteWorkflow(targetWids)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: _ => {
            this.searchResultsComponent.entries = this.searchResultsComponent.entries.filter(workflowEntry => {
              let entryWid = workflowEntry.workflow.workflow.wid;
              // Check if wid is defined and if it's not included in targetWids
              return entryWid === undefined || !targetWids.includes(entryWid);
            });
          },
          // TODO: fix this with notification component
          error: (err: unknown) => alert(err),
        });
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
