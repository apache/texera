import { Component, OnInit, Input, SimpleChanges, OnChanges } from "@angular/core";
import { Router } from "@angular/router";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { from, Observable } from "rxjs";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "../../../../common/service/workflow-persist/workflow-persist.service";
import { NgbdModalWorkflowShareAccessComponent } from "./ngbd-modal-share-access/ngbd-modal-workflow-share-access.component";
import { NgbdModalAddProjectWorkflowComponent } from "../user-project-list/user-project-section/ngbd-modal-add-project-workflow/ngbd-modal-add-project-workflow.component";
import { NgbdModalRemoveProjectWorkflowComponent } from "../user-project-list/user-project-section/ngbd-modal-remove-project-workflow/ngbd-modal-remove-project-workflow.component";
import { DashboardWorkflowEntry } from "../../../type/dashboard-workflow-entry";
import { UserService } from "../../../../common/service/user/user.service";
import { UserProjectService } from "../../../service/user-project/user-project.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import Fuse from "fuse.js";
import { concatMap, catchError } from "rxjs/operators";
import { NgbdModalWorkflowExecutionsComponent } from "./ngbd-modal-workflow-executions/ngbd-modal-workflow-executions.component";
import { environment } from "../../../../../environments/environment";
import { UserProject } from "../../../type/user-project";
import { DeletePromptComponent } from "../../delete-prompt/delete-prompt.component";
import { Workflow, WorkflowContent } from "../../../../common/type/workflow";
import { NzUploadFile } from "ng-zorro-antd/upload";
import { saveAs } from "file-saver";

export const ROUTER_WORKFLOW_BASE_URL = "/workflow";
export const ROUTER_WORKFLOW_CREATE_NEW_URL = "/";
export const ROUTER_USER_PROJECT_BASE_URL = "/dashboard/user-project";

@UntilDestroy()
@Component({
  selector: "texera-saved-workflow-section",
  templateUrl: "./saved-workflow-section.component.html",
  styleUrls: ["./saved-workflow-section.component.scss", "../../dashboard.component.scss"],
})
export class SavedWorkflowSectionComponent implements OnInit, OnChanges {
  // receive input from parent components (UserProjectSection), if any
  @Input() public pid: number = 0;
  @Input() public updateProjectStatus: string = ""; // track changes to user project(s) (i.e color update / removal)

  /* variables for workflow editing / search */
  // virtual scroll requires replacing the entire array reference in order to update view
  // see https://github.com/angular/components/issues/14635
  public dashboardWorkflowEntries: ReadonlyArray<DashboardWorkflowEntry> = [];
  public dashboardWorkflowEntriesIsEditingName: number[] = [];
  public allDashboardWorkflowEntries: DashboardWorkflowEntry[] = [];
  public filteredDashboardWorkflowNames: Array<string> = [];
  public fuse = new Fuse([] as ReadonlyArray<DashboardWorkflowEntry>, {
    shouldSort: true,
    threshold: 0.2,
    location: 0,
    distance: 100,
    minMatchCharLength: 1,
    keys: ["workflow.wid", "workflow.name", "ownerName"],
  });
  public searchCriteriaPathMapping: Map<string, string[]> = new Map([
    ["workflowName", ["workflow", "name"]],
    ["id", ["workflow", "wid"]],
    ["owner", ["ownerName"]],
  ]);
  public workflowSearchValue: string = "";
  private defaultWorkflowName: string = "Untitled Workflow";
  public searchCriteria: string[] = ["owner", "id"];
  // whether tracking metadata information about executions is enabled
  public workflowExecutionsTrackingEnabled: boolean = environment.workflowExecutionsTrackingEnabled;

  /* variables for project color tags */
  public userProjectsMap: ReadonlyMap<number, UserProject> = new Map(); // maps pid to its corresponding UserProject
  public colorBrightnessMap: ReadonlyMap<number, boolean> = new Map(); // tracks whether each project's color is light or dark
  public userProjectsLoaded: boolean = false; // tracks whether all UserProject information has been loaded (ready to render project colors)

  /* variables for filtering workflows by projects */
  public userProjectsList: ReadonlyArray<UserProject> = []; // list of projects accessible by user
  public projectFilterList: number[] = []; // for filter by project mode, track which projects are selected
  public isSearchByProject: boolean = false; // track searching mode user currently selects

  constructor(
    private userService: UserService,
    private userProjectService: UserProjectService,
    private workflowPersistService: WorkflowPersistService,
    private notificationService: NotificationService,
    private modalService: NgbModal,
    private router: Router
  ) {}

  ngOnInit() {
    this.registerDashboardWorkflowEntriesRefresh();
  }

  ngOnChanges(changes: SimpleChanges) {
    for (const propName in changes) {
      if (propName == "pid" && changes[propName].currentValue) {
        // listen to see if component is to be re-rendered inside a different project
        this.pid = changes[propName].currentValue;
        this.refreshDashboardWorkflowEntries();
      } else if (propName == "updateProjectStatus" && changes[propName].currentValue) {
        // listen to see if parent component has been mutated (e.g. project color changed)
        this.updateProjectStatus = changes[propName].currentValue;
        this.refreshUserProjects();
      }
    }
  }

  /**
   * open the Modal based on the workflow clicked on
   */
  public onClickOpenShareAccess({ workflow }: DashboardWorkflowEntry): void {
    const modalRef = this.modalService.open(NgbdModalWorkflowShareAccessComponent);
    modalRef.componentInstance.workflow = workflow;
  }

  /**
   * open the workflow executions page
   */
  public onClickGetWorkflowExecutions({ workflow }: DashboardWorkflowEntry): void {
    const modalRef = this.modalService.open(NgbdModalWorkflowExecutionsComponent, {
      size: "lg",
      windowClass: "modal-xl",
    });
    modalRef.componentInstance.workflow = workflow;
    modalRef.componentInstance.workflowName = workflow.name;
  }

  /**
   * Download the workflow as a json file
   */
  public onClickDownloadWorkfllow({ workflow: { wid } }: DashboardWorkflowEntry): void {
    if (wid) {
      this.workflowPersistService
        .retrieveWorkflow(wid)
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
          saveAs(new Blob([workflowJson], { type: "text/plain;charset=utf-8" }), fileName);
        });
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

  public searchInputOnChange(value: string): void {
    // enable autocomplete only when searching for workflow name
    if (!value.includes(":")) {
      const filteredDashboardWorkflowNames: string[] = [];
      this.allDashboardWorkflowEntries.forEach(dashboardEntry => {
        const workflowName = dashboardEntry.workflow.name;
        if (workflowName.toLowerCase().indexOf(value.toLowerCase()) !== -1) {
          filteredDashboardWorkflowNames.push(workflowName);
        }
      });
      this.filteredDashboardWorkflowNames = filteredDashboardWorkflowNames;
    }
  }

  // check https://fusejs.io/api/query.html#logical-query-operators for logical query operators rule
  public buildAndPathQuery(
    workflowSearchField: string,
    workflowSearchValue: string
  ): {
    $path: ReadonlyArray<string>;
    $val: string;
  } {
    return {
      $path: this.searchCriteriaPathMapping.get(workflowSearchField) as ReadonlyArray<string>,
      $val: workflowSearchValue,
    };
  }

  /**
   * Search workflows by owner name, workflow name or workflow id
   * Use fuse.js https://fusejs.io/ as the tool for searching
   */
  public searchWorkflow(): void {
    let andPathQuery: Object[] = [];
    // empty search value, return all workflow entries
    if (this.workflowSearchValue.trim() === "") {
      this.dashboardWorkflowEntries = [...this.allDashboardWorkflowEntries];
      return;
    } else if (!this.workflowSearchValue.includes(":")) {
      // search only by workflow name
      andPathQuery.push(this.buildAndPathQuery("workflowName", this.workflowSearchValue));
      this.dashboardWorkflowEntries = this.fuse.search({ $and: andPathQuery }).map(res => res.item);
      return;
    }
    const searchConsitionsSet = new Set(this.workflowSearchValue.trim().split(/ +(?=(?:(?:[^"]*"){2})*[^"]*$)/g));
    searchConsitionsSet.forEach(condition => {
      // field search
      if (condition.includes(":")) {
        const conditionArray = condition.split(":");
        if (conditionArray.length !== 2) {
          this.notificationService.error("Please check the format of the search query");
          return;
        }
        const workflowSearchField = conditionArray[0];
        const workflowSearchValue = conditionArray[1];
        if (!this.searchCriteria.includes(workflowSearchField)) {
          this.notificationService.error("Cannot search by " + workflowSearchField);
          return;
        }
        andPathQuery.push(this.buildAndPathQuery(workflowSearchField, workflowSearchValue));
      } else {
        //search by workflow name
        andPathQuery.push(this.buildAndPathQuery("workflowName", condition));
      }
    });
    this.dashboardWorkflowEntries = this.fuse.search({ $and: andPathQuery }).map(res => res.item);
  }

  /**
   * sort the workflow by name in ascending order
   */
  public ascSort(): void {
    this.dashboardWorkflowEntries = this.dashboardWorkflowEntries
      .slice()
      .sort((t1, t2) => t1.workflow.name.toLowerCase().localeCompare(t2.workflow.name.toLowerCase()));
  }

  /**
   * sort the project by name in descending order
   */
  public dscSort(): void {
    this.dashboardWorkflowEntries = this.dashboardWorkflowEntries
      .slice()
      .sort((t1, t2) => t2.workflow.name.toLowerCase().localeCompare(t1.workflow.name.toLowerCase()));
  }

  /**
   * sort the project by creating time
   */
  public dateSort(): void {
    this.dashboardWorkflowEntries = this.dashboardWorkflowEntries
      .slice()
      .sort((left, right) =>
        left.workflow.creationTime !== undefined && right.workflow.creationTime !== undefined
          ? left.workflow.creationTime - right.workflow.creationTime
          : 0
      );
  }

  /**
   * sort the project by last modified time
   */
  public lastSort(): void {
    this.dashboardWorkflowEntries = this.dashboardWorkflowEntries
      .slice()
      .sort((left, right) =>
        left.workflow.lastModifiedTime !== undefined && right.workflow.lastModifiedTime !== undefined
          ? left.workflow.lastModifiedTime - right.workflow.lastModifiedTime
          : 0
      );
  }

  /**
   * create a new workflow. will redirect to a pre-emptied workspace
   */
  public onClickCreateNewWorkflowFromDashboard(): void {
    this.router.navigate([`${ROUTER_WORKFLOW_CREATE_NEW_URL}`], { queryParams: { pid: this.pid } }).then(null);
  }

  /**
   * Verify Uploaded file name and upload the file
   */
  public onClickUploadExistingWorkflowFromLocal = (file: NzUploadFile): boolean => {
    var reader = new FileReader();
    reader.readAsText(file as unknown as Blob);
    reader.onload = () => {
      try {
        const result = reader.result;
        if (typeof result !== "string") {
          throw new Error("Incorrect format: file is not a string");
        }
        const workflowContent = JSON.parse(result) as WorkflowContent;
        const fileExtensionIndex = file.name.lastIndexOf(".");
        var workflowName: string;
        if (fileExtensionIndex === -1) {
          workflowName = file.name;
        } else {
          workflowName = file.name.substring(0, fileExtensionIndex);
        }
        if (workflowName.trim() === "") {
          workflowName = DEFAULT_WORKFLOW_NAME;
        }
        this.workflowPersistService
          .createWorkflow(workflowContent, workflowName)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: uploadedWorkflow => {
              this.dashboardWorkflowEntries = [...this.dashboardWorkflowEntries, uploadedWorkflow];
            },
            error: (err: unknown) => alert(err),
          });
      } catch (error) {
        this.notificationService.error(
          "An error occurred when importing the workflow. Please import a workflow json file."
        );
        console.error(error);
      }
    };
    return false;
  };

  /**
   * duplicate the current workflow. A new record will appear in frontend
   * workflow list and backend database.
   *
   * for workflow components inside a project-section, it will also add
   * the workflow to the project
   */
  public onClickDuplicateWorkflow({ workflow: { wid } }: DashboardWorkflowEntry): void {
    if (wid) {
      if (this.pid == 0) {
        // not nested within user project section
        this.workflowPersistService
          .duplicateWorkflow(wid)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: duplicatedWorkflowInfo => {
              this.dashboardWorkflowEntries = [...this.dashboardWorkflowEntries, duplicatedWorkflowInfo];
            }, // TODO: fix this with notification component
            error: (err: unknown) => alert(err),
          });
      } else {
        // is nested within project section, also add duplicate workflow to project
        this.workflowPersistService
          .duplicateWorkflow(wid)
          .pipe(
            concatMap((duplicatedWorkflowInfo: DashboardWorkflowEntry) => {
              this.dashboardWorkflowEntries = [...this.dashboardWorkflowEntries, duplicatedWorkflowInfo];
              return this.userProjectService.addWorkflowToProject(this.pid, duplicatedWorkflowInfo.workflow.wid!);
            }),
            catchError((err: unknown) => {
              throw err;
            }),
            untilDestroyed(this)
          )
          .subscribe(
            () => {},
            // @ts-ignore // TODO: fix this with notification component
            (err: unknown) => alert(err.error)
          );
      }
    }
  }

  /**
   * openNgbdModalDeleteWorkflowComponent trigger the delete workflow
   * component. If user confirms the deletion, the method sends
   * message to frontend and delete the workflow on frontend. It
   * calls the deleteProject method in service which implements backend API.
   */
  public openNgbdModalDeleteWorkflowComponent({ workflow }: DashboardWorkflowEntry): void {
    const modalRef = this.modalService.open(DeletePromptComponent);
    modalRef.componentInstance.deletionType = "workflow";
    modalRef.componentInstance.deletionName = workflow.name;

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

  /**
   * navigate to individual project page
   */
  public jumpToProject({ pid }: UserProject): void {
    this.router.navigate([`${ROUTER_USER_PROJECT_BASE_URL}/${pid}`]).then(null);
  }

  private registerDashboardWorkflowEntriesRefresh(): void {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.userService.isLogin()) {
          this.refreshDashboardWorkflowEntries();
          this.refreshUserProjects();
        } else {
          this.clearDashboardWorkflowEntries();
        }
      });
  }

  /**
   * Retrieves from the backend endpoint for projects all user projects
   * that are accessible from the current user.  This is used for
   * the project color tags
   */
  private refreshUserProjects(): void {
    this.userProjectService
      .retrieveProjectList()
      .pipe(untilDestroyed(this))
      .subscribe((userProjectList: UserProject[]) => {
        if (userProjectList != null && userProjectList.length > 0) {
          // map project ID to project object
          this.userProjectsMap = new Map(userProjectList.map(userProject => [userProject.pid, userProject]));

          // calculate whether project colors are light or dark
          const projectColorBrightnessMap: Map<number, boolean> = new Map();
          userProjectList.forEach(userProject => {
            if (userProject.color != null) {
              projectColorBrightnessMap.set(userProject.pid, this.userProjectService.isLightColor(userProject.color));
            }
          });
          this.colorBrightnessMap = projectColorBrightnessMap;

          // store the projects containing these workflows
          this.userProjectsList = userProjectList;
          this.userProjectsLoaded = true;
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
      pid => (newWorkflowEntries = newWorkflowEntries.filter(workflow => workflow.projectIDs.includes(pid)))
    );
    this.dashboardWorkflowEntries = newWorkflowEntries;
  }

  /**
   * This is a helper function that toggles between the default
   * workflow search bar and the filter by project search mode.
   */
  public toggleWorkflowSearchMode() {
    this.isSearchByProject = !this.isSearchByProject;
    if (this.isSearchByProject) {
      this.filterWorkflowsByProject();
    } else {
      this.searchWorkflow();
    }
  }
  /**
   * For color tags, enable clicking 'x' to remove a workflow from a project
   *
   * @param pid
   * @param dashboardWorkflowEntry
   * @param index
   */
  public removeWorkflowFromProject(pid: number, dashboardWorkflowEntry: DashboardWorkflowEntry, index: number): void {
    this.userProjectService
      .removeWorkflowFromProject(pid, dashboardWorkflowEntry.workflow.wid!)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        let updatedDashboardWorkFlowEntry = { ...dashboardWorkflowEntry };
        updatedDashboardWorkFlowEntry.projectIDs = dashboardWorkflowEntry.projectIDs.filter(
          projectID => projectID != pid
        );

        // update allDashboardWorkflowEntries
        const newAllDashboardEntries = this.allDashboardWorkflowEntries.slice();
        for (let i = 0; i < newAllDashboardEntries.length; ++i) {
          if (newAllDashboardEntries[i].workflow.wid == dashboardWorkflowEntry.workflow.wid) {
            newAllDashboardEntries[i] = updatedDashboardWorkFlowEntry;
            break;
          }
        }
        this.allDashboardWorkflowEntries = newAllDashboardEntries;
        this.fuse.setCollection(this.allDashboardWorkflowEntries);

        // update dashboardWorkflowEntries
        const newEntries = this.dashboardWorkflowEntries.slice();
        newEntries[index] = updatedDashboardWorkFlowEntry;
        this.dashboardWorkflowEntries = newEntries;

        // update filtering results by project, if applicable
        if (this.isSearchByProject) {
          // refilter workflows by projects (to include / exclude changed workflows)
          this.filterWorkflowsByProject();
        }
      });
  }

  private refreshDashboardWorkflowEntries(): void {
    let observable: Observable<DashboardWorkflowEntry[]>;

    if (this.pid === 0) {
      // not nested within user project section
      observable = this.workflowPersistService.retrieveWorkflowsBySessionUser();
    } else {
      // is nested within project section, get workflows belonging to project
      observable = this.userProjectService.retrieveWorkflowsOfProject(this.pid);
    }

    observable.pipe(untilDestroyed(this)).subscribe(dashboardWorkflowEntries => {
      this.dashboardWorkflowEntries = dashboardWorkflowEntries;
      this.allDashboardWorkflowEntries = dashboardWorkflowEntries;
      this.fuse.setCollection(this.allDashboardWorkflowEntries);
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
  private updateDashboardWorkflowEntryCache(dashboardWorkflowEntries: DashboardWorkflowEntry[]): void {
    this.allDashboardWorkflowEntries = dashboardWorkflowEntries;
    this.fuse.setCollection(this.allDashboardWorkflowEntries);

    // update searching / filtering
    if (this.isSearchByProject) {
      // refilter workflows by projects (to include / exclude changed w)
      this.filterWorkflowsByProject();
    } else {
      // (regular search mode) : update search results / autcomplete for current search value
      this.searchInputOnChange(this.workflowSearchValue);
      this.searchWorkflow();
    }
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
        const newEntries = this.dashboardWorkflowEntries.slice();
        newEntries[index] = updatedDashboardWorkFlowEntry;
        this.dashboardWorkflowEntries = newEntries;
      })
      .add(() => {
        this.dashboardWorkflowEntriesIsEditingName = this.dashboardWorkflowEntriesIsEditingName.filter(
          entryIsEditingIndex => entryIsEditingIndex != index
        );
      });
  }
}
