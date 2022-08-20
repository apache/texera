import { Component, OnInit, Input, SimpleChanges, OnChanges } from "@angular/core";
import { Router } from "@angular/router";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { remove } from "lodash-es";
import { from, Observable, map } from "rxjs";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "../../../../common/service/workflow-persist/workflow-persist.service";
import { NgbdModalWorkflowShareAccessComponent } from "./ngbd-modal-share-access/ngbd-modal-workflow-share-access.component";
import { NgbdModalAddProjectWorkflowComponent } from "../user-project-list/user-project-section/ngbd-modal-add-project-workflow/ngbd-modal-add-project-workflow.component";
import { NgbdModalRemoveProjectWorkflowComponent } from "../user-project-list/user-project-section/ngbd-modal-remove-project-workflow/ngbd-modal-remove-project-workflow.component";
import { DashboardWorkflowEntry, SortMethod } from "../../../type/dashboard-workflow-entry";
import { UserService } from "../../../../common/service/user/user.service";
import { UserProjectService } from "../../../service/user-project/user-project.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import Fuse from "fuse.js";
import { concatMap, catchError } from "rxjs/operators";
import { NgbdModalWorkflowExecutionsComponent } from "./ngbd-modal-workflow-executions/ngbd-modal-workflow-executions.component";
import { environment } from "../../../../../environments/environment";
import { UserProject } from "../../../type/user-project";
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";
import { DeletePromptComponent } from "../../delete-prompt/delete-prompt.component";
import { HttpClient } from "@angular/common/http";
import { AppSettings } from "src/app/common/app-setting";
import { Workflow, WorkflowContent } from "../../../../common/type/workflow";
import { NzUploadFile } from "ng-zorro-antd/upload";
import { saveAs } from "file-saver";
import * as JSZip from "jszip";

export const ROUTER_WORKFLOW_BASE_URL = "/workflow";
export const ROUTER_WORKFLOW_CREATE_NEW_URL = "/";
export const ROUTER_USER_PROJECT_BASE_URL = "/dashboard/user-project";

export const WORKFLOW_BASE_URL = "workflow";
export const WORKFLOW_OPERATOR_URL = WORKFLOW_BASE_URL + "/search-by-operators";
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
  templateUrl: "./saved-workflow-section.component.html",
  styleUrls: ["./saved-workflow-section.component.scss", "../../dashboard.component.scss"],
})
export class SavedWorkflowSectionComponent implements OnInit, OnChanges {
  // receive input from parent components (UserProjectSection), if any
  @Input() public pid: number = 0;
  @Input() public updateProjectStatus: string = ""; // track changes to user project(s) (i.e color update / removal)

  /**
   * variables for dropdown menus and searching
   */
  public owners: { userName: string; checked: boolean }[] = [];
  public wids: { id: string; checked: boolean }[] = [];
  public operatorGroups: string[] = [];
  public operators: Map<
    string,
    { userFriendlyName: string; operatorType: string; operatorGroup: string; checked: boolean }[]
  > = new Map();
  public selectedDate: null | Date = null;
  private selectedOwners: string[] = [];
  private selectedIDs: string[] = [];
  private selectedOperators: { userFriendlyName: string; operatorType: string; operatorGroup: string }[] = [];
  private selectedProjects: { name: string; pid: number }[] = [];

  public masterFilterList: string[] = [];

  /* variables for workflow editing / search / sort */
  // virtual scroll requires replacing the entire array reference in order to update view
  // see https://github.com/angular/components/issues/14635
  public dashboardWorkflowEntries: ReadonlyArray<DashboardWorkflowEntry> = [];
  public dashboardWorkflowEntriesIsEditingName: number[] = [];
  public allDashboardWorkflowEntries: DashboardWorkflowEntry[] = [];
  public filteredDashboardWorkflowNames: Array<string> = [];
  public fuse = new Fuse([] as ReadonlyArray<DashboardWorkflowEntry>, {
    useExtendedSearch: true,
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

  public searchCriteria: string[] = ["owner", "id", "ctime", "operator", "project"];
  public sortMethod = SortMethod.EditTimeDesc;

  // whether tracking metadata information about executions is enabled
  public workflowExecutionsTrackingEnabled: boolean = environment.workflowExecutionsTrackingEnabled;

  /* variables for project color tags */
  public userProjectsMap: ReadonlyMap<number, UserProject> = new Map(); // maps pid to its corresponding UserProject
  public colorBrightnessMap: ReadonlyMap<number, boolean> = new Map(); // tracks whether each project's color is light or dark
  public userProjectsLoaded: boolean = false; // tracks whether all UserProject information has been loaded (ready to render project colors)

  /* variables for filtering workflows by projects */
  public userProjectsList: ReadonlyArray<UserProject> = []; // list of projects accessible by user
  public userProjectsDropdown: { pid: number; name: string; checked: boolean }[] = [];
  public projectFilterList: number[] = []; // for filter by project mode, track which projects are selected
  public downloadListWorkflow = new Map<number, string>();
  public zip = new JSZip();

  constructor(
    private http: HttpClient,
    private userService: UserService,
    private userProjectService: UserProjectService,
    private workflowPersistService: WorkflowPersistService,
    private notificationService: NotificationService,
    private operatorMetadataService: OperatorMetadataService,
    private modalService: NgbModal,
    private router: Router
  ) {}

  ngOnInit() {
    this.registerDashboardWorkflowEntriesRefresh();
    this.searchParameterBackendSetup();
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
      size: "xl",
      modalDialogClass: "modal-dialog-centered",
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

  /**
   * Backend calls for Workflow IDs, Owners, and Operators in saved workflow component
   */
  private searchParameterBackendSetup() {
    this.operatorMetadataService.getOperatorMetadata().subscribe(opdata => {
      opdata.groups.forEach(group => {
        this.operators.set(
          group.groupName,
          opdata.operators
            .filter(operator => operator.additionalMetadata.operatorGroupName === group.groupName)
            .map(operator => {
              return {
                userFriendlyName: operator.additionalMetadata.userFriendlyName,
                operatorType: operator.operatorType,
                operatorGroup: operator.additionalMetadata.operatorGroupName,
                checked: false,
              };
            })
        );
      });
      this.operatorGroups = opdata.groups.map(group => group.groupName);
    });
    this.retrieveOwners()
      .pipe(untilDestroyed(this))
      .subscribe(list_of_owners => (this.owners = list_of_owners));
    this.retrieveIDs()
      .pipe(untilDestroyed(this))
      .subscribe(list_of_ids => (this.wids = list_of_ids));
  }

  /**
   * Search workflows based on date string
   * String Formats:
   *  - ctime:YYYY-MM-DD (workflows on this date)
   *  - ctime:<YYYY-MM-DD (workflows on or before this date)
   *  - ctime:>YYYY-MM-DD (workflows on or after this date)
   */
  private searchCreationTime(
    date: Date,
    filteredDashboardWorkflowEntries: ReadonlyArray<DashboardWorkflowEntry>
  ): ReadonlyArray<DashboardWorkflowEntry> {
    date.setHours(0), date.setMinutes(0), date.setSeconds(0), date.setMilliseconds(0);
    //sets date time at beginning of day
    //date obj from nz-calendar adds extraneous time
    return filteredDashboardWorkflowEntries.filter(workflow_entry => {
      //filters for workflows that were created on the specified date
      if (workflow_entry.workflow.creationTime) {
        return (
          workflow_entry.workflow.creationTime >= date.getTime() &&
          workflow_entry.workflow.creationTime < date.getTime() + 86400000
        );
        //checks if creation time is within the range of the whole day
      }
      return false;
    });
  }

  /**
   * updates selectedOwners array to match owners checked in dropdown menu
   */
  public updateSelectedOwners(): void {
    this.selectedOwners = this.owners.filter(owner => owner.checked).map(owner => owner.userName);
    this.searchWorkflow();
  }

  /**
   * updates selectedIDs array to match worfklow ids checked in dropdown menu
   */
  public updateSelectedIDs(): void {
    this.selectedIDs = this.wids.filter(wid => wid.checked === true).map(wid => wid.id);
    this.searchWorkflow();
  }

  /**
   * updates selectedOperators array to match operators checked in dropdown menu
   */
  public updateSelectedOperators(): void {
    const filteredOperators: { userFriendlyName: string; operatorType: string; operatorGroup: string }[] = [];
    Array.from(this.operators.values())
      .flat()
      .forEach(operator => {
        if (operator.checked) {
          filteredOperators.push({
            userFriendlyName: operator.userFriendlyName,
            operatorType: operator.operatorType,
            operatorGroup: operator.operatorGroup,
          });
        }
      });
    this.selectedOperators = filteredOperators;
    this.searchWorkflow();
  }

  /**
   * updates selectedProjects array to match projects checked in dropdown menu
   */
  public updateSelectedProjects(): void {
    this.selectedProjects = this.userProjectsDropdown
      .filter(proj => proj.checked === true)
      .map(proj => {
        return { name: proj.name, pid: proj.pid };
      });
    this.searchWorkflow();
  }

  /**
   * callback function when calendar is altered
   */
  public calendarValueChange(value: Date): void {
    this.searchWorkflow();
  }

  /**
   * updates dropdown menus when nz-select bar is changed
   */
  public updateDropdownMenus(tagListString: string[]): void {
    //operators array is not cleared, so that operator object properties can be used for reconstruction of the array
    //operators map is too expensive/difficult to search for operator object properties
    this.selectedIDs = [];
    this.selectedOwners = [];
    this.selectedProjects = [];
    let newSelectedOperators: { userFriendlyName: string; operatorType: string; operatorGroup: string }[] = [];
    this.selectedDate = null;
    this.setDropdownSelectionsToUnchecked();
    tagListString.forEach(tag => {
      if (tag.includes(":")) {
        const searchArray = tag.split(":");
        const searchField = searchArray[0];
        const searchValue = searchArray[1].trim();
        switch (searchField) {
          case "owner":
            const selectedOwnerIndex = this.owners.findIndex(owner => owner.userName === searchValue);
            if (selectedOwnerIndex === -1) {
              remove(this.masterFilterList, filterTag => filterTag === tag);
              this.notificationService.error("Invalid owner name");
              break;
            }
            this.owners[selectedOwnerIndex].checked = true;
            this.selectedOwners.push(searchValue);
            break;
          case "id":
            const selectedIDIndex = this.wids.findIndex(wid => wid.id === searchValue);
            if (selectedIDIndex === -1) {
              remove(this.masterFilterList, filterTag => filterTag === tag);
              this.notificationService.error("Invalid workflow id");
              break;
            }
            this.wids[selectedIDIndex].checked = true;
            this.selectedIDs.push(searchValue);
            break;
          case "operator":
            const selectedOperator = this.selectedOperators.find(operator => operator.userFriendlyName === searchValue);
            if (!selectedOperator) {
              remove(this.masterFilterList, filterTag => filterTag === tag);
              this.notificationService.error("Invalid operator name");
              break;
            }
            newSelectedOperators.push(selectedOperator);
            const operatorSublist = this.operators.get(selectedOperator.operatorGroup);
            if (operatorSublist) {
              for (let operator of operatorSublist) {
                if (operator.userFriendlyName === searchValue) {
                  operator.checked = true;
                  break;
                }
              }
            }
            break;
          case "project":
            const selectedProjectIndex = this.userProjectsDropdown.findIndex(proj => proj.name === searchValue);
            if (selectedProjectIndex === -1) {
              remove(this.masterFilterList, filterTag => filterTag === tag);
              this.notificationService.error("Invalid project name");
              break;
            }
            this.userProjectsDropdown[selectedProjectIndex].checked = true;
            const selectedProject = this.userProjectsDropdown[selectedProjectIndex];
            this.selectedProjects.push({ name: selectedProject.name, pid: selectedProject.pid });
            break;
          case "ctime": //should only run at most once
            if (this.selectedDate) {
              // if there is already an selected date, ignore the subsequent ctime tags
              this.notificationService.error("Multiple search dates is not allowed");
              break;
            }
            const date_regex = /^(\d{4})[-](0[1-9]|1[0-2])[-](0[1-9]|[12][0-9]|3[01])$/;
            const searchDate: RegExpMatchArray | null = searchValue.match(date_regex);
            if (!searchDate) {
              this.notificationService.error("Date format is incorrect");
              break;
            }
            this.selectedDate = new Date(parseInt(searchDate[1]), parseInt(searchDate[2]) - 1, parseInt(searchDate[3]));
            break;
        }
      }
    });
    this.selectedOperators = newSelectedOperators;
    this.searchWorkflow();
  }

  /**
   * sets all dropdown menu options to unchecked
   */
  private setDropdownSelectionsToUnchecked(): void {
    this.owners.forEach(owner => {
      owner.checked = false;
    });
    this.wids.forEach(wid => {
      wid.checked = false;
    });
    for (let operatorList of this.operators.values()) {
      operatorList.forEach(operator => (operator.checked = false));
    }
    this.userProjectsDropdown.forEach(proj => {
      proj.checked = false;
    });
  }

  /**
   * constructs OrPathQuery object for search values with in the same category (owner, id, operator, etc.)
   *  -returned object is inserted into AndPathQuery
   *
   * @param searchType - specified fuse search parameter for path mapping
   * @param searchList - list of search parameters of the same type (owner, id, etc.)
   */
  private buildOrPathQuery(searchType: string, searchList: string[], exactMatch: boolean = false) {
    let orPathQuery: Object[] = [];
    searchList
      .map(searchParameter => this.buildAndPathQuery(searchType, (exactMatch ? "=" : "") + searchParameter))
      .forEach(pathQuery => orPathQuery.push(pathQuery));
    return orPathQuery;
  }

  // check https://fusejs.io/api/query.html#logical-query-operators for logical query operators rule
  private buildAndPathQuery(
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
   * builds the tags to be displayd in the nz-select search bar
   * - Workflow names with ":" are not allowed due to conflict with other search parameters' format
   */
  private buildMasterFilterList(): void {
    let newFilterList: string[] = this.masterFilterList.filter(tag => this.checkIfWorkflowName(tag));
    newFilterList = newFilterList.concat(this.selectedOwners.map(owner => "owner: " + owner));
    newFilterList = newFilterList.concat(this.selectedIDs.map(id => "id: " + id));
    newFilterList = newFilterList.concat(
      this.selectedOperators.map(operator => "operator: " + operator.userFriendlyName)
    );
    newFilterList = newFilterList.concat(this.selectedProjects.map(proj => "project: " + proj.name));
    if (this.selectedDate !== null) {
      newFilterList.push("ctime: " + this.getFormattedDateString(this.selectedDate));
    }
    this.masterFilterList = newFilterList;
  }

  /**
   * returns a formatted string representing a Date object
   */
  private getFormattedDateString(date: Date): string {
    let dateMonth: number = date.getMonth() + 1;
    let dateDay: number = date.getDate();
    return `${date.getFullYear()}-${(dateMonth < 10 ? "0" : "") + dateMonth}-${(dateDay < 10 ? "0" : "") + dateDay}`;
  }

  /**
   * Search workflows by owner name, workflow name, or workflow id
   * Use fuse.js https://fusejs.io/ as the tool for searching
   *
   * search value Format (must follow this):
   *  - WORKFLOWNAME owner:OWNERNAME(S) id:ID(S) operator:OPERATOR(S)
   */
  public searchWorkflow(): void {
    this.buildMasterFilterList();
    if (this.masterFilterList.length === 0) {
      //if there are no tags, return all workflow entries
      this.dashboardWorkflowEntries = this.allDashboardWorkflowEntries;
      return;
    }
    if (this.selectedOperators.length > 0) {
      this.asyncSearch();
    } else {
      this.dashboardWorkflowEntries = this.synchronousSearch([]);
    }
  }

  /**
   * backend search that is called if operators are included in search value
   */
  private asyncSearch() {
    let andPathQuery: Object[] = [];
    this.retrieveWorkflowByOperator(this.selectedOperators.map(operator => operator.operatorType).toString())
      .pipe(untilDestroyed(this))
      .subscribe(list_of_wids => {
        andPathQuery.push({ $or: this.buildOrPathQuery("id", list_of_wids, true) });
        this.dashboardWorkflowEntries = this.synchronousSearch(andPathQuery);
      });
  }

  /**
   * Searches workflows with given frontend data
   * no backend calls so runs synchronously
   */
  private synchronousSearch(andPathQuery: Object[]): ReadonlyArray<DashboardWorkflowEntry> {
    let searchOutput: ReadonlyArray<DashboardWorkflowEntry> = this.allDashboardWorkflowEntries.slice();

    //builds andPathQuery from arrays containing selected values
    const workflowNames: string[] = this.masterFilterList.filter(tag => this.checkIfWorkflowName(tag));

    if (workflowNames.length !== 0) {
      andPathQuery.push({ $or: this.buildOrPathQuery("workflowName", workflowNames) });
    }
    if (this.selectedOwners.length !== 0) {
      andPathQuery.push({ $or: this.buildOrPathQuery("owner", this.selectedOwners) });
    }
    if (this.selectedIDs.length !== 0) {
      andPathQuery.push({ $or: this.buildOrPathQuery("id", this.selectedIDs) });
    }

    //executes search using AndPathQuery and then filters result if searching by ctime
    if (andPathQuery.length !== 0) {
      searchOutput = this.fuse.search({ $and: andPathQuery }).map(res => res.item);
    }

    if (this.selectedDate !== null) {
      searchOutput = this.searchCreationTime(this.selectedDate, searchOutput);
    }

    if (this.selectedProjects.length !== 0) {
      searchOutput = searchOutput.filter(workflowEntry => {
        for (const proj of this.selectedProjects) {
          if (workflowEntry.projectIDs.includes(proj.pid)) {
            return true;
          }
        }
      });
    }
    return searchOutput;
  }

  /**
   * retrieves the workflow ids of workflows with the operator(s) specified
   */
  public retrieveWorkflowByOperator(operator: string): Observable<string[]> {
    return this.http.get<string[]>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_OPERATOR_URL}?operator=${operator}`);
  }

  /**
   * retrieves all workflow owners
   */
  public retrieveOwners(): Observable<{ userName: string; checked: boolean }[]> {
    return this.http.get<string[]>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_OWNER_URL}`).pipe(
      map((owners: string[]) => {
        return owners.map((user: string) => {
          return {
            userName: user,
            checked: false,
          };
        });
      })
    );
  }

  /**
   * retrieves all workflow IDs
   */
  public retrieveIDs(): Observable<{ id: string; checked: boolean }[]> {
    return this.http.get<string[]>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_ID_URL}`).pipe(
      map((wids: string[]) => {
        return wids.map(wid => {
          return {
            id: wid,
            checked: false,
          };
        });
      })
    );
  }

  /**
   * checks if a tag string is a workflow name or dropdown menu search parameter
   */
  private checkIfWorkflowName(tag: string) {
    const stringChecked: string[] = tag.split(":");
    if (stringChecked.length == 2 && this.searchCriteria.includes(stringChecked[0])) {
      return false;
    }
    return true;
  }

  /**
   * Sort the workflows according to the sortMethod variable
   */
  public sortWorkflows(): void {
    switch (this.sortMethod) {
      case SortMethod.NameAsc:
        this.ascSort();
        break;
      case SortMethod.NameDesc:
        this.dscSort();
        break;
      case SortMethod.EditTimeDesc:
        this.lastSort();
        break;
      case SortMethod.CreateTimeDesc:
        this.dateSort();
        break;
    }
  }

  /**
   * sort the workflow by name in ascending order
   */
  public ascSort(): void {
    this.sortMethod = SortMethod.NameAsc;
    this.dashboardWorkflowEntries = this.dashboardWorkflowEntries
      .slice()
      .sort((t1, t2) => t1.workflow.name.toLowerCase().localeCompare(t2.workflow.name.toLowerCase()));
  }

  /**
   * sort the project by name in descending order
   */
  public dscSort(): void {
    this.sortMethod = SortMethod.NameDesc;
    this.dashboardWorkflowEntries = this.dashboardWorkflowEntries
      .slice()
      .sort((t1, t2) => t2.workflow.name.toLowerCase().localeCompare(t1.workflow.name.toLowerCase()));
  }

  /**
   * sort the project by creating time in descending order
   */
  public dateSort(): void {
    this.sortMethod = SortMethod.CreateTimeDesc;
    this.dashboardWorkflowEntries = this.dashboardWorkflowEntries
      .slice()
      .sort((left, right) =>
        left.workflow.creationTime !== undefined && right.workflow.creationTime !== undefined
          ? right.workflow.creationTime - left.workflow.creationTime
          : 0
      );
  }

  /**
   * sort the project by last modified time in descending order
   */
  public lastSort(): void {
    this.sortMethod = SortMethod.EditTimeDesc;
    this.dashboardWorkflowEntries = this.dashboardWorkflowEntries
      .slice()
      .sort((left, right) =>
        left.workflow.lastModifiedTime !== undefined && right.workflow.lastModifiedTime !== undefined
          ? right.workflow.lastModifiedTime - left.workflow.lastModifiedTime
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
    window.open(`${ROUTER_WORKFLOW_BASE_URL}/${wid}`);
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
        this.zip = new JSZip();
        this.downloadListWorkflow = new Map<number, string>();
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
          this.userProjectsDropdown = this.userProjectsList.map(proj => {
            return { pid: proj.pid, name: proj.name, checked: false };
          });
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
      this.sortWorkflows();
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
    this.searchWorkflow();
  }

  private clearDashboardWorkflowEntries(): void {
    for (let wid of this.downloadListWorkflow.keys()) {
      const checkbox = document.getElementById(wid.toString()) as HTMLInputElement | null;
      if (checkbox != null) {
        checkbox.checked = false;
      }
    }
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

  /**
   * Verify Uploaded file name and upload the file
   */
  public onClickUploadExistingWorkflowFromLocal = (file: NzUploadFile): boolean => {
    const fileExtensionIndex = file.name.lastIndexOf(".");
    if (file.name.substring(fileExtensionIndex) == ".zip") {
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
              this.dashboardWorkflowEntries = [...this.dashboardWorkflowEntries, uploadedWorkflow];
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
  public onClickOpenDownloadZip() {
    let dateTime = new Date();
    let filename = "workflowExports-" + dateTime.toISOString() + ".zip";
    this.zip.generateAsync({ type: "blob" }).then(function (content) {
      saveAs(content, filename);
    });
  }

  /**
   * Adding the workflow as pending download zip file
   */
  public onClickAddToDownload(dashboardWorkflowEntry: DashboardWorkflowEntry, event: Event) {
    if ((<HTMLInputElement>event.target).checked) {
      const fileName = this.nameWorkflow(dashboardWorkflowEntry.workflow.name) + ".json";
      if (dashboardWorkflowEntry.workflow.wid) {
        if (!this.downloadListWorkflow.has(dashboardWorkflowEntry.workflow.wid)) {
          this.downloadListWorkflow.set(dashboardWorkflowEntry.workflow.wid, fileName);
          this.notificationService.success(
            "Successfully added workflow " + dashboardWorkflowEntry.workflow.wid + " to download list."
          );
        }
        this.workflowPersistService
          .retrieveWorkflow(dashboardWorkflowEntry.workflow.wid)
          .pipe(untilDestroyed(this))
          .subscribe(data => {
            const workflowCopy: Workflow = {
              ...data,
              wid: undefined,
              creationTime: undefined,
              lastModifiedTime: undefined,
            };
            const workflowJson = JSON.stringify(workflowCopy.content);
            this.zip.file(fileName, workflowJson);
          });
      }
    } else {
      if (dashboardWorkflowEntry.workflow.wid) {
        const existFileName = this.downloadListWorkflow.get(dashboardWorkflowEntry.workflow.wid) as string;
        this.zip.file(existFileName, "remove").remove(existFileName);
        this.downloadListWorkflow.delete(dashboardWorkflowEntry.workflow.wid);
        this.notificationService.info(
          "Workflow " + dashboardWorkflowEntry.workflow.wid + " removed from download list."
        );
      }
    }
  }

  /**
   * Resolve name conflict
   */
  private nameWorkflow(name: string) {
    let count = 0;
    const values = [...this.downloadListWorkflow.values()];
    let copyName = name;
    while (true) {
      if (!values.includes(copyName + ".json")) {
        return copyName;
      } else {
        copyName = name + "-" + ++count;
      }
    }
  }
}
