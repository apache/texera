import { Component, OnInit, Input, SimpleChanges, OnChanges } from "@angular/core";
import { Router } from "@angular/router";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { cloneDeep, concat, forEach, over } from "lodash-es";
import { from, Observable, Operator } from "rxjs";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { NgbdModalDeleteWorkflowComponent } from "./ngbd-modal-delete-workflow/ngbd-modal-delete-workflow.component";
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
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";

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

  /**
  * variables for dropdown menus and searching
  */
  public owners: { userName: string; checked: boolean }[] = [];
  public wids: { id: string; checked: boolean }[] = [];
  public operatorGroups: string[] = [];
  public operators: Map<string, {userFriendlyName: string, operatorType: string, operatorGroup: string, checked: boolean}[]> = new Map();
  public selectedDate: null | Date = null;

  private selectedOwners: string[] = [];
  private selectedIDs: string[] = [];
  private selectedOperators: {userFriendlyName: string, operatorType: string, operatorGroup: string}[] = [];
  
  public masterFilterList: string[] = [];

  /* variables for workflow editing / search */
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
  public searchCriteria: string[] = ["owner", "id", "ctime", "operator"];
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
      size: "lg",
      windowClass: "modal-xl",
    });
    modalRef.componentInstance.workflow = workflow;
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
    this.workflowPersistService
      .retrieveOwners()
      .pipe(untilDestroyed(this))
      .subscribe(list_of_owners => (this.owners = list_of_owners));
    this.workflowPersistService
      .retrieveIDs()
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
    date: string,
    filteredDashboardWorkflowEntries: ReadonlyArray<DashboardWorkflowEntry>
  ): ReadonlyArray<DashboardWorkflowEntry> {
    const date_regex = /^([<>]?)(\d{4})[-](0[1-9]|1[0-2])[-](0[1-9]|[12][0-9]|3[01])$/;
    const search_date: RegExpMatchArray | null = date.match(date_regex);
    if (!search_date) {
      this.notificationService.error("Date format is incorrect");
      return this.dashboardWorkflowEntries;
      //maintains the displayed saved workflows
    }
    const search_year: number = parseInt(search_date[2]);
    const search_month: number = parseInt(search_date[3]); //month: 1-12
    const search_day: number = parseInt(search_date[4]);
    const search_date_obj: Date = new Date(search_year, search_month - 1, search_day); // month: 0-11
    return filteredDashboardWorkflowEntries.filter(workflow_entry => {
      //filters for workflows that were created on the specified date
      if (workflow_entry.workflow.creationTime) {
        if (search_date[1] === "<") {
          return workflow_entry.workflow.creationTime < search_date_obj.getTime() + 86400000;
          //checks if creation time is before or on the specified date
        } else if (search_date[1] === ">") {
          return workflow_entry.workflow.creationTime >= search_date_obj.getTime();
          //checks if creation time is after or on the specfied date
        } else {
          return (
            workflow_entry.workflow.creationTime >= search_date_obj.getTime() &&
            workflow_entry.workflow.creationTime < search_date_obj.getTime() + 86400000
          );
          //checks if creation time is within the range of the whole day
        }
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
    const filteredOperators: {userFriendlyName: string, operatorType: string, operatorGroup: string}[] = [];
    Array.from(this.operators.values()).forEach(
      (operator_list: {userFriendlyName: string, operatorType: string, operatorGroup: string, checked: boolean }[]) => {
        operator_list.forEach(operator => {
          if (operator.checked) {
            filteredOperators.push({userFriendlyName: operator.userFriendlyName, operatorType: operator.operatorType, operatorGroup: operator.operatorGroup});
          }
        });
      }
      );
      this.selectedOperators = filteredOperators;
      this.searchWorkflow();
    }

    /**
     * callback function when calendar is altered
     */
    public calendarValueChange(value: Date): void {
      this.searchWorkflow();
    }
    
  /**
   * builds the tags to be displayd in the nz-select search bar
   * - Workflow names with ":" are not allowed due to conflict with other search parameters' format
   */
  private buildMasterFilterList(): void {
    let newFilterList: string[] = this.masterFilterList.filter(tag => !tag.includes(":"))
    newFilterList = newFilterList.concat(this.selectedOwners.map(owner => "owner: " + owner));
    newFilterList = newFilterList.concat(this.selectedIDs.map(id => "id: " + id));
    newFilterList = newFilterList.concat(this.selectedOperators.map(operator => "operator: "+operator.userFriendlyName));
    if(this.selectedDate !== null) {
      newFilterList.push("ctime: " + this.getFormattedDateString(this.selectedDate));
    }
    this.masterFilterList = newFilterList;
  }
  /**
   * sets all dropdown menu options to unchecked
   */
  private setSelectedDropdownsToUnchecked() {
    this.owners.forEach(owner => {
      owner.checked = false;
    })
    this.wids.forEach(wid => {
      wid.checked = false;
    })
    for (let operatorList of this.operators.values()) {
      operatorList.forEach(operator => operator.checked = false)
    }
  }

  /**
   * updates dropdown menus when nz-select bar is changed
   */
  public updateDropdownMenus(tagListString: string) {
    const tagList = Array.from(tagListString);
    let hasDate: boolean = false;
    this.setSelectedDropdownsToUnchecked();
    tagList.forEach( tag => {
      if(tag.includes(":")) {
        const searchArray = tag.split(":");
        const searchField = searchArray[0];
        const searchValue = searchArray[1].trim();
        switch (searchField) {
          case "owner":
            this.owners[this.owners.findIndex(owner => owner.userName === searchValue)].checked = true;
            break;
          case "id":
            this.wids[this.wids.findIndex(wid => wid.id === searchValue)].checked = true;
            break;
          case "operator":
            const operator = this.selectedOperators.find(operator => operator.userFriendlyName === searchValue);
            const operatorSublist = this.operators.get(operator ? operator.operatorGroup : "");
            if(operatorSublist) {
              operatorSublist.forEach(operator => {if(operator.userFriendlyName === searchValue) {operator.checked = true}} );
            }
            break;
          case "ctime":
            hasDate = true;
            break;
        }
      }
    })
    if(!hasDate) {
      this.selectedDate = null;
    }

    //updates selected parameter lists
    this.selectedOwners = this.owners.filter(owner => owner.checked).map(owner => owner.userName);
    this.selectedIDs = this.wids.filter(wid => wid.checked === true).map(wid => wid.id);
    const filteredOperators: {userFriendlyName: string, operatorType: string, operatorGroup: string}[] = [];
    Array.from(this.operators.values()).forEach(
      (operator_list: {userFriendlyName: string, operatorType: string, operatorGroup: string, checked: boolean }[]) => {
        operator_list.forEach(operator => {
          if (operator.checked) {
            filteredOperators.push({userFriendlyName: operator.userFriendlyName, operatorType: operator.operatorType, operatorGroup: operator.operatorGroup});
          }
        });
      }
    );
    this.selectedOperators = filteredOperators;

    this.searchWorkflow();
  }


  /**
   * returns a formatted string representing a Date object
   */
  private getFormattedDateString(date: Date): string {
    let dateMonth: number = date.getMonth() + 1;
    let dateDay: number = date.getDate();
    return `${date.getFullYear()}-${((dateMonth < 10) ? "0" : "")+dateMonth}-${((dateDay < 10) ? "0" : "")+dateDay}`;
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
   * Search workflows by owner name, workflow name, or workflow id
   * Use fuse.js https://fusejs.io/ as the tool for searching
   * 
   * search value Format (must follow this): 
   *  - WORKFLOWNAME owner:OWNERNAME(S) id:ID(S) operator:OPERATOR(S)
   */
  public searchWorkflow(): void {
    this.buildMasterFilterList();
    if(this.masterFilterList.length === 0) {
      //if there are no tags, return all workflow entries
      this.dashboardWorkflowEntries = this.allDashboardWorkflowEntries;
      return;
    }
    if(this.selectedOperators.length > 0) {
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
    this.workflowPersistService
      .retrieveWorkflowByOperator(this.selectedOperators.map(operator => operator.operatorType).toString())
      .pipe(untilDestroyed(this))
      .subscribe(list_of_wids => {
        andPathQuery.push({ $or: this.buildOrPathQuery("id", list_of_wids, true)})
        this.dashboardWorkflowEntries = this.synchronousSearch(andPathQuery);
      });
  }

  /**
   * Searches workflows with given frontend data
   * no backend calls so runs synchronously
   */
  private synchronousSearch(andPathQuery: Object[]): ReadonlyArray<DashboardWorkflowEntry> {
    let searchOutput: ReadonlyArray<DashboardWorkflowEntry> = this.allDashboardWorkflowEntries.slice();
    const workflowNames = this.masterFilterList.filter(tag => !tag.includes(":"));
    if(workflowNames.length !== 0) {
      andPathQuery.push({$or: this.buildOrPathQuery("workflowName", workflowNames)})
    }
    if(this.selectedOwners.length !== 0)
      andPathQuery.push({$or: this.buildOrPathQuery("owner", this.selectedOwners)})
    if(this.selectedIDs.length !== 0)
      andPathQuery.push({$or: this.buildOrPathQuery("id", this.selectedIDs)})
    if (andPathQuery.length !== 0) 
      searchOutput = this.fuse.search({ $and: andPathQuery }).map(res => res.item);
    if (this.selectedDate !== null) {
      searchOutput = this.searchCreationTime(this.getFormattedDateString(this.selectedDate), searchOutput);
    }
    return searchOutput;
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
   * sort the project by create time
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
