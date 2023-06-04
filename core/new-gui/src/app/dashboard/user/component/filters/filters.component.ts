import { Component, EventEmitter, OnInit, Output } from "@angular/core";
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Observable, map } from "rxjs";
import { HttpClient } from "@angular/common/http";
import { UserProject } from "../../type/user-project";
import { remove } from "lodash-es";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { UserProjectService } from "../../service/user-project/user-project.service";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";

@UntilDestroy()
@Component({
  selector: "texera-filters",
  templateUrl: "./filters.component.html",
  styleUrls: ["./filters.component.scss"],
})
export class FiltersComponent implements OnInit {
  private _masterFilterList: string[] = [];
  @Output()
  public masterFilterListChange = new EventEmitter<typeof this._masterFilterList>();
  public get masterFilterList(): string[] {
    return this._masterFilterList;
  }
  public set masterFilterList(value: string[]) {
    if (
      !this._masterFilterList ||
      !value ||
      this._masterFilterList.length !== value.length ||
      this._masterFilterList.some((v, i) => v !== value[i])
    ) {
      // Only update when there is a change to prevent unnecessary calls to search.
      this._masterFilterList = value;
      this.updateDropdownMenus(value);
      this.masterFilterListChange.emit(value);
    }
  }
  public owners: { userName: string; checked: boolean }[] = [];
  public wids: { id: string; checked: boolean }[] = [];
  public operatorGroups: string[] = [];
  public operators: Map<
    string,
    { userFriendlyName: string; operatorType: string; operatorGroup: string; checked: boolean }[]
  > = new Map();
  public selectedCtime: Date[] = [];
  public selectedMtime: Date[] = [];
  public selectedOwners: string[] = [];
  public selectedIDs: string[] = [];
  public selectedOperators: { userFriendlyName: string; operatorType: string; operatorGroup: string }[] = [];
  public selectedProjects: { name: string; pid: number }[] = [];
  /* variables for filtering workflows by projects */
  public userProjectsList: Observable<UserProject[]>; // list of projects accessible by user
  public userProjectsDropdown: { pid: number; name: string; checked: boolean }[] = [];
  /* variables for project color tags */
  public userProjectsMap: ReadonlyMap<number, UserProject> = new Map(); // maps pid to its corresponding UserProject
  public userProjectsLoaded: boolean = false; // tracks whether all UserProject information has been loaded (ready to render project colors)
  public searchCriteria: string[] = ["owner", "id", "ctime", "mtime", "operator", "project"];

  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private notificationService: NotificationService,
    private userProjectService: UserProjectService,
    private workflowPersistService: WorkflowPersistService
  ) {
    this.userProjectsList = this.userProjectService.retrieveProjectList().pipe(untilDestroyed(this));
    this.userProjectsList.pipe(untilDestroyed(this)).subscribe((userProjectsList: UserProject[]) => {
      if (userProjectsList != null && userProjectsList.length > 0) {
        // map project ID to project object
        this.userProjectsMap = new Map(userProjectsList.map(userProject => [userProject.pid, userProject]));
        // store the projects containing these workflows
        this.userProjectsDropdown = userProjectsList.map(proj => {
          return { pid: proj.pid, name: proj.name, checked: false };
        });
        this.userProjectsLoaded = true;
      }
    });
  }

  ngOnInit(): void {
    this.searchParameterBackendSetup();
  }

  /**
   * Backend calls for Workflow IDs, Owners, and Operators in saved workflow component
   */
  private searchParameterBackendSetup() {
    this.operatorMetadataService
      .getOperatorMetadata()
      .pipe(untilDestroyed(this))
      .subscribe(opdata => {
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
      .subscribe(list_of_owners => {
        this.owners = list_of_owners.map(i => ({ userName: i, checked: false }));
      });
    this.workflowPersistService
      .retrieveWorkflowIDs()
      .pipe(untilDestroyed(this))
      .subscribe(wids => {
        this.wids = wids.map(wid => {
          return {
            id: wid.toString(),
            checked: false,
          };
        });
      });
  }

  /**
   * updates selectedOwners array to match owners checked in dropdown menu
   */
  public updateSelectedOwners(): void {
    this.selectedOwners = this.owners.filter(owner => owner.checked).map(owner => owner.userName);
    this.buildMasterFilterList();
  }

  /**
   * updates selectedIDs array to match worfklow ids checked in dropdown menu
   */
  public updateSelectedIDs(): void {
    this.selectedIDs = this.wids.filter(wid => wid.checked).map(wid => wid.id);
    this.buildMasterFilterList();
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
    this.buildMasterFilterList();
  }

  /**
   * updates selectedProjects array to match projects checked in dropdown menu
   */
  public updateSelectedProjects(): void {
    this.selectedProjects = this.userProjectsDropdown
      .filter(proj => proj.checked)
      .map(proj => {
        return { name: proj.name, pid: proj.pid };
      });
    this.buildMasterFilterList();
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
    this.selectedCtime = [];
    this.selectedMtime = [];
    this.setDropdownSelectionsToUnchecked();
    tagListString.forEach(tag => {
      if (tag.includes(":")) {
        const searchArray = tag.split(":");
        const searchField = searchArray[0];
        const searchValue = searchArray[1].trim();
        const date_regex =
          /^(\d{4})[-](0[1-9]|1[0-2])[-](0[1-9]|[12][0-9]|3[01])[~](\d{4})[-](0[1-9]|1[0-2])[-](0[1-9]|[12][0-9]|3[01])$/;
        const searchDate: RegExpMatchArray | null = searchValue.match(date_regex);
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
            if (this.selectedCtime.length > 0) {
              // if there is already an selected date, ignore the subsequent ctime tags
              this.notificationService.error("Multiple search dates is not allowed");
              break;
            }
            if (!searchDate) {
              this.notificationService.error("Date format is incorrect");
              break;
            }
            this.selectedCtime[0] = new Date(
              parseInt(searchDate[1]),
              parseInt(searchDate[2]) - 1,
              parseInt(searchDate[3])
            );
            this.selectedCtime[1] = new Date(
              parseInt(searchDate[4]),
              parseInt(searchDate[5]) - 1,
              parseInt(searchDate[6])
            );
            break;
          case "mtime": //should only run at most once
            if (this.selectedMtime.length > 0) {
              // if there is already an selected date, ignore the subsequent ctime tags
              this.notificationService.error("Multiple search dates is not allowed");
              break;
            }
            if (!searchDate) {
              this.notificationService.error("Date format is incorrect");
              break;
            }
            this.selectedMtime[0] = new Date(
              parseInt(searchDate[1]),
              parseInt(searchDate[2]) - 1,
              parseInt(searchDate[3])
            );
            this.selectedMtime[1] = new Date(
              parseInt(searchDate[4]),
              parseInt(searchDate[5]) - 1,
              parseInt(searchDate[6])
            );
            break;
        }
      }
    });
    this.selectedOperators = newSelectedOperators;
    this.buildMasterFilterList();
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
   * checks if a tag string is a workflow name or dropdown menu search parameter
   */
  public checkIfWorkflowName(tag: string) {
    const stringChecked: string[] = tag.split(":");
    return !(stringChecked.length === 2 && this.searchCriteria.includes(stringChecked[0]));
  }

  /**
   * builds the tags to be displayd in the nz-select search bar
   * - Workflow names with ":" are not allowed due to conflict with other search parameters' format
   */
  public buildMasterFilterList(): void {
    let newFilterList: string[] = this.masterFilterList.filter(tag => this.checkIfWorkflowName(tag));
    newFilterList = newFilterList.concat(this.selectedOwners.map(owner => "owner: " + owner));
    newFilterList = newFilterList.concat(this.selectedIDs.map(id => "id: " + id));
    newFilterList = newFilterList.concat(
      this.selectedOperators.map(operator => "operator: " + operator.userFriendlyName)
    );
    newFilterList = newFilterList.concat(this.selectedProjects.map(proj => "project: " + proj.name));
    if (this.selectedCtime.length != 0) {
      newFilterList.push(
        "ctime: " +
          this.getFormattedDateString(this.selectedCtime[0]) +
          " ~ " +
          this.getFormattedDateString(this.selectedCtime[1])
      );
    }
    if (this.selectedMtime.length != 0) {
      newFilterList.push(
        "mtime: " +
          this.getFormattedDateString(this.selectedMtime[0]) +
          " ~ " +
          this.getFormattedDateString(this.selectedMtime[1])
      );
    }
    this._masterFilterList = newFilterList; // Avoid the call to updateDropdownMenus.
    this.masterFilterListChange.emit(newFilterList);
  }

  /**
   * returns a formatted string representing a Date object
   */
  private getFormattedDateString(date: Date): string {
    let dateMonth: number = date.getMonth() + 1;
    let dateDay: number = date.getDate();
    return `${date.getFullYear()}-${(dateMonth < 10 ? "0" : "") + dateMonth}-${(dateDay < 10 ? "0" : "") + dateDay}`;
  }
}
